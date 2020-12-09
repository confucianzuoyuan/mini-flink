/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.util.OperatorSubtaskDescriptionText;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.CloseableIterable;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;

public class StreamTaskStateInitializerImpl implements StreamTaskStateInitializer {

	/** The logger for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(StreamTaskStateInitializerImpl.class);

	/**
	 * The environment of the task. This is required as parameter to construct state backends via their factory.
	 */
	private final Environment environment;

	/** The state manager of the tasks provides the information used to restore potential previous state. */
	private final TaskStateManager taskStateManager;

	/** This object is the factory for everything related to state backends and checkpointing. */
	private final StateBackend stateBackend;


	@VisibleForTesting
	public StreamTaskStateInitializerImpl(
		Environment environment,
		StateBackend stateBackend) {

		this.environment = environment;
		this.taskStateManager = Preconditions.checkNotNull(environment.getTaskStateManager());
		this.stateBackend = Preconditions.checkNotNull(stateBackend);
	}

	// -----------------------------------------------------------------------------------------------------------------

	@Override
	public StreamOperatorStateContext streamOperatorStateContext(
		@Nonnull OperatorID operatorID,
		@Nonnull String operatorClassName,
		@Nonnull ProcessingTimeService processingTimeService,
		@Nonnull KeyContext keyContext,
		@Nullable TypeSerializer<?> keySerializer,
		@Nonnull CloseableRegistry streamTaskCloseableRegistry) throws Exception {

		TaskInfo taskInfo = environment.getTaskInfo();
		OperatorSubtaskDescriptionText operatorSubtaskDescription =
			new OperatorSubtaskDescriptionText(
				operatorID,
				operatorClassName,
				taskInfo.getIndexOfThisSubtask(),
				taskInfo.getNumberOfParallelSubtasks());

		final String operatorIdentifierText = operatorSubtaskDescription.toString();

		final PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates =
			taskStateManager.prioritizedOperatorState(operatorID);

		AbstractKeyedStateBackend<?> keyedStatedBackend = null;
		OperatorStateBackend operatorStateBackend = null;
		CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs = null;
		InternalTimeServiceManager<?> timeServiceManager;

		try {

			// -------------- Keyed State Backend --------------
			keyedStatedBackend = keyedStatedBackend(
				keySerializer,
				operatorIdentifierText,
				prioritizedOperatorSubtaskStates,
				streamTaskCloseableRegistry);

			// -------------- Raw State Streams --------------
			rawOperatorStateInputs = rawOperatorStateInputs(
				prioritizedOperatorSubtaskStates.getPrioritizedRawOperatorState().iterator());
			streamTaskCloseableRegistry.registerCloseable(rawOperatorStateInputs);

			// -------------- Internal Timer Service Manager --------------
			timeServiceManager = internalTimeServiceManager(keyedStatedBackend, keyContext, processingTimeService);

			// -------------- Preparing return value --------------

			return new StreamOperatorStateContextImpl(
				prioritizedOperatorSubtaskStates.isRestored(),
				operatorStateBackend,
				keyedStatedBackend,
				timeServiceManager,
				rawOperatorStateInputs);
		} catch (Exception ex) {
			throw new Exception("Exception while creating StreamOperatorStateContext.", ex);
		}
	}

	protected <K> InternalTimeServiceManager<K> internalTimeServiceManager(
		AbstractKeyedStateBackend<K> keyedStatedBackend,
		KeyContext keyContext, //the operator
		ProcessingTimeService processingTimeService) throws Exception {

		if (keyedStatedBackend == null) {
			return null;
		}

		final KeyGroupRange keyGroupRange = keyedStatedBackend.getKeyGroupRange();

		final InternalTimeServiceManager<K> timeServiceManager = new InternalTimeServiceManager<>(
			keyGroupRange,
			keyContext,
			keyedStatedBackend,
			processingTimeService,
			keyedStatedBackend.requiresLegacySynchronousTimerSnapshots());

		return timeServiceManager;
	}

	protected <K> AbstractKeyedStateBackend<K> keyedStatedBackend(
		TypeSerializer<K> keySerializer,
		String operatorIdentifierText,
		PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskStates,
		CloseableRegistry backendCloseableRegistry) throws Exception {

		if (keySerializer == null) {
			return null;
		}

		String logDescription = "keyed state backend for " + operatorIdentifierText;

		TaskInfo taskInfo = environment.getTaskInfo();

		final KeyGroupRange keyGroupRange = KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(
			taskInfo.getMaxNumberOfParallelSubtasks(),
			taskInfo.getNumberOfParallelSubtasks(),
			taskInfo.getIndexOfThisSubtask());

		// Now restore processing is included in backend building/constructing process, so we need to make sure
		// each stream constructed in restore could also be closed in case of task cancel, for example the data
		// input stream opened for serDe during restore.
		CloseableRegistry cancelStreamRegistryForRestore = new CloseableRegistry();
		backendCloseableRegistry.registerCloseable(cancelStreamRegistryForRestore);
		BackendRestorerProcedure<AbstractKeyedStateBackend<K>, KeyedStateHandle> backendRestorer =
			new BackendRestorerProcedure<>(
				(stateHandles) -> stateBackend.createKeyedStateBackend(
					environment,
					environment.getJobID(),
					operatorIdentifierText,
					keySerializer,
					taskInfo.getMaxNumberOfParallelSubtasks(),
					keyGroupRange,
					environment.getTaskKvStateRegistry(),
					stateHandles,
					cancelStreamRegistryForRestore),
				backendCloseableRegistry,
				logDescription);

		try {
			return backendRestorer.createAndRestore(
				prioritizedOperatorSubtaskStates.getPrioritizedManagedKeyedState());
		} finally {
			if (backendCloseableRegistry.unregisterCloseable(cancelStreamRegistryForRestore)) {
			}
		}
	}

	protected CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs(
		Iterator<StateObjectCollection<OperatorStateHandle>> restoreStateAlternatives) {

		if (restoreStateAlternatives.hasNext()) {

			Collection<OperatorStateHandle> rawOperatorState = restoreStateAlternatives.next();
			// TODO currently this does not support local state recovery, so we expect there is only one handle.
			Preconditions.checkState(
				!restoreStateAlternatives.hasNext(),
				"Local recovery is currently not implemented for raw operator state, but found state alternative.");
		}

		return CloseableIterable.empty();
	}

	// =================================================================================================================

	private static class StreamOperatorStateContextImpl implements StreamOperatorStateContext {

		private final boolean restored;

		private final OperatorStateBackend operatorStateBackend;
		private final AbstractKeyedStateBackend<?> keyedStateBackend;
		private final InternalTimeServiceManager<?> internalTimeServiceManager;

		private final CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs;

		StreamOperatorStateContextImpl(
			boolean restored,
			OperatorStateBackend operatorStateBackend,
			AbstractKeyedStateBackend<?> keyedStateBackend,
			InternalTimeServiceManager<?> internalTimeServiceManager,
			CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs) {

			this.restored = restored;
			this.operatorStateBackend = operatorStateBackend;
			this.keyedStateBackend = keyedStateBackend;
			this.internalTimeServiceManager = internalTimeServiceManager;
			this.rawOperatorStateInputs = rawOperatorStateInputs;
		}

		@Override
		public boolean isRestored() {
			return restored;
		}

		@Override
		public AbstractKeyedStateBackend<?> keyedStateBackend() {
			return keyedStateBackend;
		}

		@Override
		public OperatorStateBackend operatorStateBackend() {
			return operatorStateBackend;
		}

		@Override
		public InternalTimeServiceManager<?> internalTimerServiceManager() {
			return internalTimeServiceManager;
		}

		@Override
		public CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs() {
			return rawOperatorStateInputs;
		}

	}

}
