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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
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
import java.io.IOException;
import java.util.*;

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
		CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs = null;
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
			rawKeyedStateInputs = rawKeyedStateInputs(
				prioritizedOperatorSubtaskStates.getPrioritizedRawKeyedState().iterator());
			streamTaskCloseableRegistry.registerCloseable(rawKeyedStateInputs);

			rawOperatorStateInputs = rawOperatorStateInputs(
				prioritizedOperatorSubtaskStates.getPrioritizedRawOperatorState().iterator());
			streamTaskCloseableRegistry.registerCloseable(rawOperatorStateInputs);

			// -------------- Internal Timer Service Manager --------------
			timeServiceManager = internalTimeServiceManager(keyedStatedBackend, keyContext, processingTimeService, rawKeyedStateInputs);

			// -------------- Preparing return value --------------

			return new StreamOperatorStateContextImpl(
				prioritizedOperatorSubtaskStates.isRestored(),
				operatorStateBackend,
				keyedStatedBackend,
				timeServiceManager,
				rawOperatorStateInputs,
				rawKeyedStateInputs);
		} catch (Exception ex) {
			throw new Exception("Exception while creating StreamOperatorStateContext.", ex);
		}
	}

	protected <K> InternalTimeServiceManager<K> internalTimeServiceManager(
		AbstractKeyedStateBackend<K> keyedStatedBackend,
		KeyContext keyContext, //the operator
		ProcessingTimeService processingTimeService,
		Iterable<KeyGroupStatePartitionStreamProvider> rawKeyedStates) throws Exception {

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

	protected CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs(
		Iterator<StateObjectCollection<KeyedStateHandle>> restoreStateAlternatives) {

		if (restoreStateAlternatives.hasNext()) {
			Collection<KeyedStateHandle> rawKeyedState = restoreStateAlternatives.next();

			// TODO currently this does not support local state recovery, so we expect there is only one handle.
			Preconditions.checkState(
				!restoreStateAlternatives.hasNext(),
				"Local recovery is currently not implemented for raw keyed state, but found state alternative.");

			if (rawKeyedState != null) {
				Collection<KeyGroupsStateHandle> keyGroupsStateHandles = transform(rawKeyedState);
				final CloseableRegistry closeableRegistry = new CloseableRegistry();

				return new CloseableIterable<KeyGroupStatePartitionStreamProvider>() {
					@Override
					public void close() throws IOException {
						closeableRegistry.close();
					}

					@Override
					public Iterator<KeyGroupStatePartitionStreamProvider> iterator() {
						return new KeyGroupStreamIterator(keyGroupsStateHandles.iterator(), closeableRegistry);
					}
				};
			}
		}

		return CloseableIterable.empty();
	}

	// =================================================================================================================

	private static class KeyGroupStreamIterator
		extends AbstractStateStreamIterator<KeyGroupStatePartitionStreamProvider, KeyGroupsStateHandle> {

		private Iterator<Tuple2<Integer, Long>> currentOffsetsIterator;

		KeyGroupStreamIterator(
			Iterator<KeyGroupsStateHandle> stateHandleIterator, CloseableRegistry closableRegistry) {

			super(stateHandleIterator, closableRegistry);
		}

		@Override
		public boolean hasNext() {

			if (null != currentStateHandle && currentOffsetsIterator.hasNext()) {

				return true;
			}

			closeCurrentStream();

			while (stateHandleIterator.hasNext()) {
				currentStateHandle = stateHandleIterator.next();
				if (currentStateHandle.getKeyGroupRange().getNumberOfKeyGroups() > 0) {
					currentOffsetsIterator = currentStateHandle.getGroupRangeOffsets().iterator();

					return true;
				}
			}

			return false;
		}

		@Override
		public KeyGroupStatePartitionStreamProvider next() {

			if (!hasNext()) {

				throw new NoSuchElementException("Iterator exhausted");
			}

			Tuple2<Integer, Long> keyGroupOffset = currentOffsetsIterator.next();
			try {
				if (null == currentStream) {
					openCurrentStream();
				}

				currentStream.seek(keyGroupOffset.f1);
				return new KeyGroupStatePartitionStreamProvider(currentStream, keyGroupOffset.f0);

			} catch (IOException ioex) {
				return new KeyGroupStatePartitionStreamProvider(ioex, keyGroupOffset.f0);
			}
		}
	}

	private abstract static class AbstractStateStreamIterator<
		T extends StatePartitionStreamProvider, H extends StreamStateHandle>
		implements Iterator<T> {

		protected final Iterator<H> stateHandleIterator;
		protected final CloseableRegistry closableRegistry;

		protected H currentStateHandle;
		protected FSDataInputStream currentStream;

		AbstractStateStreamIterator(
			Iterator<H> stateHandleIterator,
			CloseableRegistry closableRegistry) {

			this.stateHandleIterator = Preconditions.checkNotNull(stateHandleIterator);
			this.closableRegistry = Preconditions.checkNotNull(closableRegistry);
		}

		protected void openCurrentStream() throws IOException {

			Preconditions.checkState(currentStream == null);

			FSDataInputStream stream = currentStateHandle.openInputStream();
			closableRegistry.registerCloseable(stream);
			currentStream = stream;
		}

		protected void closeCurrentStream() {
			if (closableRegistry.unregisterCloseable(currentStream)) {
			}
			currentStream = null;
		}

		@Override
		public void remove() {
			throw new UnsupportedOperationException("Read only Iterator");
		}
	}

	private static Collection<KeyGroupsStateHandle> transform(Collection<KeyedStateHandle> keyedStateHandles) {

		if (keyedStateHandles == null) {
			return null;
		}

		List<KeyGroupsStateHandle> keyGroupsStateHandles = new ArrayList<>(keyedStateHandles.size());

		for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {

			if (keyedStateHandle instanceof KeyGroupsStateHandle) {
				keyGroupsStateHandles.add((KeyGroupsStateHandle) keyedStateHandle);
			} else if (keyedStateHandle != null) {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected: " + KeyGroupsStateHandle.class +
					", but found: " + keyedStateHandle.getClass() + ".");
			}
		}

		return keyGroupsStateHandles;
	}

	private static class StreamOperatorStateContextImpl implements StreamOperatorStateContext {

		private final boolean restored;

		private final OperatorStateBackend operatorStateBackend;
		private final AbstractKeyedStateBackend<?> keyedStateBackend;
		private final InternalTimeServiceManager<?> internalTimeServiceManager;

		private final CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs;
		private final CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs;

		StreamOperatorStateContextImpl(
			boolean restored,
			OperatorStateBackend operatorStateBackend,
			AbstractKeyedStateBackend<?> keyedStateBackend,
			InternalTimeServiceManager<?> internalTimeServiceManager,
			CloseableIterable<StatePartitionStreamProvider> rawOperatorStateInputs,
			CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs) {

			this.restored = restored;
			this.operatorStateBackend = operatorStateBackend;
			this.keyedStateBackend = keyedStateBackend;
			this.internalTimeServiceManager = internalTimeServiceManager;
			this.rawOperatorStateInputs = rawOperatorStateInputs;
			this.rawKeyedStateInputs = rawKeyedStateInputs;
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

		@Override
		public CloseableIterable<KeyGroupStatePartitionStreamProvider> rawKeyedStateInputs() {
			return rawKeyedStateInputs;
		}
	}

}
