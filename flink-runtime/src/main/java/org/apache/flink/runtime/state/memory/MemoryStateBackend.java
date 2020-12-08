/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.memory;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.AbstractFileStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.util.TernaryBoolean;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;

import static org.apache.flink.util.Preconditions.checkArgument;

@PublicEvolving
public class MemoryStateBackend extends AbstractFileStateBackend implements ConfigurableStateBackend {

	private static final long serialVersionUID = 4109305377809414635L;

	/** The default maximal size that the snapshotted memory state may have (5 MiBytes). */
	public static final int DEFAULT_MAX_STATE_SIZE = 5 * 1024 * 1024;

	/** The maximal size that the snapshotted memory state may have. */
	private final int maxStateSize;

	/** Switch to chose between synchronous and asynchronous snapshots.
	 * A value of 'UNDEFINED' means not yet configured, in which case the default will be used. */
	private final TernaryBoolean asynchronousSnapshots;

	// ------------------------------------------------------------------------

	public MemoryStateBackend() {
		this(null, null, DEFAULT_MAX_STATE_SIZE, TernaryBoolean.UNDEFINED);
	}

	public MemoryStateBackend(
			@Nullable String checkpointPath,
			@Nullable String savepointPath,
			int maxStateSize,
			TernaryBoolean asynchronousSnapshots) {

		super(checkpointPath == null ? null : new Path(checkpointPath),
				savepointPath == null ? null : new Path(savepointPath));

		checkArgument(maxStateSize > 0, "maxStateSize must be > 0");
		this.maxStateSize = maxStateSize;

		this.asynchronousSnapshots = asynchronousSnapshots;
	}

	/**
	 * Private constructor that creates a re-configured copy of the state backend.
	 *
	 * @param original The state backend to re-configure
	 * @param configuration The configuration
	 * @param classLoader The class loader
	 */
	private MemoryStateBackend(MemoryStateBackend original, ReadableConfig configuration, ClassLoader classLoader) {
		super(original.getCheckpointPath(), original.getSavepointPath(), configuration);

		this.maxStateSize = original.maxStateSize;

		// if asynchronous snapshots were configured, use that setting,
		// else check the configuration
		this.asynchronousSnapshots = original.asynchronousSnapshots.resolveUndefined(
				configuration.get(CheckpointingOptions.ASYNC_SNAPSHOTS));
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the maximum size that an individual state can have, as configured in the
	 * constructor (by default {@value #DEFAULT_MAX_STATE_SIZE}).
	 *
	 * @return The maximum size that an individual state can have
	 */
	public int getMaxStateSize() {
		return maxStateSize;
	}

	/**
	 * Gets whether the key/value data structures are asynchronously snapshotted.
	 *
	 * <p>If not explicitly configured, this is the default value of
	 * {@link CheckpointingOptions#ASYNC_SNAPSHOTS}.
	 */
	public boolean isUsingAsynchronousSnapshots() {
		return asynchronousSnapshots.getOrDefault(CheckpointingOptions.ASYNC_SNAPSHOTS.defaultValue());
	}

	// ------------------------------------------------------------------------
	//  Reconfiguration
	// ------------------------------------------------------------------------

	/**
	 * Creates a copy of this state backend that uses the values defined in the configuration
	 * for fields where that were not specified in this state backend.
	 *
	 * @param config The configuration
	 * @param classLoader The class loader
	 * @return The re-configured variant of the state backend
	 */
	@Override
	public MemoryStateBackend configure(ReadableConfig config, ClassLoader classLoader) {
		return new MemoryStateBackend(this, config, classLoader);
	}

	// ------------------------------------------------------------------------
	//  checkpoint state persistence
	// ------------------------------------------------------------------------

	// ------------------------------------------------------------------------
	//  state holding structures
	// ------------------------------------------------------------------------

	@Override
	public <K> AbstractKeyedStateBackend<K> createKeyedStateBackend(
		Environment env,
		JobID jobID,
		String operatorIdentifier,
		TypeSerializer<K> keySerializer,
		int numberOfKeyGroups,
		KeyGroupRange keyGroupRange,
		TaskKvStateRegistry kvStateRegistry,
		@Nonnull Collection<KeyedStateHandle> stateHandles,
		CloseableRegistry cancelStreamRegistry) throws BackendBuildingException {

		TaskStateManager taskStateManager = env.getTaskStateManager();
		HeapPriorityQueueSetFactory priorityQueueSetFactory =
			new HeapPriorityQueueSetFactory(keyGroupRange, numberOfKeyGroups, 128);
		return new HeapKeyedStateBackendBuilder<>(
			kvStateRegistry,
			keySerializer,
			env.getUserClassLoader(),
			numberOfKeyGroups,
			keyGroupRange,
			env.getExecutionConfig(),
			stateHandles,
			AbstractStateBackend.getCompressionDecorator(env.getExecutionConfig()),
			taskStateManager.createLocalRecoveryConfig(),
			priorityQueueSetFactory,
			isUsingAsynchronousSnapshots(),
			cancelStreamRegistry).build();
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return "MemoryStateBackend (data in heap memory / checkpoints to JobManager) " +
				"(checkpoints: '" + getCheckpointPath() +
				"', savepoints: '" + getSavepointPath() +
				"', asynchronous: " + asynchronousSnapshots +
				", maxStateSize: " + maxStateSize + ")";
	}
}
