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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.*;

import java.util.Map;

/**
 * Base class for the snapshots of the heap backend that outlines the algorithm and offers some hooks to realize
 * the concrete strategies. Subclasses must be threadsafe.
 */
class HeapSnapshotStrategy<K>
	extends AbstractSnapshotStrategy<KeyedStateHandle> implements SnapshotStrategySynchronicityBehavior<K> {

	private final SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait;
	private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;
	private final StreamCompressionDecorator keyGroupCompressionDecorator;
	private final LocalRecoveryConfig localRecoveryConfig;
	private final KeyGroupRange keyGroupRange;
	private final CloseableRegistry cancelStreamRegistry;
	private final StateSerializerProvider<K> keySerializerProvider;

	HeapSnapshotStrategy(
		SnapshotStrategySynchronicityBehavior<K> snapshotStrategySynchronicityTrait,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		StreamCompressionDecorator keyGroupCompressionDecorator,
		LocalRecoveryConfig localRecoveryConfig,
		KeyGroupRange keyGroupRange,
		CloseableRegistry cancelStreamRegistry,
		StateSerializerProvider<K> keySerializerProvider) {
		super("Heap backend snapshot");
		this.snapshotStrategySynchronicityTrait = snapshotStrategySynchronicityTrait;
		this.registeredKVStates = registeredKVStates;
		this.registeredPQStates = registeredPQStates;
		this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
		this.localRecoveryConfig = localRecoveryConfig;
		this.keyGroupRange = keyGroupRange;
		this.cancelStreamRegistry = cancelStreamRegistry;
		this.keySerializerProvider = keySerializerProvider;
	}

	@Override
	public void finalizeSnapshotBeforeReturnHook(Runnable runnable) {
		snapshotStrategySynchronicityTrait.finalizeSnapshotBeforeReturnHook(runnable);
	}

	@Override
	public boolean isAsynchronous() {
		return snapshotStrategySynchronicityTrait.isAsynchronous();
	}

	@Override
	public <N, V> StateTable<K, N, V> newStateTable(
		InternalKeyContext<K> keyContext,
		RegisteredKeyValueStateBackendMetaInfo<N, V> newMetaInfo,
		TypeSerializer<K> keySerializer) {
		return snapshotStrategySynchronicityTrait.newStateTable(keyContext, newMetaInfo, keySerializer);
	}

	public TypeSerializer<K> getKeySerializer() {
		return keySerializerProvider.currentSchemaSerializer();
	}
}
