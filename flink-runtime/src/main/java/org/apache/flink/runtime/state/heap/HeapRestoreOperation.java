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

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.StateSerializerProvider;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.util.Collection;
import java.util.Map;

/**
 * Implementation of heap restore operation.
 *
 * @param <K> The data type that the serializer serializes.
 */
public class HeapRestoreOperation<K> implements RestoreOperation<Void> {
	private final Collection<KeyedStateHandle> restoreStateHandles;
	private final StateSerializerProvider<K> keySerializerProvider;
	private final ClassLoader userCodeClassLoader;
	private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
	private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates;
	private final CloseableRegistry cancelStreamRegistry;
	private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
	@Nonnull
	private final KeyGroupRange keyGroupRange;
	@Nonnegative
	private final int numberOfKeyGroups;
	private final HeapSnapshotStrategy<K> snapshotStrategy;
	private final InternalKeyContext<K> keyContext;

	HeapRestoreOperation(
		@Nonnull Collection<KeyedStateHandle> restoreStateHandles,
		StateSerializerProvider<K> keySerializerProvider,
		ClassLoader userCodeClassLoader,
		Map<String, StateTable<K, ?, ?>> registeredKVStates,
		Map<String, HeapPriorityQueueSnapshotRestoreWrapper> registeredPQStates,
		CloseableRegistry cancelStreamRegistry,
		HeapPriorityQueueSetFactory priorityQueueSetFactory,
		@Nonnull KeyGroupRange keyGroupRange,
		int numberOfKeyGroups,
		HeapSnapshotStrategy<K> snapshotStrategy,
		InternalKeyContext<K> keyContext) {
		this.restoreStateHandles = restoreStateHandles;
		this.keySerializerProvider = keySerializerProvider;
		this.userCodeClassLoader = userCodeClassLoader;
		this.registeredKVStates = registeredKVStates;
		this.registeredPQStates = registeredPQStates;
		this.cancelStreamRegistry = cancelStreamRegistry;
		this.priorityQueueSetFactory = priorityQueueSetFactory;
		this.keyGroupRange = keyGroupRange;
		this.numberOfKeyGroups = numberOfKeyGroups;
		this.snapshotStrategy = snapshotStrategy;
		this.keyContext = keyContext;
	}

	@Override
	public Void restore() throws Exception {
		registeredKVStates.clear();
		registeredPQStates.clear();
		return null;
	}

}
