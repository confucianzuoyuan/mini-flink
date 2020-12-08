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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.shaded.guava18.com.google.common.io.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Optional;

/**
 * Class encapsulating various state backend handling logic for {@link StreamOperator} implementations.
 */
@Internal
public class StreamOperatorStateHandler {

	protected static final Logger LOG = LoggerFactory.getLogger(StreamOperatorStateHandler.class);

	/** Backend for keyed state. This might be empty if we're not on a keyed stream. */
	@Nullable
	private final AbstractKeyedStateBackend<?> keyedStateBackend;
	private final CloseableRegistry closeableRegistry;
	@Nullable
	private final DefaultKeyedStateStore keyedStateStore;
	private final OperatorStateBackend operatorStateBackend;
	private final StreamOperatorStateContext context;

	public StreamOperatorStateHandler(
			StreamOperatorStateContext context,
			ExecutionConfig executionConfig,
			CloseableRegistry closeableRegistry) {
		this.context = context;
		operatorStateBackend = context.operatorStateBackend();
		keyedStateBackend = context.keyedStateBackend();
		this.closeableRegistry = closeableRegistry;

		if (keyedStateBackend != null) {
			keyedStateStore = new DefaultKeyedStateStore(keyedStateBackend, executionConfig);
		}
		else {
			keyedStateStore = null;
		}
	}

	public void dispose() throws Exception {
		try (Closer closer = Closer.create()) {
			if (closeableRegistry.unregisterCloseable(operatorStateBackend)) {
				closer.register(operatorStateBackend);
			}
			if (closeableRegistry.unregisterCloseable(keyedStateBackend)) {
				closer.register(keyedStateBackend);
			}
			if (operatorStateBackend != null) {
				closer.register(() -> operatorStateBackend.dispose());
			}
			if (keyedStateBackend != null) {
				closer.register(() -> keyedStateBackend.dispose());
			}
		}
	}

	@SuppressWarnings("unchecked")
	public <K> KeyedStateBackend<K> getKeyedStateBackend() {
		return (KeyedStateBackend<K>) keyedStateBackend;
	}

	public OperatorStateBackend getOperatorStateBackend() {
		return operatorStateBackend;
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	protected <S extends State, N> S getPartitionedState(
			N namespace,
			TypeSerializer<N> namespaceSerializer,
			StateDescriptor<S, ?> stateDescriptor) throws Exception {

		/*
	    TODO: NOTE: This method does a lot of work caching / retrieving states just to update the namespace.
	    This method should be removed for the sake of namespaces being lazily fetched from the keyed
	    state backend, or being set on the state directly.
	    */

		if (keyedStateStore != null) {
			return keyedStateBackend.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
		} else {
			throw new RuntimeException("Cannot create partitioned state. The keyed state " +
				"backend has not been set. This indicates that the operator is not " +
				"partitioned/keyed.");
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setCurrentKey(Object key) {
		if (keyedStateBackend != null) {
			try {
				// need to work around type restrictions
				@SuppressWarnings("unchecked,rawtypes")
				AbstractKeyedStateBackend rawBackend = (AbstractKeyedStateBackend) keyedStateBackend;

				rawBackend.setCurrentKey(key);
			} catch (Exception e) {
				throw new RuntimeException("Exception occurred while setting the current key context.", e);
			}
		}
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public Object getCurrentKey() {
		if (keyedStateBackend != null) {
			return keyedStateBackend.getCurrentKey();
		} else {
			throw new UnsupportedOperationException("Key can only be retrieved on KeyedStream.");
		}
	}

	public Optional<KeyedStateStore> getKeyedStateStore() {
		return Optional.ofNullable(keyedStateStore);
	}

	/**
	 * Custom state handling hooks to be invoked by {@link StreamOperatorStateHandler}.
	 */
	public interface CheckpointedStreamOperator {
	}
}
