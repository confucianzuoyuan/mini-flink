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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.functions.util.AbstractRuntimeUDFContext;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;

import javax.annotation.Nullable;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Implementation of the {@link org.apache.flink.api.common.functions.RuntimeContext},
 * for streaming operators.
 */
@Internal
public class StreamingRuntimeContext extends AbstractRuntimeUDFContext {

	/** The task environment running the operator. */
	private final Environment taskEnvironment;
	private final StreamConfig streamConfig;
	private final String operatorUniqueID;
	private final ProcessingTimeService processingTimeService;
	private @Nullable KeyedStateStore keyedStateStore;

	@VisibleForTesting
	public StreamingRuntimeContext(
			AbstractStreamOperator<?> operator,
			Environment env,
			Map<String, Accumulator<?, ?>> accumulators) {
		this(
			env,
			accumulators,
			operator.getOperatorID(),
			operator.getProcessingTimeService(),
			operator.getKeyedStateStore());
	}

	public StreamingRuntimeContext(
			Environment env,
			Map<String, Accumulator<?, ?>> accumulators,
			OperatorID operatorID,
			ProcessingTimeService processingTimeService,
			@Nullable KeyedStateStore keyedStateStore) {
		super(checkNotNull(env).getTaskInfo(),
				env.getUserClassLoader(),
				env.getExecutionConfig(),
				accumulators);
		this.taskEnvironment = env;
		this.streamConfig = new StreamConfig(env.getTaskConfiguration());
		this.operatorUniqueID = checkNotNull(operatorID).toString();
		this.processingTimeService = processingTimeService;
		this.keyedStateStore = keyedStateStore;
	}

	public void setKeyedStateStore(@Nullable KeyedStateStore keyedStateStore) {
		this.keyedStateStore = keyedStateStore;
	}

	// ------------------------------------------------------------------------

	/**
	 * Returns the input split provider associated with the operator.
	 *
	 * @return The input split provider.
	 */
	public InputSplitProvider getInputSplitProvider() {
		return taskEnvironment.getInputSplitProvider();
	}

	public ProcessingTimeService getProcessingTimeService() {
		return processingTimeService;
	}

	// ------------------------------------------------------------------------
	//  key/value state
	// ------------------------------------------------------------------------

	@Override
	public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties) {
		KeyedStateStore keyedStateStore = checkPreconditionsAndGetKeyedStateStore(stateProperties);
		stateProperties.initializeSerializerUnlessSet(getExecutionConfig());
		return keyedStateStore.getState(stateProperties);
	}

	private KeyedStateStore checkPreconditionsAndGetKeyedStateStore(StateDescriptor<?, ?> stateDescriptor) {
		checkNotNull(stateDescriptor, "The state properties must not be null");
		checkNotNull(keyedStateStore, "Keyed state can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.");
		return keyedStateStore;
	}

}
