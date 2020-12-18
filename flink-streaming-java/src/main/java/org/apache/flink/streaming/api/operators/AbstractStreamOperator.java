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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamOperatorStateHandler.CheckpointedStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.LatencyStats;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Optional;

@PublicEvolving
public abstract class AbstractStreamOperator<OUT>
		implements StreamOperator<OUT>, SetupableStreamOperator<OUT>, CheckpointedStreamOperator, Serializable {

	private static final long serialVersionUID = 1L;

	/** The logger used by the operator class and its subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(AbstractStreamOperator.class);

	// ----------- configuration properties -------------

	// A sane default for most operators
	protected ChainingStrategy chainingStrategy = ChainingStrategy.HEAD;

	// ---------------- runtime fields ------------------

	/** The task that contains this operator (and other operators in the same chain). */
	private transient StreamTask<?, ?> container;

	protected transient StreamConfig config;

	protected transient Output<StreamRecord<OUT>> output;

	/** The runtime context for UDFs. */
	private transient StreamingRuntimeContext runtimeContext;

	// ---------------- key/value state ------------------

	/**
	 * {@code KeySelector} for extracting a key from an element being processed. This is used to
	 * scope keyed state to a key. This is null if the operator is not a keyed operator.
	 *
	 * <p>This is for elements from the first input.
	 */
	private transient KeySelector<?, ?> stateKeySelector1;

	/**
	 * {@code KeySelector} for extracting a key from an element being processed. This is used to
	 * scope keyed state to a key. This is null if the operator is not a keyed operator.
	 *
	 * <p>This is for elements from the second input.
	 */
	private transient KeySelector<?, ?> stateKeySelector2;

	private transient StreamOperatorStateHandler stateHandler;

	private transient InternalTimeServiceManager<?> timeServiceManager;

	// --------------- Metrics ---------------------------

	protected transient LatencyStats latencyStats;

	// ---------------- time handler ------------------

	protected transient ProcessingTimeService processingTimeService;

	// ---------------- two-input operator watermarks ------------------

	// We keep track of watermarks from both inputs, the combined input is the minimum
	// Once the minimum advances we emit a new watermark for downstream operators
	private long combinedWatermark = Long.MIN_VALUE;
	private long input1Watermark = Long.MIN_VALUE;
	private long input2Watermark = Long.MIN_VALUE;

	// ------------------------------------------------------------------------
	//  Life Cycle
	// ------------------------------------------------------------------------

	@Override
	public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<OUT>> output) {
		final Environment environment = containingTask.getEnvironment();
		this.container = containingTask;
		this.config = config;
		try {
			this.output = new CountingOutput<>(output);
		} catch (Exception e) {
			this.output = output;
		}

		this.runtimeContext = new StreamingRuntimeContext(
			environment,
			environment.getAccumulatorRegistry().getUserMap(),
			getOperatorID(),
			getProcessingTimeService(),
			null);

		stateKeySelector1 = config.getStatePartitioner(0, getUserCodeClassloader());
		stateKeySelector2 = config.getStatePartitioner(1, getUserCodeClassloader());
	}

	/**
	 * @deprecated The {@link ProcessingTimeService} instance should be passed by the operator
	 * constructor and this method will be removed along with {@link SetupableStreamOperator}.
	 */
	@Deprecated
	public void setProcessingTimeService(ProcessingTimeService processingTimeService) {
		this.processingTimeService = Preconditions.checkNotNull(processingTimeService);
	}

	@Override
	public final void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {

		final TypeSerializer<?> keySerializer = config.getStateKeySerializer(getUserCodeClassloader());

		final StreamTask<?, ?> containingTask =
			Preconditions.checkNotNull(getContainingTask());
		final CloseableRegistry streamTaskCloseableRegistry =
			Preconditions.checkNotNull(containingTask.getCancelables());

		final StreamOperatorStateContext context =
			streamTaskStateManager.streamOperatorStateContext(
				getOperatorID(),
				getClass().getSimpleName(),
				getProcessingTimeService(),
				this,
				keySerializer,
				streamTaskCloseableRegistry);

		stateHandler = new StreamOperatorStateHandler(context, getExecutionConfig(), streamTaskCloseableRegistry);
		timeServiceManager = context.internalTimerServiceManager();
		runtimeContext.setKeyedStateStore(stateHandler.getKeyedStateStore().orElse(null));
	}

	/**
	 * This method is called immediately before any elements are processed, it should contain the
	 * operator's initialization logic, e.g. state initialization.
	 *
	 * <p>The default implementation does nothing.
	 *
	 * @throws Exception An exception in this method causes the operator to fail.
	 */
	@Override
	public void open() throws Exception {}

	@Override
	public void close() throws Exception {}

	/**
	 * This method is called at the very end of the operator's life, both in the case of a successful
	 * completion of the operation, and in the case of a failure and canceling.
	 *
	 * <p>This method is expected to make a thorough effort to release all resources
	 * that the operator has acquired.
	 */
	@Override
	public void dispose() throws Exception {
		if (stateHandler != null) {
			stateHandler.dispose();
		}
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		// the default implementation does nothing and accepts the checkpoint
		// this is purely for subclasses to override
	}

	// ------------------------------------------------------------------------
	//  Properties and Services
	// ------------------------------------------------------------------------

	/**
	 * Gets the execution config defined on the execution environment of the job to which this
	 * operator belongs.
	 *
	 * @return The job's execution config.
	 */
	public ExecutionConfig getExecutionConfig() {
		return container.getExecutionConfig();
	}

	public StreamConfig getOperatorConfig() {
		return config;
	}

	public StreamTask<?, ?> getContainingTask() {
		return container;
	}

	public ClassLoader getUserCodeClassloader() {
		return container.getUserCodeClassLoader();
	}

	/**
	 * Return the operator name. If the runtime context has been set, then the task name with
	 * subtask index is returned. Otherwise, the simple class name is returned.
	 *
	 * @return If runtime context is set, then return task name with subtask index. Otherwise return
	 * 			simple class name.
	 */
	protected String getOperatorName() {
		if (runtimeContext != null) {
			return runtimeContext.getTaskNameWithSubtasks();
		} else {
			return getClass().getSimpleName();
		}
	}

	/**
	 * Returns a context that allows the operator to query information about the execution and also
	 * to interact with systems such as broadcast variables and managed state. This also allows
	 * to register timers.
	 */
	@VisibleForTesting
	public StreamingRuntimeContext getRuntimeContext() {
		return runtimeContext;
	}

	@VisibleForTesting
	public <K> KeyedStateBackend<K> getKeyedStateBackend() {
		return stateHandler.getKeyedStateBackend();
	}

	@VisibleForTesting
	public OperatorStateBackend getOperatorStateBackend() {
		return stateHandler.getOperatorStateBackend();
	}

	/**
	 * Returns the {@link ProcessingTimeService} responsible for getting the current
	 * processing time and registering timers.
	 */
	@VisibleForTesting
	public ProcessingTimeService getProcessingTimeService() {
		return processingTimeService;
	}

	/**
	 * Creates a partitioned state handle, using the state backend configured for this task.
	 *
	 * @throws IllegalStateException Thrown, if the key/value state was already initialized.
	 * @throws Exception Thrown, if the state backend cannot create the key/value state.
	 */
	protected <S extends State> S getPartitionedState(StateDescriptor<S, ?> stateDescriptor) throws Exception {
		return getPartitionedState(VoidNamespace.INSTANCE, VoidNamespaceSerializer.INSTANCE, stateDescriptor);
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
		return stateHandler.getPartitionedState(namespace, namespaceSerializer, stateDescriptor);
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContextElement1(StreamRecord record) throws Exception {
		setKeyContextElement(record, stateKeySelector1);
	}

	@Override
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void setKeyContextElement2(StreamRecord record) throws Exception {
		setKeyContextElement(record, stateKeySelector2);
	}

	private <T> void setKeyContextElement(StreamRecord<T> record, KeySelector<T, ?> selector) throws Exception {
		if (selector != null) {
			Object key = selector.getKey(record.getValue());
			setCurrentKey(key);
		}
	}

	public void setCurrentKey(Object key) {
		stateHandler.setCurrentKey(key);
	}

	public Object getCurrentKey() {
		return stateHandler.getCurrentKey();
	}

	public KeyedStateStore getKeyedStateStore() {
		if (stateHandler == null) {
			return null;
		}
		return stateHandler.getKeyedStateStore().orElse(null);
	}

	// ------------------------------------------------------------------------
	//  Context and chaining properties
	// ------------------------------------------------------------------------

	@Override
	public final void setChainingStrategy(ChainingStrategy strategy) {
		this.chainingStrategy = strategy;
	}

	@Override
	public final ChainingStrategy getChainingStrategy() {
		return chainingStrategy;
	}

	// ------------------------------------------------------------------------
	//  Metrics
	// ------------------------------------------------------------------------

	// ------- One input stream
	public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
		reportOrForwardLatencyMarker(latencyMarker);
	}

	// ------- Two input stream
	public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
		reportOrForwardLatencyMarker(latencyMarker);
	}

	public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
		reportOrForwardLatencyMarker(latencyMarker);
	}

	protected void reportOrForwardLatencyMarker(LatencyMarker marker) {
		// all operators are tracking latencies
		this.latencyStats.reportLatency(marker);

		// everything except sinks forwards latency markers
		this.output.emitLatencyMarker(marker);
	}

	// ------------------------------------------------------------------------
	//  Watermark handling
	// ------------------------------------------------------------------------

	public void processWatermark(Watermark mark) throws Exception {
		if (timeServiceManager != null) {
			timeServiceManager.advanceWatermark(mark);
		}
		output.emitWatermark(mark);
	}

	@Override
	public OperatorID getOperatorID() {
		return config.getOperatorID();
	}

	protected Optional<InternalTimeServiceManager<?>> getTimeServiceManager() {
		return Optional.ofNullable(timeServiceManager);
	}
}
