/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.queryablestate.KvStateID;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.*;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.NoOpFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.jobmanager.PartitionProducerDisposedException;
import org.apache.flink.runtime.jobmaster.SerializedInputSplit;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinatorHolder;
import org.apache.flink.runtime.query.KvStateLocation;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.query.UnknownKvStateLocation;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.function.FunctionUtils;
import org.slf4j.Logger;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class which can be used to implement {@link SchedulerNG}.
 */
public abstract class SchedulerBase implements SchedulerNG {

	private final Logger log;

	private final JobGraph jobGraph;

	private final ExecutionGraph executionGraph;

	private final SchedulingTopology schedulingTopology;

	private final InputsLocationsRetriever inputsLocationsRetriever;

	private final Executor ioExecutor;

	private final Configuration jobMasterConfiguration;

	private final SlotProvider slotProvider;

	private final ScheduledExecutorService futureExecutor;

	private final ClassLoader userCodeLoader;

	private final Time rpcTimeout;

	private final BlobWriter blobWriter;

	private final Time slotRequestTimeout;

	private final boolean legacyScheduling;

	protected final ExecutionVertexVersioner executionVertexVersioner;

	private final Map<OperatorID, OperatorCoordinatorHolder> coordinatorMap;

	private ComponentMainThreadExecutor mainThreadExecutor = new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
		"SchedulerBase is not initialized with proper main thread executor. " +
			"Call to SchedulerBase.setMainThreadExecutor(...) required.");

	public SchedulerBase(
		final Logger log,
		final JobGraph jobGraph,
		final Executor ioExecutor,
		final Configuration jobMasterConfiguration,
		final SlotProvider slotProvider,
		final ScheduledExecutorService futureExecutor,
		final ClassLoader userCodeLoader,
		final Time rpcTimeout,
		final BlobWriter blobWriter,
		final Time slotRequestTimeout,
		final ShuffleMaster<?> shuffleMaster,
		final JobMasterPartitionTracker partitionTracker,
		final ExecutionVertexVersioner executionVertexVersioner,
		final boolean legacyScheduling) throws Exception {

		this.log = checkNotNull(log);
		this.jobGraph = checkNotNull(jobGraph);
		this.ioExecutor = checkNotNull(ioExecutor);
		this.jobMasterConfiguration = checkNotNull(jobMasterConfiguration);
		this.slotProvider = checkNotNull(slotProvider);
		this.futureExecutor = checkNotNull(futureExecutor);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.rpcTimeout = checkNotNull(rpcTimeout);

		final RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration =
			jobGraph.getSerializedExecutionConfig()
				.deserializeValue(userCodeLoader)
				.getRestartStrategy();

		this.blobWriter = checkNotNull(blobWriter);
		this.slotRequestTimeout = checkNotNull(slotRequestTimeout);
		this.executionVertexVersioner = checkNotNull(executionVertexVersioner);
		this.legacyScheduling = legacyScheduling;

		this.executionGraph = createAndRestoreExecutionGraph(checkNotNull(shuffleMaster), checkNotNull(partitionTracker));
		this.schedulingTopology = executionGraph.getSchedulingTopology();

		this.inputsLocationsRetriever = new ExecutionGraphToInputsLocationsRetrieverAdapter(executionGraph);

		this.coordinatorMap = createCoordinatorMap();
	}

	private ExecutionGraph createAndRestoreExecutionGraph(
		ShuffleMaster<?> shuffleMaster,
		JobMasterPartitionTracker partitionTracker) throws Exception {

		ExecutionGraph newExecutionGraph = createExecutionGraph(shuffleMaster, partitionTracker);

		System.out.println(newExecutionGraph.getJsonPlan());

		return newExecutionGraph;
	}

	private ExecutionGraph createExecutionGraph(
		ShuffleMaster<?> shuffleMaster,
		final JobMasterPartitionTracker partitionTracker) throws JobExecutionException, JobException {

		final FailoverStrategy.Factory failoverStrategy = new NoOpFailoverStrategy.Factory();

		return ExecutionGraphBuilder.buildGraph(
			null,
			jobGraph,
			jobMasterConfiguration,
			futureExecutor,
			ioExecutor,
			slotProvider,
			userCodeLoader,
			rpcTimeout,
			blobWriter,
			slotRequestTimeout,
			log,
			shuffleMaster,
			partitionTracker,
			failoverStrategy);
	}

	protected void transitionToScheduled(final List<ExecutionVertexID> verticesToDeploy) {
		verticesToDeploy.forEach(executionVertexId -> getExecutionVertex(executionVertexId)
			.getCurrentExecutionAttempt()
			.transitionState(ExecutionState.SCHEDULED));
	}

	protected ComponentMainThreadExecutor getMainThreadExecutor() {
		return mainThreadExecutor;
	}

	protected final SchedulingTopology getSchedulingTopology() {
		return schedulingTopology;
	}

	protected final ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker() {
		return executionGraph.getResultPartitionAvailabilityChecker();
	}

	protected final InputsLocationsRetriever getInputsLocationsRetriever() {
		return inputsLocationsRetriever;
	}

	protected final void prepareExecutionGraphForNgScheduling() {
		executionGraph.enableNgScheduling(new UpdateSchedulerNgOnInternalFailuresListener(this, jobGraph.getJobID()));
		executionGraph.transitionToRunning();
	}

	public ExecutionVertex getExecutionVertex(final ExecutionVertexID executionVertexId) {
		return executionGraph.getAllVertices().get(executionVertexId.getJobVertexId()).getTaskVertices()[executionVertexId.getSubtaskIndex()];
	}

	protected JobGraph getJobGraph() {
		return jobGraph;
	}

	protected abstract long getNumberOfRestarts();

	private Map<ExecutionVertexID, ExecutionVertexVersion> incrementVersionsOfAllVertices() {
		return executionVertexVersioner.recordVertexModifications(
			IterableUtils.toStream(schedulingTopology.getVertices())
				.map(SchedulingExecutionVertex::getId)
				.collect(Collectors.toSet()));
	}

	// ------------------------------------------------------------------------
	// SchedulerNG
	// ------------------------------------------------------------------------

	@Override
	public void setMainThreadExecutor(final ComponentMainThreadExecutor mainThreadExecutor) {
		this.mainThreadExecutor = checkNotNull(mainThreadExecutor);
		executionGraph.start(mainThreadExecutor);
	}

	@Override
	public void registerJobStatusListener(final JobStatusListener jobStatusListener) {
		executionGraph.registerJobStatusListener(jobStatusListener);
	}

	@Override
	public final void startScheduling() {
		// 开始调度
		startSchedulingInternal();
	}

	protected abstract void startSchedulingInternal();

	@Override
	public void suspend(Throwable cause) {
		mainThreadExecutor.assertRunningInMainThread();

		incrementVersionsOfAllVertices();
		disposeAllOperatorCoordinators();
	}

	@Override
	public void cancel() {
		mainThreadExecutor.assertRunningInMainThread();

		incrementVersionsOfAllVertices();
	}

	@Override
	public CompletableFuture<Void> getTerminationFuture() {
		return executionGraph.getTerminationFuture().thenApply(FunctionUtils.nullFn());
	}

	@Override
	public final boolean updateTaskExecutionState(final TaskExecutionState taskExecutionState) {
		boolean updateSuccess = executionGraph.updateState(taskExecutionState);
		return true;
	}


	@Override
	public SerializedInputSplit requestNextInputSplit(JobVertexID vertexID, ExecutionAttemptID executionAttempt) throws IOException {
		mainThreadExecutor.assertRunningInMainThread();

		final Execution execution = executionGraph.getRegisteredExecutions().get(executionAttempt);
		if (execution == null) {
			// can happen when JobManager had already unregistered this execution upon on task failure,
			// but TaskManager get some delay to aware of that situation
			if (log.isDebugEnabled()) {
				log.debug("Can not find Execution for attempt {}.", executionAttempt);
			}
			// but we should TaskManager be aware of this
			throw new IllegalArgumentException("Can not find Execution for attempt " + executionAttempt);
		}

		final ExecutionJobVertex vertex = executionGraph.getJobVertex(vertexID);
		if (vertex == null) {
			throw new IllegalArgumentException("Cannot find execution vertex for vertex ID " + vertexID);
		}

		if (vertex.getSplitAssigner() == null) {
			throw new IllegalStateException("No InputSplitAssigner for vertex ID " + vertexID);
		}

		final InputSplit nextInputSplit = execution.getNextInputSplit();

		if (log.isDebugEnabled()) {
			log.debug("Send next input split {}.", nextInputSplit);
		}

		try {
			final byte[] serializedInputSplit = InstantiationUtil.serializeObject(nextInputSplit);
			return new SerializedInputSplit(serializedInputSplit);
		} catch (Exception ex) {
			IOException reason = new IOException("Could not serialize the next input split of class " +
				nextInputSplit.getClass() + ".", ex);
			vertex.fail(reason);
			throw reason;
		}
	}

	@Override
	public ExecutionState requestPartitionState(
		final IntermediateDataSetID intermediateResultId,
		final ResultPartitionID resultPartitionId) throws PartitionProducerDisposedException {

		mainThreadExecutor.assertRunningInMainThread();

		final Execution execution = executionGraph.getRegisteredExecutions().get(resultPartitionId.getProducerId());
		if (execution != null) {
			return execution.getState();
		}
		else {
			final IntermediateResult intermediateResult =
				executionGraph.getAllIntermediateResults().get(intermediateResultId);

			if (intermediateResult != null) {
				// Try to find the producing execution
				Execution producerExecution = intermediateResult
					.getPartitionById(resultPartitionId.getPartitionId())
					.getProducer()
					.getCurrentExecutionAttempt();

				if (producerExecution.getAttemptId().equals(resultPartitionId.getProducerId())) {
					return producerExecution.getState();
				} else {
					throw new PartitionProducerDisposedException(resultPartitionId);
				}
			} else {
				throw new IllegalArgumentException("Intermediate data set with ID "
					+ intermediateResultId + " not found.");
			}
		}
	}

	@Override
	public final void scheduleOrUpdateConsumers(final ResultPartitionID partitionId) {
		mainThreadExecutor.assertRunningInMainThread();

		try {
			executionGraph.scheduleOrUpdateConsumers(partitionId);
		} catch (ExecutionGraphException e) {
			throw new RuntimeException(e);
		}

		scheduleOrUpdateConsumersInternal(partitionId.getPartitionId());
	}

	protected void scheduleOrUpdateConsumersInternal(IntermediateResultPartitionID resultPartitionId) {
	}

	@Override
	public ArchivedExecutionGraph requestJob() {
		mainThreadExecutor.assertRunningInMainThread();
		return ArchivedExecutionGraph.createFrom(executionGraph);
	}

	@Override
	public JobStatus requestJobStatus() {
		return executionGraph.getState();
	}

	@Override
	public KvStateLocation requestKvStateLocation(final JobID jobId, final String registrationName) throws UnknownKvStateLocation, FlinkJobNotFoundException {
		mainThreadExecutor.assertRunningInMainThread();

		// sanity check for the correct JobID
		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Lookup key-value state for job {} with registration " +
					"name {}.", jobGraph.getJobID(), registrationName);
			}

			final KvStateLocationRegistry registry = executionGraph.getKvStateLocationRegistry();
			final KvStateLocation location = registry.getKvStateLocation(registrationName);
			if (location != null) {
				return location;
			} else {
				throw new UnknownKvStateLocation(registrationName);
			}
		} else {
			if (log.isDebugEnabled()) {
				log.debug("Request of key-value state location for unknown job {} received.", jobId);
			}
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void notifyKvStateRegistered(final JobID jobId, final JobVertexID jobVertexId, final KeyGroupRange keyGroupRange, final String registrationName, final KvStateID kvStateId, final InetSocketAddress kvStateServerAddress) throws FlinkJobNotFoundException {
		mainThreadExecutor.assertRunningInMainThread();

		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state registered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraph.getKvStateLocationRegistry().notifyKvStateRegistered(
					jobVertexId, keyGroupRange, registrationName, kvStateId, kvStateServerAddress);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void notifyKvStateUnregistered(final JobID jobId, final JobVertexID jobVertexId, final KeyGroupRange keyGroupRange, final String registrationName) throws FlinkJobNotFoundException {
		mainThreadExecutor.assertRunningInMainThread();

		if (jobGraph.getJobID().equals(jobId)) {
			if (log.isDebugEnabled()) {
				log.debug("Key value state unregistered for job {} under name {}.",
					jobGraph.getJobID(), registrationName);
			}

			try {
				executionGraph.getKvStateLocationRegistry().notifyKvStateUnregistered(
					jobVertexId, keyGroupRange, registrationName);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		} else {
			throw new FlinkJobNotFoundException(jobId);
		}
	}

	@Override
	public void updateAccumulators(final AccumulatorSnapshot accumulatorSnapshot) {
		mainThreadExecutor.assertRunningInMainThread();

		executionGraph.updateAccumulators(accumulatorSnapshot);
	}

	private void disposeAllOperatorCoordinators() {
		getAllCoordinators().forEach(IOUtils::closeQuietly);
	}

	private Collection<OperatorCoordinatorHolder> getAllCoordinators() {
		return coordinatorMap.values();
	}

	private Map<OperatorID, OperatorCoordinatorHolder> createCoordinatorMap() {
		Map<OperatorID, OperatorCoordinatorHolder> coordinatorMap = new HashMap<>();
		return coordinatorMap;
	}
}
