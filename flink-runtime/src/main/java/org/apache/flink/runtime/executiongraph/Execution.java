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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.Archiveable;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.shuffle.*;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory.getConsumedPartitionShuffleDescriptor;
import static org.apache.flink.runtime.execution.ExecutionState.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A single execution of a vertex. While an {@link ExecutionVertex} can be executed multiple times
 * (for recovery, re-computation, re-configuration), this class tracks the state of a single execution
 * of that vertex and the resources.
 *
 * <h2>Lock free state transitions</h2>
 *
 * <p>In several points of the code, we need to deal with possible concurrent state changes and actions.
 * For example, while the call to deploy a task (send it to the TaskManager) happens, the task gets cancelled.
 *
 * <p>We could lock the entire portion of the code (decision to deploy, deploy, set state to running) such that
 * it is guaranteed that any "cancel command" will only pick up after deployment is done and that the "cancel
 * command" call will never overtake the deploying call.
 *
 * <p>This blocks the threads big time, because the remote calls may take long. Depending of their locking behavior, it
 * may even result in distributed deadlocks (unless carefully avoided). We therefore use atomic state updates and
 * occasional double-checking to ensure that the state after a completed call is as expected, and trigger correcting
 * actions if it is not. Many actions are also idempotent (like canceling).
 */
public class Execution implements AccessExecution, Archiveable<ArchivedExecution>, LogicalSlot.Payload {

	private static final Logger LOG = ExecutionGraph.LOG;

	private static final int NUM_CANCEL_CALL_TRIES = 3;

	// --------------------------------------------------------------------------------------------

	/** The executor which is used to execute futures. */
	private final Executor executor;

	/** The execution vertex whose task this execution executes. */
	private final ExecutionVertex vertex;

	/** The unique ID marking the specific execution instant of the task. */
	private final ExecutionAttemptID attemptId;

	/** Gets the global modification version of the execution graph when this execution was created.
	 * This version is bumped in the ExecutionGraph whenever a global failover happens. It is used
	 * to resolve conflicts between concurrent modification by global and local failover actions. */
	private final long globalModVersion;

	/** The timestamps when state transitions occurred, indexed by {@link ExecutionState#ordinal()}. */
	private final long[] stateTimestamps;

	private final int attemptNumber;

	private final Time rpcTimeout;

	private final Collection<PartitionInfo> partitionInfos;

	/** A future that completes once the Execution reaches a terminal ExecutionState. */
	private final CompletableFuture<ExecutionState> terminalStateFuture;

	private final CompletableFuture<?> releaseFuture;

	private final CompletableFuture<TaskManagerLocation> taskManagerLocationFuture;

	private volatile ExecutionState state = CREATED;

	private LogicalSlot assignedResource;

	private Throwable failureCause;          // once assigned, never changes

	/** Information to restore the task on recovery, such as checkpoint id and task state snapshot. */
	@Nullable
	private JobManagerTaskRestore taskRestore;

	/** This field holds the allocation id once it was assigned successfully. */
	@Nullable
	private AllocationID assignedAllocationID;

	// ------------------------ Accumulators & Metrics ------------------------

	/** Lock for updating the accumulators atomically.
	 * Prevents final accumulators to be overwritten by partial accumulators on a late heartbeat. */
	private final Object accumulatorLock = new Object();

	/* Continuously updated map of user-defined accumulators */
	private Map<String, Accumulator<?, ?>> userAccumulators;

	private Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions;

	// --------------------------------------------------------------------------------------------

	/**
	 * Creates a new Execution attempt.
	 *
	 * @param executor
	 *             The executor used to dispatch callbacks from futures and asynchronous RPC calls.
	 * @param vertex
	 *             The execution vertex to which this Execution belongs
	 * @param attemptNumber
	 *             The execution attempt number.
	 * @param globalModVersion
	 *             The global modification version of the execution graph when this execution was created
	 * @param startTimestamp
	 *             The timestamp that marks the creation of this Execution
	 * @param rpcTimeout
	 *             The rpcTimeout for RPC calls like deploy/cancel/stop.
	 */
	public Execution(
			Executor executor,
			ExecutionVertex vertex,
			int attemptNumber,
			long globalModVersion,
			long startTimestamp,
			Time rpcTimeout) {

		this.executor = checkNotNull(executor);
		this.vertex = checkNotNull(vertex);
		this.attemptId = new ExecutionAttemptID();
		this.rpcTimeout = checkNotNull(rpcTimeout);

		this.globalModVersion = globalModVersion;
		this.attemptNumber = attemptNumber;

		this.stateTimestamps = new long[ExecutionState.values().length];
		markTimestamp(CREATED, startTimestamp);

		this.partitionInfos = new ArrayList<>(16);
		this.producedPartitions = Collections.emptyMap();
		this.terminalStateFuture = new CompletableFuture<>();
		this.releaseFuture = new CompletableFuture<>();
		this.taskManagerLocationFuture = new CompletableFuture<>();

		this.assignedResource = null;
	}

	// --------------------------------------------------------------------------------------------
	//   Properties
	// --------------------------------------------------------------------------------------------

	public ExecutionVertex getVertex() {
		return vertex;
	}

	@Override
	public ExecutionAttemptID getAttemptId() {
		return attemptId;
	}

	@Override
	public int getAttemptNumber() {
		return attemptNumber;
	}

	@Override
	public ExecutionState getState() {
		return state;
	}

	@Nullable
	public AllocationID getAssignedAllocationID() {
		return assignedAllocationID;
	}

	/**
	 * Gets the global modification version of the execution graph when this execution was created.
	 *
	 * <p>This version is bumped in the ExecutionGraph whenever a global failover happens. It is used
	 * to resolve conflicts between concurrent modification by global and local failover actions.
	 */
	public long getGlobalModVersion() {
		return globalModVersion;
	}

	public CompletableFuture<TaskManagerLocation> getTaskManagerLocationFuture() {
		return taskManagerLocationFuture;
	}

	public LogicalSlot getAssignedResource() {
		return assignedResource;
	}

	public Optional<ResultPartitionDeploymentDescriptor> getResultPartitionDeploymentDescriptor(
			IntermediateResultPartitionID id) {
		return Optional.ofNullable(producedPartitions.get(id));
	}

	/**
	 * Tries to assign the given slot to the execution. The assignment works only if the
	 * Execution is in state SCHEDULED. Returns true, if the resource could be assigned.
	 *
	 * @param logicalSlot to assign to this execution
	 * @return true if the slot could be assigned to the execution, otherwise false
	 */
	public boolean tryAssignResource(final LogicalSlot logicalSlot) {

		assertRunningInJobMasterMainThread();

		checkNotNull(logicalSlot);

		// only allow to set the assigned resource in state SCHEDULED or CREATED
		// note: we also accept resource assignment when being in state CREATED for testing purposes
		if (state == SCHEDULED || state == CREATED) {
			if (assignedResource == null) {
				assignedResource = logicalSlot;
				if (logicalSlot.tryAssignPayload(this)) {
					// check for concurrent modification (e.g. cancelling call)
					if ((state == SCHEDULED || state == CREATED) && !taskManagerLocationFuture.isDone()) {
						taskManagerLocationFuture.complete(logicalSlot.getTaskManagerLocation());
						assignedAllocationID = logicalSlot.getAllocationId();
						return true;
					} else {
						// free assigned resource and return false
						assignedResource = null;
						return false;
					}
				} else {
					assignedResource = null;
					return false;
				}
			} else {
				// the slot already has another slot assigned
				return false;
			}
		} else {
			// do not allow resource assignment if we are not in state SCHEDULED
			return false;
		}
	}

	public InputSplit getNextInputSplit() {
		final LogicalSlot slot = this.getAssignedResource();
		final String host = slot != null ? slot.getTaskManagerLocation().getHostname() : null;
		return this.vertex.getNextInputSplit(host);
	}

	@Override
	public TaskManagerLocation getAssignedResourceLocation() {
		// returns non-null only when a location is already assigned
		final LogicalSlot currentAssignedResource = assignedResource;
		return currentAssignedResource != null ? currentAssignedResource.getTaskManagerLocation() : null;
	}

	public Throwable getFailureCause() {
		return failureCause;
	}

	@Override
	public String getFailureCauseAsString() {
		return ExceptionUtils.stringifyException(getFailureCause());
	}

	@Override
	public long[] getStateTimestamps() {
		return stateTimestamps;
	}

	@Override
	public long getStateTimestamp(ExecutionState state) {
		return this.stateTimestamps[state.ordinal()];
	}

	public boolean isFinished() {
		return state.isTerminal();
	}

	@Nullable
	public JobManagerTaskRestore getTaskRestore() {
		return taskRestore;
	}

	/**
	 * Sets the initial state for the execution. The serialized state is then shipped via the
	 * {@link TaskDeploymentDescriptor} to the TaskManagers.
	 *
	 * @param taskRestore information to restore the state
	 */
	public void setInitialState(@Nullable JobManagerTaskRestore taskRestore) {
		this.taskRestore = taskRestore;
	}

	/**
	 * Gets a future that completes once the task execution reaches a terminal state.
	 * The future will be completed with specific state that the execution reached.
	 * This future is always completed from the job master's main thread.
	 *
	 * @return A future which is completed once the execution reaches a terminal state
	 */
	@Override
	public CompletableFuture<ExecutionState> getTerminalStateFuture() {
		return terminalStateFuture;
	}

	/**
	 * Gets the release future which is completed once the execution reaches a terminal
	 * state and the assigned resource has been released.
	 * This future is always completed from the job master's main thread.
	 *
	 * @return A future which is completed once the assigned resource has been released
	 */
	public CompletableFuture<?> getReleaseFuture() {
		return releaseFuture;
	}

	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	public CompletableFuture<Execution> registerProducedPartitions(
			TaskManagerLocation location,
			boolean sendScheduleOrUpdateConsumersMessage) {

		assertRunningInJobMasterMainThread();

		return FutureUtils.thenApplyAsyncIfNotDone(
			registerProducedPartitions(vertex, location, attemptId, sendScheduleOrUpdateConsumersMessage),
			vertex.getExecutionGraph().getJobMasterMainThreadExecutor(),
			producedPartitionsCache -> {
				producedPartitions = producedPartitionsCache;
				startTrackingPartitions(location.getResourceID(), producedPartitionsCache.values());
				return this;
			});
	}

	/**
	 * Register producedPartitions to {@link ShuffleMaster}
	 *
	 * <p>HACK: Please notice that this method simulates asynchronous registration in a synchronous way
	 * by making sure the returned {@link CompletableFuture} from {@link ShuffleMaster#registerPartitionWithProducer}
	 * is completed immediately.
	 *
	 * <p>{@link Execution#producedPartitions} are registered through an asynchronous interface
	 * {@link ShuffleMaster#registerPartitionWithProducer} to {@link ShuffleMaster}, however they are not always
	 * accessed through callbacks. So, it is possible that {@link Execution#producedPartitions}
	 * have not been available yet when accessed (in {@link Execution#deploy} for example).
	 *
	 * <p>Since the only implementation of {@link ShuffleMaster} is {@link NettyShuffleMaster},
	 * which indeed registers producedPartition in a synchronous way, this method enforces
	 * synchronous registration under an asynchronous interface for now.
	 *
	 * <p>TODO: If asynchronous registration is needed in the future, use callbacks to access {@link Execution#producedPartitions}.
	 *
	 * @return completed future of partition deployment descriptors.
	 */
	@VisibleForTesting
	static CompletableFuture<Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor>> registerProducedPartitions(
			ExecutionVertex vertex,
			TaskManagerLocation location,
			ExecutionAttemptID attemptId,
			boolean sendScheduleOrUpdateConsumersMessage) {

		ProducerDescriptor producerDescriptor = ProducerDescriptor.create(location, attemptId);

		Collection<IntermediateResultPartition> partitions = vertex.getProducedPartitions().values();
		Collection<CompletableFuture<ResultPartitionDeploymentDescriptor>> partitionRegistrations =
			new ArrayList<>(partitions.size());

		for (IntermediateResultPartition partition : partitions) {
			PartitionDescriptor partitionDescriptor = PartitionDescriptor.from(partition);
			int maxParallelism = getPartitionMaxParallelism(partition);
			CompletableFuture<? extends ShuffleDescriptor> shuffleDescriptorFuture = vertex
				.getExecutionGraph()
				.getShuffleMaster()
				.registerPartitionWithProducer(partitionDescriptor, producerDescriptor);

			// temporary hack; the scheduler does not handle incomplete futures properly
			Preconditions.checkState(shuffleDescriptorFuture.isDone(), "ShuffleDescriptor future is incomplete.");

			CompletableFuture<ResultPartitionDeploymentDescriptor> partitionRegistration = shuffleDescriptorFuture
				.thenApply(shuffleDescriptor -> new ResultPartitionDeploymentDescriptor(
					partitionDescriptor,
					shuffleDescriptor,
					maxParallelism,
					sendScheduleOrUpdateConsumersMessage));
			partitionRegistrations.add(partitionRegistration);
		}

		return FutureUtils.combineAll(partitionRegistrations).thenApply(rpdds -> {
			Map<IntermediateResultPartitionID, ResultPartitionDeploymentDescriptor> producedPartitions =
				new LinkedHashMap<>(partitions.size());
			rpdds.forEach(rpdd -> producedPartitions.put(rpdd.getPartitionId(), rpdd));
			return producedPartitions;
		});
	}

	private static int getPartitionMaxParallelism(IntermediateResultPartition partition) {
		final List<List<ExecutionEdge>> consumers = partition.getConsumers();
		Preconditions.checkArgument(!consumers.isEmpty(), "Currently there has to be exactly one consumer in real jobs");
		List<ExecutionEdge> consumer = consumers.get(0);
		ExecutionJobVertex consumerVertex = consumer.get(0).getTarget().getJobVertex();
		int maxParallelism = consumerVertex.getMaxParallelism();
		return maxParallelism;
	}

	/**
	 * Deploys the execution to the previously assigned resource.
	 *
	 * @throws JobException if the execution cannot be deployed to the assigned resource
	 */
	public void deploy() throws JobException {
		assertRunningInJobMasterMainThread();

		final LogicalSlot slot  = assignedResource;

		// make sure exactly one deployment call happens from the correct state
		// note: the transition from CREATED to DEPLOYING is for testing purposes only
		ExecutionState previous = this.state;
		if (previous == SCHEDULED || previous == CREATED) {
			if (!transitionState(previous, DEPLOYING)) {
				// race condition, someone else beat us to the deploying call.
				// this should actually not happen and indicates a race somewhere else
				throw new IllegalStateException("Cannot deploy task: Concurrent deployment call race.");
			}
		}
		else {
			// vertex may have been cancelled, or it was already scheduled
			throw new IllegalStateException("The vertex must be in CREATED or SCHEDULED state to be deployed. Found state " + previous);
		}

		if (this != slot.getPayload()) {
			throw new IllegalStateException(
				String.format("The execution %s has not been assigned to the assigned slot.", this));
		}

		try {

			// race double check, did we fail/cancel and do we need to release the slot?
			if (this.state != DEPLOYING) {
				slot.releaseSlot(new FlinkException("Actual state of execution " + this + " (" + state + ") does not match expected state DEPLOYING."));
				return;
			}

			final TaskDeploymentDescriptor deployment = TaskDeploymentDescriptorFactory
				.fromExecutionVertex(vertex, attemptNumber)
				.createDeploymentDescriptor(
					slot.getAllocationId(),
					slot.getPhysicalSlotNumber(),
					taskRestore,
					producedPartitions.values());

			// null taskRestore to let it be GC'ed
			// 手动GC
			taskRestore = null;

			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();

			final ComponentMainThreadExecutor jobMasterMainThreadExecutor =
				vertex.getExecutionGraph().getJobMasterMainThreadExecutor();

			// 调用submitTask
			CompletableFuture.supplyAsync(() -> taskManagerGateway.submitTask(deployment, rpcTimeout), executor)
				.thenCompose(Function.identity())
				.whenCompleteAsync(
					(ack, failure) -> { },
					jobMasterMainThreadExecutor);

		}
		catch (Throwable t) {
			markFailed(t);

			if (isLegacyScheduling()) {
				ExceptionUtils.rethrow(t);
			}
		}
	}

	public void cancel() {
		// depending on the previous state, we go directly to cancelled (no cancel call necessary)
		// -- or to canceling (cancel call needs to be sent to the task manager)

		// because of several possibly previous states, we need to again loop until we make a
		// successful atomic state transition
		assertRunningInJobMasterMainThread();
		while (true) {

			ExecutionState current = this.state;

			if (current == CANCELING || current == CANCELED) {
				// already taken care of, no need to cancel again
				return;
			}

			// these two are the common cases where we need to send a cancel call
			else if (current == RUNNING || current == DEPLOYING) {
				// try to transition to canceling, if successful, send the cancel call
				if (startCancelling(NUM_CANCEL_CALL_TRIES)) {
					return;
				}
				// else: fall through the loop
			}

			else if (current == FINISHED) {
				// finished before it could be cancelled.
				// in any case, the task is removed from the TaskManager already

				// a pipelined partition whose consumer has never been deployed could still be buffered on the TM
				// release it here since pipelined partitions for FINISHED executions aren't handled elsewhere
				// covers the following cases:
				// 		a) restarts of this vertex
				// 		b) a global failure (which may result in a FAILED job state)
				sendReleaseIntermediateResultPartitionsRpcCall();

				return;
			}
			else if (current == FAILED) {
				// failed before it could be cancelled.
				// in any case, the task is removed from the TaskManager already

				return;
			}
			else if (current == CREATED || current == SCHEDULED) {
				// from here, we can directly switch to cancelled, because no task has been deployed
				if (cancelAtomically()) {
					return;
				}
				// else: fall through the loop
			}
			else {
				throw new IllegalStateException(current.name());
			}
		}
	}

	public CompletableFuture<?> suspend() {
		switch(state) {
			case RUNNING:
			case DEPLOYING:
			case CREATED:
			case SCHEDULED:
				if (!cancelAtomically()) {
					throw new IllegalStateException(
						String.format("Could not directly go to %s from %s.", CANCELED.name(), state.name()));
				}
				break;
			case CANCELING:
				completeCancelling();
				break;
			case FINISHED:
				// a pipelined partition whose consumer has never been deployed could still be buffered on the TM
				// release it here since pipelined partitions for FINISHED executions aren't handled elsewhere
				// most notably, the TaskExecutor does not release pipelined partitions when disconnecting from the JM
				sendReleaseIntermediateResultPartitionsRpcCall();
				break;
			case FAILED:
			case CANCELED:
				break;
			default:
				throw new IllegalStateException(state.name());
		}

		return releaseFuture;
	}

	void scheduleOrUpdateConsumers(List<List<ExecutionEdge>> allConsumers) {
		assertRunningInJobMasterMainThread();

		final HashSet<ExecutionVertex> consumerDeduplicator = new HashSet<>();
		scheduleOrUpdateConsumers(allConsumers, consumerDeduplicator);
	}

	private void scheduleOrUpdateConsumers(
			final List<List<ExecutionEdge>> allConsumers,
			final HashSet<ExecutionVertex> consumerDeduplicator) {

		if (allConsumers.size() == 0) {
			return;
		}
		if (allConsumers.size() > 1) {
			fail(new IllegalStateException("Currently, only a single consumer group per partition is supported."));
			return;
		}

		for (ExecutionEdge edge : allConsumers.get(0)) {
			final ExecutionVertex consumerVertex = edge.getTarget();
			final Execution consumer = consumerVertex.getCurrentExecutionAttempt();
			final ExecutionState consumerState = consumer.getState();

			// ----------------------------------------------------------------
			// Consumer is created => needs to be scheduled
			// ----------------------------------------------------------------
			if (consumerState == CREATED) {
			}
			// ----------------------------------------------------------------
			// Consumer is running => send update message now
			// Consumer is deploying => cache the partition info which would be
			// sent after switching to running
			// ----------------------------------------------------------------
			else if (consumerState == DEPLOYING || consumerState == RUNNING) {
				final PartitionInfo partitionInfo = createPartitionInfo(edge);

				if (consumerState == DEPLOYING) {
					consumerVertex.cachePartitionInfo(partitionInfo);
				} else {
					consumer.sendUpdatePartitionInfoRpcCall(Collections.singleton(partitionInfo));
				}
			}
		}
	}

	private static PartitionInfo createPartitionInfo(ExecutionEdge executionEdge) {
		IntermediateDataSetID intermediateDataSetID = executionEdge.getSource().getIntermediateResult().getId();
		ShuffleDescriptor shuffleDescriptor = getConsumedPartitionShuffleDescriptor(executionEdge, false);
		return new PartitionInfo(intermediateDataSetID, shuffleDescriptor);
	}

	/**
	 * This method fails the vertex due to an external condition. The task will move to state FAILED.
	 * If the task was in state RUNNING or DEPLOYING before, it will send a cancel call to the TaskManager.
	 *
	 * @param t The exception that caused the task to fail.
	 */
	@Override
	public void fail(Throwable t) {
	}

	// --------------------------------------------------------------------------------------------
	//   Callbacks
	// --------------------------------------------------------------------------------------------

	/**
	 * This method marks the task as failed, but will make no attempt to remove task execution from the task manager.
	 * It is intended for cases where the task is known not to be running, or then the TaskManager reports failure
	 * (in which case it has already removed the task).
	 *
	 * @param t The exception that caused the task to fail.
	 */
	void markFailed(Throwable t) {
	}

	void markFinished(Map<String, Accumulator<?, ?>> userAccumulators) {

		assertRunningInJobMasterMainThread();

		// this call usually comes during RUNNING, but may also come while still in deploying (very fast tasks!)
		while (true) {
			ExecutionState current = this.state;

			if (current == RUNNING || current == DEPLOYING) {

				if (transitionState(current, FINISHED)) {
					try {
						finishPartitionsAndScheduleOrUpdateConsumers();
						updateAccumulatorsAndMetrics(userAccumulators);
						releaseAssignedResource(null);
						vertex.getExecutionGraph().deregisterExecution(this);
					}
					finally {
						vertex.executionFinished(this);
					}
					return;
				}
			}
			else if (current == CANCELING) {
				// we sent a cancel call, and the task manager finished before it arrived. We
				// will never get a CANCELED call back from the job manager
				completeCancelling(userAccumulators, true);
				return;
			}
			else if (current == CANCELED || current == FAILED) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Task FINISHED, but concurrently went to state " + state);
				}
				return;
			}
			else {
				// this should not happen, we need to fail this
				markFailed(new Exception("Vertex received FINISHED message while being in state " + state));
				return;
			}
		}
	}

	private void finishPartitionsAndScheduleOrUpdateConsumers() {
		final List<IntermediateResultPartition> newlyFinishedResults = getVertex().finishAllBlockingPartitions();
		if (newlyFinishedResults.isEmpty()) {
			return;
		}

		final HashSet<ExecutionVertex> consumerDeduplicator = new HashSet<>();

		for (IntermediateResultPartition finishedPartition : newlyFinishedResults) {
			final IntermediateResultPartition[] allPartitionsOfNewlyFinishedResults =
					finishedPartition.getIntermediateResult().getPartitions();

			for (IntermediateResultPartition partition : allPartitionsOfNewlyFinishedResults) {
				scheduleOrUpdateConsumers(partition.getConsumers(), consumerDeduplicator);
			}
		}
	}

	private boolean cancelAtomically() {
		if (startCancelling(0)) {
			completeCancelling();
			return true;
		} else {
			return false;
		}
	}

	private boolean startCancelling(int numberCancelRetries) {
		if (transitionState(state, CANCELING)) {
			taskManagerLocationFuture.cancel(false);
			sendCancelRpcCall(numberCancelRetries);
			return true;
		} else {
			return false;
		}
	}

	void completeCancelling() {
		completeCancelling(null, true);
	}

	void completeCancelling(Map<String, Accumulator<?, ?>> userAccumulators, boolean releasePartitions) {

		// the taskmanagers can themselves cancel tasks without an external trigger, if they find that the
		// network stack is canceled (for example by a failing / canceling receiver or sender
		// this is an artifact of the old network runtime, but for now we need to support task transitions
		// from running directly to canceled

		while (true) {
			ExecutionState current = this.state;

			if (current == CANCELED) {
				return;
			}
			else if (current == CANCELING || current == RUNNING || current == DEPLOYING) {

				updateAccumulatorsAndMetrics(userAccumulators);

				if (transitionState(current, CANCELED)) {
					finishCancellation(releasePartitions);
					return;
				}

				// else fall through the loop
			}
			else {
				// failing in the meantime may happen and is no problem.
				// anything else is a serious problem !!!
				if (current != FAILED) {
					String message = String.format("Asynchronous race: Found %s in state %s after successful cancel call.", vertex.getTaskNameWithSubtaskIndex(), state);
					LOG.error(message);
					vertex.getExecutionGraph().failGlobal(new Exception(message));
				}
				return;
			}
		}
	}

	private void finishCancellation(boolean releasePartitions) {
		releaseAssignedResource(new FlinkException("Execution " + this + " was cancelled."));
		vertex.getExecutionGraph().deregisterExecution(this);
		handlePartitionCleanup(releasePartitions, releasePartitions);
	}

	void cachePartitionInfo(PartitionInfo partitionInfo) {
		partitionInfos.add(partitionInfo);
	}

	private void sendPartitionInfos() {
		if (!partitionInfos.isEmpty()) {
			sendUpdatePartitionInfoRpcCall(new ArrayList<>(partitionInfos));

			partitionInfos.clear();
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Internal Actions
	// --------------------------------------------------------------------------------------------

	private boolean isLegacyScheduling() {
		return getVertex().isLegacyScheduling();
	}

	boolean switchToRunning() {

		if (transitionState(DEPLOYING, RUNNING)) {
			sendPartitionInfos();
			return true;
		}
		else {
			// something happened while the call was in progress.
			// it can mean:
			//  - canceling, while deployment was in progress. state is now canceling, or canceled, if the response overtook
			//  - finishing (execution and finished call overtook the deployment answer, which is possible and happens for fast tasks)
			//  - failed (execution, failure, and failure message overtook the deployment answer)

			ExecutionState currentState = this.state;

			if (currentState == FINISHED || currentState == CANCELED) {
				// do nothing, the task was really fast (nice)
				// or it was canceled really fast
			}
			else if (currentState == CANCELING || currentState == FAILED) {
				if (LOG.isDebugEnabled()) {
					// this log statement is guarded because the 'getVertexWithAttempt()' method
					// performs string concatenations
					LOG.debug("Concurrent canceling/failing of {} while deployment was in progress.", getVertexWithAttempt());
				}
				sendCancelRpcCall(NUM_CANCEL_CALL_TRIES);
			}
			else {
				String message = String.format("Concurrent unexpected state transition of task %s to %s while deployment was in progress.",
						getVertexWithAttempt(), currentState);

				LOG.debug(message);

				// undo the deployment
				sendCancelRpcCall(NUM_CANCEL_CALL_TRIES);

				// record the failure
				markFailed(new Exception(message));
			}

			return false;
		}
	}

	/**
	 * This method sends a CancelTask message to the instance of the assigned slot.
	 *
	 * <p>The sending is tried up to NUM_CANCEL_CALL_TRIES times.
	 */
	private void sendCancelRpcCall(int numberRetries) {
	}

	private void startTrackingPartitions(final ResourceID taskExecutorId, final Collection<ResultPartitionDeploymentDescriptor> partitions) {
		JobMasterPartitionTracker partitionTracker = vertex.getExecutionGraph().getPartitionTracker();
		for (ResultPartitionDeploymentDescriptor partition : partitions) {
			partitionTracker.startTrackingPartition(
				taskExecutorId,
				partition);
		}
	}

	void handlePartitionCleanup(boolean releasePipelinedPartitions, boolean releaseBlockingPartitions) {
		if (releasePipelinedPartitions) {
			sendReleaseIntermediateResultPartitionsRpcCall();
		}

		final Collection<ResultPartitionID> partitionIds = getPartitionIds();
		final JobMasterPartitionTracker partitionTracker = getVertex().getExecutionGraph().getPartitionTracker();

		if (!partitionIds.isEmpty()) {
			if (releaseBlockingPartitions) {
				LOG.info("Discarding the results produced by task execution {}.", attemptId);
				partitionTracker.stopTrackingAndReleasePartitions(partitionIds);
			} else {
				partitionTracker.stopTrackingPartitions(partitionIds);
			}
		}
	}

	private Collection<ResultPartitionID> getPartitionIds() {
		return producedPartitions.values().stream()
			.map(ResultPartitionDeploymentDescriptor::getShuffleDescriptor)
			.map(ShuffleDescriptor::getResultPartitionID)
			.collect(Collectors.toList());
	}

	private void sendReleaseIntermediateResultPartitionsRpcCall() {
	}

	/**
	 * Update the partition infos on the assigned resource.
	 *
	 * @param partitionInfos for the remote task
	 */
	private void sendUpdatePartitionInfoRpcCall(
			final Iterable<PartitionInfo> partitionInfos) {

		final LogicalSlot slot = assignedResource;

		if (slot != null) {
			final TaskManagerGateway taskManagerGateway = slot.getTaskManagerGateway();
			final TaskManagerLocation taskManagerLocation = slot.getTaskManagerLocation();

			CompletableFuture<Acknowledge> updatePartitionsResultFuture = taskManagerGateway.updatePartitions(attemptId, partitionInfos, rpcTimeout);

			updatePartitionsResultFuture.whenCompleteAsync(
				(ack, failure) -> {
					// fail if there was a failure
					if (failure != null) {
						fail(new IllegalStateException("Update to task [" + getVertexWithAttempt() +
							"] on TaskManager " + taskManagerLocation + " failed", failure));
					}
				}, getVertex().getExecutionGraph().getJobMasterMainThreadExecutor());
		}
	}

	/**
	 * Releases the assigned resource and completes the release future
	 * once the assigned resource has been successfully released.
	 *
	 * @param cause for the resource release, null if none
	 */
	private void releaseAssignedResource(@Nullable Throwable cause) {

		assertRunningInJobMasterMainThread();

		final LogicalSlot slot = assignedResource;

		if (slot != null) {
			ComponentMainThreadExecutor jobMasterMainThreadExecutor =
				getVertex().getExecutionGraph().getJobMasterMainThreadExecutor();

			slot.releaseSlot(cause)
				.whenComplete((Object ignored, Throwable throwable) -> {
					jobMasterMainThreadExecutor.assertRunningInMainThread();
					if (throwable != null) {
						releaseFuture.completeExceptionally(throwable);
					} else {
						releaseFuture.complete(null);
					}
				});
		} else {
			// no assigned resource --> we can directly complete the release future
			releaseFuture.complete(null);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Miscellaneous
	// --------------------------------------------------------------------------------------------

	public void transitionState(ExecutionState targetState) {
		transitionState(state, targetState);
	}

	private boolean transitionState(ExecutionState currentState, ExecutionState targetState) {
		return transitionState(currentState, targetState, null);
	}

	private boolean transitionState(ExecutionState currentState, ExecutionState targetState, Throwable error) {
		// sanity check
		if (currentState.isTerminal()) {
			throw new IllegalStateException("Cannot leave terminal state " + currentState + " to transition to " + targetState + '.');
		}

		if (state == currentState) {
			state = targetState;
			markTimestamp(targetState);

			if (error == null) {
				LOG.info("{} ({}) switched from {} to {}.", getVertex().getTaskNameWithSubtaskIndex(), getAttemptId(), currentState, targetState);
			} else {
				if (LOG.isInfoEnabled()) {
					final String locationInformation = getAssignedResource() != null ? getAssignedResource().toString() : "not deployed";

					LOG.info(
						"{} ({}) switched from {} to {} on {}.",
						getVertex().getTaskNameWithSubtaskIndex(),
						getAttemptId(),
						currentState,
						targetState,
						locationInformation,
						error);
				}
			}

			if (targetState.isTerminal()) {
				// complete the terminal state future
				terminalStateFuture.complete(targetState);
			}

			// make sure that the state transition completes normally.
			// potential errors (in listeners may not affect the main logic)
			try {
				vertex.notifyStateTransition(this, targetState, error);
			}
			catch (Throwable t) {
				LOG.error("Error while notifying execution graph of execution state transition.", t);
			}
			return true;
		} else {
			return false;
		}
	}

	private void markTimestamp(ExecutionState state) {
		markTimestamp(state, System.currentTimeMillis());
	}

	private void markTimestamp(ExecutionState state, long timestamp) {
		this.stateTimestamps[state.ordinal()] = timestamp;
	}

	public String getVertexWithAttempt() {
		return vertex.getTaskNameWithSubtaskIndex() + " - execution #" + attemptNumber;
	}

	// ------------------------------------------------------------------------
	//  Accumulators
	// ------------------------------------------------------------------------

	/**
	 * Update accumulators (discarded when the Execution has already been terminated).
	 * @param userAccumulators the user accumulators
	 */
	public void setAccumulators(Map<String, Accumulator<?, ?>> userAccumulators) {
		synchronized (accumulatorLock) {
			if (!state.isTerminal()) {
				this.userAccumulators = userAccumulators;
			}
		}
	}

	public Map<String, Accumulator<?, ?>> getUserAccumulators() {
		return userAccumulators;
	}

	@Override
	public StringifiedAccumulatorResult[] getUserAccumulatorsStringified() {
		Map<String, OptionalFailure<Accumulator<?, ?>>> accumulators =
			userAccumulators == null ?
				null :
				userAccumulators.entrySet()
					.stream()
					.collect(Collectors.toMap(Map.Entry::getKey, entry -> OptionalFailure.of(entry.getValue())));
		return StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulators);
	}

	@Override
	public int getParallelSubtaskIndex() {
		return getVertex().getParallelSubtaskIndex();
	}

	private void updateAccumulatorsAndMetrics(Map<String, Accumulator<?, ?>> userAccumulators) {
		if (userAccumulators != null) {
			synchronized (accumulatorLock) {
				this.userAccumulators = userAccumulators;
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Standard utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		final LogicalSlot slot = assignedResource;

		return String.format("Attempt #%d (%s) @ %s - [%s]", attemptNumber, vertex.getTaskNameWithSubtaskIndex(),
				(slot == null ? "(unassigned)" : slot), state);
	}

	@Override
	public ArchivedExecution archive() {
		return new ArchivedExecution(this);
	}

	private void assertRunningInJobMasterMainThread() {
		vertex.getExecutionGraph().assertRunningInJobMasterMainThread();
	}
}
