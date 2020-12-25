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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.ExecutionFailureHandler;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.scheduler.NoResourceAvailableException;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.slotpool.ThrowingSlotProvider;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.util.ExceptionUtils;
import org.slf4j.Logger;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The future default scheduler.
 */
public class DefaultScheduler extends SchedulerBase implements SchedulerOperations {

	private final Logger log;

	private final ClassLoader userCodeLoader;

	private final ExecutionSlotAllocator executionSlotAllocator;

	private final ExecutionFailureHandler executionFailureHandler;

	private final ScheduledExecutor delayExecutor;

	private final SchedulingStrategy schedulingStrategy;

	private final ExecutionVertexOperations executionVertexOperations;

	private final Set<ExecutionVertexID> verticesWaitingForRestart;

	DefaultScheduler(
		final Logger log,
		final JobGraph jobGraph,
		final Executor ioExecutor,
		final Configuration jobMasterConfiguration,
		final ScheduledExecutorService futureExecutor,
		final ScheduledExecutor delayExecutor,
		final ClassLoader userCodeLoader,
		final Time rpcTimeout,
		final BlobWriter blobWriter,
		final ShuffleMaster<?> shuffleMaster,
		final JobMasterPartitionTracker partitionTracker,
		final SchedulingStrategyFactory schedulingStrategyFactory,
		final FailoverStrategy.Factory failoverStrategyFactory,
		final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
		final ExecutionVertexOperations executionVertexOperations,
		final ExecutionVertexVersioner executionVertexVersioner,
		final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory) throws Exception {

		super(
			log,
			jobGraph,
			ioExecutor,
			jobMasterConfiguration,
			new ThrowingSlotProvider(), // this is not used any more in the new scheduler
			futureExecutor,
			userCodeLoader,
			rpcTimeout,
			blobWriter,
			Time.seconds(0), // this is not used any more in the new scheduler
			shuffleMaster,
			partitionTracker,
			executionVertexVersioner,
			false);

		this.log = log;

		this.delayExecutor = checkNotNull(delayExecutor);
		this.userCodeLoader = checkNotNull(userCodeLoader);
		this.executionVertexOperations = checkNotNull(executionVertexOperations);

		final FailoverStrategy failoverStrategy = failoverStrategyFactory.create(
			getSchedulingTopology(),
			getResultPartitionAvailabilityChecker());
		log.info("Using failover strategy {} for {} ({}).", failoverStrategy, jobGraph.getName(), jobGraph.getJobID());

		this.executionFailureHandler = new ExecutionFailureHandler(
			getSchedulingTopology(),
			failoverStrategy,
			restartBackoffTimeStrategy);
		this.schedulingStrategy = schedulingStrategyFactory.createInstance(this, getSchedulingTopology());
		this.executionSlotAllocator = checkNotNull(executionSlotAllocatorFactory).createInstance(getInputsLocationsRetriever());

		this.verticesWaitingForRestart = new HashSet<>();
	}

	// ------------------------------------------------------------------------
	// SchedulerNG
	// ------------------------------------------------------------------------

	@Override
	protected long getNumberOfRestarts() {
		return executionFailureHandler.getNumberOfRestarts();
	}

	@Override
	protected void startSchedulingInternal() {
		// 准备ExecutionGraph
		prepareExecutionGraphForNgScheduling();
		// 开始调度
		schedulingStrategy.startScheduling();
	}

	@Override
	protected void scheduleOrUpdateConsumersInternal(final IntermediateResultPartitionID partitionId) {
		schedulingStrategy.onPartitionConsumable(partitionId);
	}

	// ------------------------------------------------------------------------
	// SchedulerOperations
	// ------------------------------------------------------------------------

	@Override
	public void allocateSlotsAndDeploy(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		validateDeploymentOptions(executionVertexDeploymentOptions);

		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex =
			groupDeploymentOptionsByVertexId(executionVertexDeploymentOptions);

		final List<ExecutionVertexID> verticesToDeploy = executionVertexDeploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.collect(Collectors.toList());

		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex =
			executionVertexVersioner.recordVertexModifications(verticesToDeploy);

		transitionToScheduled(verticesToDeploy);

		// 分配任务槽
		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments =
			allocateSlots(executionVertexDeploymentOptions);

		final List<DeploymentHandle> deploymentHandles = createDeploymentHandles(
			requiredVersionByVertex,
			deploymentOptionsByVertex,
			slotExecutionVertexAssignments);

		// 执行任务
		waitForAllSlotsAndDeploy(deploymentHandles);
	}

	private void validateDeploymentOptions(final Collection<ExecutionVertexDeploymentOption> deploymentOptions) {
		deploymentOptions.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.map(this::getExecutionVertex)
			.forEach(v -> checkState(
				v.getExecutionState() == ExecutionState.CREATED,
				"expected vertex %s to be in CREATED state, was: %s", v.getID(), v.getExecutionState()));
	}

	private static Map<ExecutionVertexID, ExecutionVertexDeploymentOption> groupDeploymentOptionsByVertexId(
			final Collection<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionVertexDeploymentOptions.stream().collect(Collectors.toMap(
				ExecutionVertexDeploymentOption::getExecutionVertexId,
				Function.identity()));
	}

	private List<SlotExecutionVertexAssignment> allocateSlots(final List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions) {
		return executionSlotAllocator.allocateSlotsFor(executionVertexDeploymentOptions
			.stream()
			.map(ExecutionVertexDeploymentOption::getExecutionVertexId)
			.map(this::getExecutionVertex)
			.map(ExecutionVertexSchedulingRequirementsMapper::from)
			.collect(Collectors.toList()));
	}

	private static List<DeploymentHandle> createDeploymentHandles(
		final Map<ExecutionVertexID, ExecutionVertexVersion> requiredVersionByVertex,
		final Map<ExecutionVertexID, ExecutionVertexDeploymentOption> deploymentOptionsByVertex,
		final List<SlotExecutionVertexAssignment> slotExecutionVertexAssignments) {

		return slotExecutionVertexAssignments
			.stream()
			.map(slotExecutionVertexAssignment -> {
				final ExecutionVertexID executionVertexId = slotExecutionVertexAssignment.getExecutionVertexId();
				return new DeploymentHandle(
					requiredVersionByVertex.get(executionVertexId),
					deploymentOptionsByVertex.get(executionVertexId),
					slotExecutionVertexAssignment);
			})
			.collect(Collectors.toList());
	}

	private void waitForAllSlotsAndDeploy(final List<DeploymentHandle> deploymentHandles) {
		// deployAll执行程序
		assignAllResources(deploymentHandles).handle(deployAll(deploymentHandles));
	}

	private CompletableFuture<Void> assignAllResources(final List<DeploymentHandle> deploymentHandles) {
		final List<CompletableFuture<Void>> slotAssignedFutures = new ArrayList<>();
		for (DeploymentHandle deploymentHandle : deploymentHandles) {
			final CompletableFuture<Void> slotAssigned = deploymentHandle
				.getSlotExecutionVertexAssignment()
				.getLogicalSlotFuture()
				.handle(assignResourceOrHandleError(deploymentHandle));
			slotAssignedFutures.add(slotAssigned);
		}
		return FutureUtils.waitForAll(slotAssignedFutures);
	}

	private BiFunction<Void, Throwable, Void> deployAll(final List<DeploymentHandle> deploymentHandles) {
		return (ignored, throwable) -> {
			propagateIfNonNull(throwable);
			for (final DeploymentHandle deploymentHandle : deploymentHandles) {
				final SlotExecutionVertexAssignment slotExecutionVertexAssignment = deploymentHandle.getSlotExecutionVertexAssignment();
				final CompletableFuture<LogicalSlot> slotAssigned = slotExecutionVertexAssignment.getLogicalSlotFuture();
				// 调用deployOrHandleError方法执行作业
				slotAssigned.handle(deployOrHandleError(deploymentHandle));
			}
			return null;
		};
	}

	private static void propagateIfNonNull(final Throwable throwable) {
		if (throwable != null) {
			throw new CompletionException(throwable);
		}
	}

	private BiFunction<LogicalSlot, Throwable, Void> assignResourceOrHandleError(final DeploymentHandle deploymentHandle) {
		final ExecutionVertexID executionVertexId = deploymentHandle.getExecutionVertexId();

		return (logicalSlot, throwable) -> {
			if (throwable == null) {
				final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
				final boolean sendScheduleOrUpdateConsumerMessage = deploymentHandle.getDeploymentOption().sendScheduleOrUpdateConsumerMessage();
				executionVertex
					.getCurrentExecutionAttempt()
					.registerProducedPartitions(logicalSlot.getTaskManagerLocation(), sendScheduleOrUpdateConsumerMessage);
				executionVertex.tryAssignResource(logicalSlot);
			} else {
				handleTaskDeploymentFailure(executionVertexId, maybeWrapWithNoResourceAvailableException(throwable));
			}
			return null;
		};
	}

	private void handleTaskDeploymentFailure(final ExecutionVertexID executionVertexId, final Throwable error) {
		executionVertexOperations.markFailed(getExecutionVertex(executionVertexId), error);
	}

	private static Throwable maybeWrapWithNoResourceAvailableException(final Throwable failure) {
		final Throwable strippedThrowable = ExceptionUtils.stripCompletionException(failure);
		if (strippedThrowable instanceof TimeoutException) {
			return new NoResourceAvailableException("Could not allocate the required slot within slot request timeout. " +
				"Please make sure that the cluster has enough resources.", failure);
		} else {
			return failure;
		}
	}

	private BiFunction<Object, Throwable, Void> deployOrHandleError(final DeploymentHandle deploymentHandle) {
		final ExecutionVertexVersion requiredVertexVersion = deploymentHandle.getRequiredVertexVersion();
		final ExecutionVertexID executionVertexId = requiredVertexVersion.getExecutionVertexId();

		return (ignored, throwable) -> {
			// 执行作业
			deployTaskSafe(executionVertexId);
			return null;
		};
	}

	private void deployTaskSafe(final ExecutionVertexID executionVertexId) {
		try {
			final ExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
			// 调用deploy
			executionVertexOperations.deploy(executionVertex);
		} catch (Throwable e) {
			handleTaskDeploymentFailure(executionVertexId, e);
		}
	}

}
