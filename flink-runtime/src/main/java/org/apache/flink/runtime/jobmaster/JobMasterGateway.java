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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.FencedRpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.AccumulatorReport;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;

/**
 * {@link JobMaster} rpc gateway interface.
 */
public interface JobMasterGateway extends
	FencedRpcGateway<JobMasterId>,
	KvStateLocationOracle,
	KvStateRegistryGateway,
	JobMasterOperatorEventGateway {

	/**
	 * Updates the task execution state for a given task.
	 *
	 * @param taskExecutionState New task execution state for a given task
	 * @return Future flag of the task execution state update result
	 */
	CompletableFuture<Acknowledge> updateTaskExecutionState(
			final TaskExecutionState taskExecutionState);

	/**
	 * Requests the next input split for the {@link ExecutionJobVertex}.
	 * The next input split is sent back to the sender as a
	 * {@link SerializedInputSplit} message.
	 *
	 * @param vertexID         The job vertex id
	 * @param executionAttempt The execution attempt id
	 * @return The future of the input split. If there is no further input split, will return an empty object.
	 */
	CompletableFuture<SerializedInputSplit> requestNextInputSplit(
			final JobVertexID vertexID,
			final ExecutionAttemptID executionAttempt);

	/**
	 * Requests the current state of the partition. The state of a
	 * partition is currently bound to the state of the producing execution.
	 *
	 * @param intermediateResultId The execution attempt ID of the task requesting the partition state.
	 * @param partitionId          The partition ID of the partition to request the state of.
	 * @return The future of the partition state
	 */
	CompletableFuture<ExecutionState> requestPartitionState(
			final IntermediateDataSetID intermediateResultId,
			final ResultPartitionID partitionId);

	/**
	 * Notifies the JobManager about available data for a produced partition.
	 *
	 * <p>There is a call to this method for each {@link ExecutionVertex} instance once per produced
	 * {@link ResultPartition} instance, either when first producing data (for pipelined executions)
	 * or when all data has been produced (for staged executions).
	 *
	 * <p>The JobManager then can decide when to schedule the partition consumers of the given session.
	 *
	 * @param partitionID     The partition which has already produced data
	 * @param timeout         before the rpc call fails
	 * @return Future acknowledge of the schedule or update operation
	 */
	CompletableFuture<Acknowledge> scheduleOrUpdateConsumers(
			final ResultPartitionID partitionID,
			@RpcTimeout final Time timeout);

	/**
	 * Disconnects the given {@link org.apache.flink.runtime.taskexecutor.TaskExecutor} from the
	 * {@link JobMaster}.
	 *
	 * @param resourceID identifying the TaskManager to disconnect
	 * @param cause for the disconnection of the TaskManager
	 * @return Future acknowledge once the JobMaster has been disconnected from the TaskManager
	 */
	CompletableFuture<Acknowledge> disconnectTaskManager(ResourceID resourceID, Exception cause);

	/**
	 * Disconnects the resource manager from the job manager because of the given cause.
	 *
	 * @param resourceManagerId identifying the resource manager leader id
	 * @param cause of the disconnect
	 */
	void disconnectResourceManager(
		final ResourceManagerId resourceManagerId,
		final Exception cause);

	/**
	 * Offers the given slots to the job manager. The response contains the set of accepted slots.
	 *
	 * @param taskManagerId identifying the task manager
	 * @param slots         to offer to the job manager
	 * @param timeout       for the rpc call
	 * @return Future set of accepted slots.
	 */
	CompletableFuture<Collection<SlotOffer>> offerSlots(
			final ResourceID taskManagerId,
			final Collection<SlotOffer> slots,
			@RpcTimeout final Time timeout);

	/**
	 * Fails the slot with the given allocation id and cause.
	 *
	 * @param taskManagerId identifying the task manager
	 * @param allocationId  identifying the slot to fail
	 * @param cause         of the failing
	 */
	void failSlot(final ResourceID taskManagerId,
			final AllocationID allocationId,
			final Exception cause);

	/**
	 * Registers the task manager at the job manager.
	 *
	 * @param taskManagerRpcAddress the rpc address of the task manager
	 * @param unresolvedTaskManagerLocation   unresolved location of the task manager
	 * @param timeout               for the rpc call
	 * @return Future registration response indicating whether the registration was successful or not
	 */
	CompletableFuture<RegistrationResponse> registerTaskManager(
			final String taskManagerRpcAddress,
			final UnresolvedTaskManagerLocation unresolvedTaskManagerLocation,
			@RpcTimeout final Time timeout);

	/**
	 * Sends the heartbeat to job manager from task manager.
	 *
	 * @param resourceID unique id of the task manager
	 * @param accumulatorReport report containing accumulator updates
	 */
	void heartbeatFromTaskManager(
		final ResourceID resourceID,
		final AccumulatorReport accumulatorReport);

	/**
	 * Sends heartbeat request from the resource manager.
	 *
	 * @param resourceID unique id of the resource manager
	 */
	void heartbeatFromResourceManager(final ResourceID resourceID);

	CompletableFuture<JobStatus> requestJobStatus(@RpcTimeout Time timeout);

	void notifyAllocationFailure(AllocationID allocationID, Exception cause);

}
