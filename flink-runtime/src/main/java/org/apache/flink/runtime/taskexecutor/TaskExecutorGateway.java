package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobmaster.AllocatedSlotReport;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.resourcemanager.ResourceManagerId;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.util.SerializedValue;

import java.util.Collection;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

public interface TaskExecutorGateway extends RpcGateway, TaskExecutorOperatorEventGateway {

	CompletableFuture<Acknowledge> requestSlot(
		SlotID slotId,
		JobID jobId,
		AllocationID allocationId,
		ResourceProfile resourceProfile,
		String targetAddress,
		ResourceManagerId resourceManagerId,
		@RpcTimeout Time timeout);

	CompletableFuture<TaskBackPressureResponse> requestTaskBackPressure(
		ExecutionAttemptID executionAttemptId,
		int requestId,
		@RpcTimeout Time timeout);

	CompletableFuture<Acknowledge> submitTask(
		TaskDeploymentDescriptor tdd,
		JobMasterId jobMasterId,
		@RpcTimeout Time timeout);

	CompletableFuture<Acknowledge> updatePartitions(
		ExecutionAttemptID executionAttemptID,
		Iterable<PartitionInfo> partitionInfos,
		@RpcTimeout Time timeout);

	void releaseOrPromotePartitions(JobID jobId, Set<ResultPartitionID> partitionToRelease, Set<ResultPartitionID> partitionsToPromote);

	CompletableFuture<Acknowledge> releaseClusterPartitions(Collection<IntermediateDataSetID> dataSetsToRelease, @RpcTimeout Time timeout);

	void heartbeatFromJobManager(ResourceID heartbeatOrigin, AllocatedSlotReport allocatedSlotReport);

	/**
	 * Heartbeat request from the resource manager.
	 *
	 * @param heartbeatOrigin unique id of the resource manager
	 */
	void heartbeatFromResourceManager(ResourceID heartbeatOrigin);

	/**
	 * Disconnects the given JobManager from the TaskManager.
	 *
	 * @param jobId JobID for which the JobManager was the leader
	 * @param cause for the disconnection from the JobManager
	 */
	void disconnectJobManager(JobID jobId, Exception cause);

	/**
	 * Disconnects the ResourceManager from the TaskManager.
	 *
	 * @param cause for the disconnection from the ResourceManager
	 */
	void disconnectResourceManager(Exception cause);

	/**
	 * Frees the slot with the given allocation ID.
	 *
	 * @param allocationId identifying the slot to free
	 * @param cause of the freeing operation
	 * @param timeout for the operation
	 * @return Future acknowledge which is returned once the slot has been freed
	 */
	CompletableFuture<Acknowledge> freeSlot(
		final AllocationID allocationId,
		final Throwable cause,
		@RpcTimeout final Time timeout);

	/**
	 * Checks whether the task executor can be released. It cannot be released if there're unconsumed result partitions.
	 *
	 * @return Future flag indicating whether the task executor can be released.
	 */
	CompletableFuture<Boolean> canBeReleased();

	@Override
	CompletableFuture<Acknowledge> sendOperatorEventToTask(
			ExecutionAttemptID task,
			OperatorID operator,
			SerializedValue<OperatorEvent> evt);

}
