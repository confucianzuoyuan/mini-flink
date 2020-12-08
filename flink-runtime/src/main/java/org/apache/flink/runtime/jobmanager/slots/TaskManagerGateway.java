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

package org.apache.flink.runtime.jobmanager.slots;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.PartitionInfo;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.TaskBackPressureResponse;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.rpc.RpcTimeout;
import org.apache.flink.runtime.taskexecutor.TaskExecutorOperatorEventGateway;
import org.apache.flink.util.SerializedValue;

import java.util.concurrent.CompletableFuture;

/**
 * Task manager gateway interface to communicate with the task manager.
 */
public interface TaskManagerGateway extends TaskExecutorOperatorEventGateway {

	String getAddress();

	CompletableFuture<TaskBackPressureResponse> requestTaskBackPressure(
		ExecutionAttemptID executionAttemptID,
		int requestId,
		Time timeout);

	CompletableFuture<Acknowledge> submitTask(
		TaskDeploymentDescriptor tdd,
		Time timeout);

	CompletableFuture<Acknowledge> updatePartitions(
		ExecutionAttemptID executionAttemptID,
		Iterable<PartitionInfo> partitionInfos,
		Time timeout);

	CompletableFuture<Acknowledge> freeSlot(
		final AllocationID allocationId,
		final Throwable cause,
		@RpcTimeout final Time timeout);

	@Override
	CompletableFuture<Acknowledge> sendOperatorEventToTask(
		ExecutionAttemptID task,
		OperatorID operator,
		SerializedValue<OperatorEvent> evt);
}
