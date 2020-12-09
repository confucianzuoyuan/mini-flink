/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

/**
 * Default {@link ResourceManagerPartitionTracker} implementation.
 *
 * <p>Internal tracking info must only be updated upon reception of a {@link ClusterPartitionReport}, as the task
 * executor state is the source of truth.
 */
public class ResourceManagerPartitionTrackerImpl implements ResourceManagerPartitionTracker {

	private static final Logger LOG = LoggerFactory.getLogger(ResourceManagerPartitionTrackerImpl.class);

	private final TaskExecutorClusterPartitionReleaser taskExecutorClusterPartitionReleaser;

	public ResourceManagerPartitionTrackerImpl(TaskExecutorClusterPartitionReleaser taskExecutorClusterPartitionReleaser) {
		this.taskExecutorClusterPartitionReleaser = taskExecutorClusterPartitionReleaser;
	}

	@Override
	public void processTaskExecutorClusterPartitionReport(ResourceID taskExecutorId, ClusterPartitionReport clusterPartitionReport) {
		Preconditions.checkNotNull(taskExecutorId);
		Preconditions.checkNotNull(clusterPartitionReport);
		LOG.debug("Processing cluster partition report from task executor {}: {}.", taskExecutorId, clusterPartitionReport);

		internalProcessClusterPartitionReport(taskExecutorId, clusterPartitionReport);
	}

	@Override
	public void processTaskExecutorShutdown(ResourceID taskExecutorId) {
		Preconditions.checkNotNull(taskExecutorId);
		LOG.debug("Processing shutdown of task executor {}.", taskExecutorId);

		internalProcessClusterPartitionReport(taskExecutorId, new ClusterPartitionReport(Collections.emptyList()));
	}

	private void internalProcessClusterPartitionReport(ResourceID taskExecutorId, ClusterPartitionReport clusterPartitionReport) {
	}

}
