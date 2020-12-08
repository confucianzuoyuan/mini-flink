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

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.OptionalFailure;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;
import java.util.Map;

/**
 * Common interface for the runtime {@link ExecutionGraph} and {@link ArchivedExecutionGraph}.
 */
public interface AccessExecutionGraph {

	String getJsonPlan();

	JobID getJobID();

	String getJobName();

	JobStatus getState();

	@Nullable
	ErrorInfo getFailureInfo();

	@Nullable
	AccessExecutionJobVertex getJobVertex(JobVertexID id);

	Map<JobVertexID, ? extends AccessExecutionJobVertex> getAllVertices();

	Iterable<? extends AccessExecutionJobVertex> getVerticesTopologically();

	Iterable<? extends AccessExecutionVertex> getAllExecutionVertices();

	long getStatusTimestamp(JobStatus status);

	@Nullable
	ArchivedExecutionConfig getArchivedExecutionConfig();

	boolean isStoppable();

	StringifiedAccumulatorResult[] getAccumulatorResultsStringified();

	Map<String, SerializedValue<OptionalFailure<Object>>> getAccumulatorsSerialized();

	boolean isArchived();

}
