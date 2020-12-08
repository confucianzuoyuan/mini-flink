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

package org.apache.flink.core.execution;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * A client that is scoped to a specific job.
 */
@PublicEvolving
public interface JobClient {

	/**
	 * Returns the {@link JobID} that uniquely identifies the job this client is scoped to.
	 */
	JobID getJobID();

	/**
	 * Requests the accumulators of the associated job. Accumulators can be requested while it is running
	 * or after it has finished. The class loader is used to deserialize the incoming accumulator results.
	 */
	CompletableFuture<Map<String, Object>> getAccumulators(ClassLoader classLoader);

	/**
	 * Returns the {@link JobExecutionResult result of the job execution} of the submitted job.
	 *
	 * @param userClassloader the classloader used to de-serialize the accumulators of the job.
	 */
	CompletableFuture<JobExecutionResult> getJobExecutionResult(final ClassLoader userClassloader);
}
