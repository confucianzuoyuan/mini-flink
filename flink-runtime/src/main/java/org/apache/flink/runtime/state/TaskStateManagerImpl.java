/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.JobManagerTaskRestore;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReaderImpl;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;

public class TaskStateManagerImpl implements TaskStateManager {

	/** The logger for this class. */
	private static final Logger LOG = LoggerFactory.getLogger(TaskStateManagerImpl.class);

	/** The id of the job for which this manager was created, can report, and recover. */
	private final JobID jobId;

	/** The execution attempt id that this manager reports for. */
	private final ExecutionAttemptID executionAttemptID;

	/** The data given by the job manager to restore the job. This is null for a new job without previous state. */
	@Nullable
	private final JobManagerTaskRestore jobManagerTaskRestore;

	/** The local state store to which this manager reports local state snapshots. */
	private final TaskLocalStateStore localStateStore;

	private final ChannelStateReader channelStateReader;

	public TaskStateManagerImpl(
			@Nonnull JobID jobId,
			@Nonnull ExecutionAttemptID executionAttemptID,
			@Nonnull TaskLocalStateStore localStateStore,
			@Nullable JobManagerTaskRestore jobManagerTaskRestore) {
		this(
			jobId,
			executionAttemptID,
			localStateStore,
			jobManagerTaskRestore,
			new ChannelStateReaderImpl(jobManagerTaskRestore == null ? new TaskStateSnapshot() : jobManagerTaskRestore.getTaskStateSnapshot())
		);
	}

	public TaskStateManagerImpl(
			@Nonnull JobID jobId,
			@Nonnull ExecutionAttemptID executionAttemptID,
			@Nonnull TaskLocalStateStore localStateStore,
			@Nullable JobManagerTaskRestore jobManagerTaskRestore,
			@Nonnull ChannelStateReader channelStateReader) {
		this.jobId = jobId;
		this.localStateStore = localStateStore;
		this.jobManagerTaskRestore = jobManagerTaskRestore;
		this.executionAttemptID = executionAttemptID;
		this.channelStateReader = channelStateReader;
	}

	@Nonnull
	@Override
	public PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID) {

		if (jobManagerTaskRestore == null) {
			return PrioritizedOperatorSubtaskState.emptyNotRestored();
		}

		TaskStateSnapshot jobManagerStateSnapshot =
			jobManagerTaskRestore.getTaskStateSnapshot();

		OperatorSubtaskState jobManagerSubtaskState =
			jobManagerStateSnapshot.getSubtaskStateByOperatorID(operatorID);

		if (jobManagerSubtaskState == null) {
			return PrioritizedOperatorSubtaskState.emptyNotRestored();
		}

		long restoreCheckpointId = jobManagerTaskRestore.getRestoreCheckpointId();

		TaskStateSnapshot localStateSnapshot =
			localStateStore.retrieveLocalState(restoreCheckpointId);

		List<OperatorSubtaskState> alternativesByPriority = Collections.emptyList();

		if (localStateSnapshot != null) {
			OperatorSubtaskState localSubtaskState = localStateSnapshot.getSubtaskStateByOperatorID(operatorID);

			if (localSubtaskState != null) {
				alternativesByPriority = Collections.singletonList(localSubtaskState);
			}
		}

		PrioritizedOperatorSubtaskState.Builder builder = new PrioritizedOperatorSubtaskState.Builder(
			jobManagerSubtaskState,
			alternativesByPriority,
			true);

		return builder.build();
	}

	@Nonnull
	@Override
	public LocalRecoveryConfig createLocalRecoveryConfig() {
		return localStateStore.getLocalRecoveryConfig();
	}

	@Override
	public ChannelStateReader getChannelStateReader() {
		return channelStateReader;
	}

	@Override
	public void close() throws Exception {
		channelStateReader.close();
	}
}
