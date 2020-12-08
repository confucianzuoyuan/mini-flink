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

import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.jobgraph.OperatorID;

import javax.annotation.Nonnull;

/**
 * This interface provides methods to report and retrieve state for a task.
 *
 * <p>When a checkpoint or savepoint is triggered on a task, it will create snapshots for all stream operator instances
 * it owns. All operator snapshots from the task are then reported via this interface. A typical implementation will
 * dispatch and forward the reported state information to interested parties such as the checkpoint coordinator or a
 * local state store.
 *
 * <p>This interface also offers the complementary method that provides access to previously saved state of operator
 * instances in the task for restore purposes.
 */
public interface TaskStateManager extends AutoCloseable {

	/**
	 * Returns means to restore previously reported state of an operator running in the owning task.
	 *
	 * @param operatorID the id of the operator for which we request state.
	 * @return Previous state for the operator. The previous state can be empty if the operator had no previous state.
	 */
	@Nonnull
	PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID);

	/**
	 * Returns the configuration for local recovery, i.e. the base directories for all file-based local state of the
	 * owning subtask and the general mode for local recovery.
	 */
	@Nonnull
	LocalRecoveryConfig createLocalRecoveryConfig();

	ChannelStateReader getChannelStateReader();
}
