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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Objects;

public class CheckpointCoordinatorConfiguration implements Serializable {

	public static final long MINIMAL_CHECKPOINT_TIME = 10;

	private static final long serialVersionUID = 2L;

	private final long checkpointInterval;

	private final long checkpointTimeout;

	private final long minPauseBetweenCheckpoints;

	private final int maxConcurrentCheckpoints;

	private final int tolerableCheckpointFailureNumber;

	/** Settings for what to do with checkpoints when a job finishes. */
	private final CheckpointRetentionPolicy checkpointRetentionPolicy;

	/**
	 * Flag indicating whether exactly once checkpoint mode has been configured.
	 * If <code>false</code>, at least once mode has been configured. This is
	 * not a necessary attribute, because the checkpointing mode is only relevant
	 * for the stream tasks, but we expose it here to forward it to the web runtime
	 * UI.
	 */
	private final boolean isExactlyOnce;

	private final boolean isPreferCheckpointForRecovery;

	private final boolean isUnalignedCheckpointsEnabled;

	@Deprecated
	@VisibleForTesting
	public CheckpointCoordinatorConfiguration(
			long checkpointInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			CheckpointRetentionPolicy checkpointRetentionPolicy,
			boolean isExactlyOnce,
			boolean isUnalignedCheckpoint,
			boolean isPreferCheckpointForRecovery,
			int tolerableCpFailureNumber) {
		this(
			checkpointInterval,
			checkpointTimeout,
			minPauseBetweenCheckpoints,
			maxConcurrentCheckpoints,
			checkpointRetentionPolicy,
			isExactlyOnce,
			isPreferCheckpointForRecovery,
			tolerableCpFailureNumber,
			isUnalignedCheckpoint);
	}

	private CheckpointCoordinatorConfiguration(
			long checkpointInterval,
			long checkpointTimeout,
			long minPauseBetweenCheckpoints,
			int maxConcurrentCheckpoints,
			CheckpointRetentionPolicy checkpointRetentionPolicy,
			boolean isExactlyOnce,
			boolean isPreferCheckpointForRecovery,
			int tolerableCpFailureNumber,
			boolean isUnalignedCheckpointsEnabled) {

		// sanity checks
		if (checkpointInterval < MINIMAL_CHECKPOINT_TIME || checkpointTimeout < MINIMAL_CHECKPOINT_TIME ||
			minPauseBetweenCheckpoints < 0 || maxConcurrentCheckpoints < 1 ||
			tolerableCpFailureNumber < 0) {
			throw new IllegalArgumentException();
		}
		Preconditions.checkArgument(!isUnalignedCheckpointsEnabled || maxConcurrentCheckpoints <= 1,
				"maxConcurrentCheckpoints can't be > 1 if UnalignedCheckpoints enabled");

		this.checkpointInterval = checkpointInterval;
		this.checkpointTimeout = checkpointTimeout;
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
		this.maxConcurrentCheckpoints = maxConcurrentCheckpoints;
		this.checkpointRetentionPolicy = Preconditions.checkNotNull(checkpointRetentionPolicy);
		this.isExactlyOnce = isExactlyOnce;
		this.isPreferCheckpointForRecovery = isPreferCheckpointForRecovery;
		this.tolerableCheckpointFailureNumber = tolerableCpFailureNumber;
		this.isUnalignedCheckpointsEnabled = isUnalignedCheckpointsEnabled;
	}

	public long getCheckpointInterval() {
		return checkpointInterval;
	}

	public long getCheckpointTimeout() {
		return checkpointTimeout;
	}

	public long getMinPauseBetweenCheckpoints() {
		return minPauseBetweenCheckpoints;
	}

	public int getMaxConcurrentCheckpoints() {
		return maxConcurrentCheckpoints;
	}

	public CheckpointRetentionPolicy getCheckpointRetentionPolicy() {
		return checkpointRetentionPolicy;
	}

	public boolean isExactlyOnce() {
		return isExactlyOnce;
	}

	public boolean isPreferCheckpointForRecovery() {
		return isPreferCheckpointForRecovery;
	}

	public int getTolerableCheckpointFailureNumber() {
		return tolerableCheckpointFailureNumber;
	}

	public boolean isUnalignedCheckpointsEnabled() {
		return isUnalignedCheckpointsEnabled;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		CheckpointCoordinatorConfiguration that = (CheckpointCoordinatorConfiguration) o;
		return checkpointInterval == that.checkpointInterval &&
			checkpointTimeout == that.checkpointTimeout &&
			minPauseBetweenCheckpoints == that.minPauseBetweenCheckpoints &&
			maxConcurrentCheckpoints == that.maxConcurrentCheckpoints &&
			isExactlyOnce == that.isExactlyOnce &&
			isUnalignedCheckpointsEnabled == that.isUnalignedCheckpointsEnabled &&
			checkpointRetentionPolicy == that.checkpointRetentionPolicy &&
			isPreferCheckpointForRecovery == that.isPreferCheckpointForRecovery &&
			tolerableCheckpointFailureNumber == that.tolerableCheckpointFailureNumber;
	}

	@Override
	public int hashCode() {
		return Objects.hash(
				checkpointInterval,
				checkpointTimeout,
				minPauseBetweenCheckpoints,
				maxConcurrentCheckpoints,
				checkpointRetentionPolicy,
				isExactlyOnce,
				isUnalignedCheckpointsEnabled,
				isPreferCheckpointForRecovery,
				tolerableCheckpointFailureNumber);
	}

	@Override
	public String toString() {
		return "JobCheckpointingConfiguration{" +
			"checkpointInterval=" + checkpointInterval +
			", checkpointTimeout=" + checkpointTimeout +
			", minPauseBetweenCheckpoints=" + minPauseBetweenCheckpoints +
			", maxConcurrentCheckpoints=" + maxConcurrentCheckpoints +
			", checkpointRetentionPolicy=" + checkpointRetentionPolicy +
			", isExactlyOnce=" + isExactlyOnce +
			", isUnalignedCheckpoint=" + isUnalignedCheckpointsEnabled +
			", isPreferCheckpointForRecovery=" + isPreferCheckpointForRecovery +
			", tolerableCheckpointFailureNumber=" + tolerableCheckpointFailureNumber +
			'}';
	}

}
