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

package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.requireNonNull;
import static org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration.MINIMAL_CHECKPOINT_TIME;

/**
 * Configuration that captures all checkpointing related settings.
 */
@Public
public class CheckpointConfig implements java.io.Serializable {

	private static final long serialVersionUID = -750378776078908147L;

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointConfig.class);

	/** The default checkpoint mode: exactly once. */
	public static final CheckpointingMode DEFAULT_MODE = CheckpointingMode.EXACTLY_ONCE;

	/** The default timeout of a checkpoint attempt: 10 minutes. */
	public static final long DEFAULT_TIMEOUT = 10 * 60 * 1000;

	/** The default minimum pause to be made between checkpoints: none. */
	public static final long DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS = 0;

	/** The default limit of concurrently happening checkpoints: one. */
	public static final int DEFAULT_MAX_CONCURRENT_CHECKPOINTS = 1;

	public static final int UNDEFINED_TOLERABLE_CHECKPOINT_NUMBER = -1;

	// ------------------------------------------------------------------------

	/** Checkpointing mode (exactly-once vs. at-least-once). */
	private CheckpointingMode checkpointingMode = DEFAULT_MODE;

	/** Periodic checkpoint triggering interval. */
	private long checkpointInterval = -1; // disabled

	/** Maximum time checkpoint may take before being discarded. */
	private long checkpointTimeout = DEFAULT_TIMEOUT;

	/** Minimal pause between checkpointing attempts. */
	private long minPauseBetweenCheckpoints = DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS;

	/** Maximum number of checkpoint attempts in progress at the same time. */
	private int maxConcurrentCheckpoints = DEFAULT_MAX_CONCURRENT_CHECKPOINTS;

	/** Flag to force checkpointing in iterative jobs. */
	private boolean forceCheckpointing;

	/** Flag to enable unaligned checkpoints. */
	private boolean unalignedCheckpointsEnabled;

	/** Cleanup behaviour for persistent checkpoints. */
	private ExternalizedCheckpointCleanup externalizedCheckpointCleanup;

	/**
	 * Task would not fail if there is an error in their checkpointing.
	 *
	 * <p>{@link #tolerableCheckpointFailureNumber} would always overrule this deprecated field if they have conflicts.
	 *
	 * @deprecated Use {@link #tolerableCheckpointFailureNumber}.
	 */
	@Deprecated
	private boolean failOnCheckpointingErrors = true;

	/** Determines if a job will fallback to checkpoint when there is a more recent savepoint. **/
	private boolean preferCheckpointForRecovery = false;

	/**
	 * Determines the threshold that we tolerance declined checkpoint failure number.
	 * The default value is -1 meaning undetermined and not set via {@link #setTolerableCheckpointFailureNumber(int)}.
	 * */
	private int tolerableCheckpointFailureNumber = UNDEFINED_TOLERABLE_CHECKPOINT_NUMBER;

	// ------------------------------------------------------------------------

	/**
	 * Checks whether checkpointing is enabled.
	 *
	 * @return True if checkpointing is enables, false otherwise.
	 */
	public boolean isCheckpointingEnabled() {
		return checkpointInterval > 0;
	}

	/**
	 * Gets the checkpointing mode (exactly-once vs. at-least-once).
	 *
	 * @return The checkpointing mode.
	 */
	public CheckpointingMode getCheckpointingMode() {
		return checkpointingMode;
	}

	/**
	 * Sets the checkpointing mode (exactly-once vs. at-least-once).
	 *
	 * @param checkpointingMode The checkpointing mode.
	 */
	public void setCheckpointingMode(CheckpointingMode checkpointingMode) {
		this.checkpointingMode = requireNonNull(checkpointingMode);
	}

	/**
	 * Gets the maximum time that a checkpoint may take before being discarded.
	 *
	 * @return The checkpoint timeout, in milliseconds.
	 */
	public long getCheckpointTimeout() {
		return checkpointTimeout;
	}

	/**
	 * Sets the maximum time that a checkpoint may take before being discarded.
	 *
	 * @param checkpointTimeout The checkpoint timeout, in milliseconds.
	 */
	public void setCheckpointTimeout(long checkpointTimeout) {
		if (checkpointTimeout < MINIMAL_CHECKPOINT_TIME) {
			throw new IllegalArgumentException(String.format("Checkpoint timeout must be larger than or equal to %s ms", MINIMAL_CHECKPOINT_TIME));
		}
		this.checkpointTimeout = checkpointTimeout;
	}

	public void setMinPauseBetweenCheckpoints(long minPauseBetweenCheckpoints) {
		if (minPauseBetweenCheckpoints < 0) {
			throw new IllegalArgumentException("Pause value must be zero or positive");
		}
		this.minPauseBetweenCheckpoints = minPauseBetweenCheckpoints;
	}

	/**
	 * Checks whether checkpointing is forced, despite currently non-checkpointable iteration feedback.
	 *
	 * @return True, if checkpointing is forced, false otherwise.
	 *
	 * @deprecated This will be removed once iterations properly participate in checkpointing.
	 */
	@Deprecated
	@PublicEvolving
	public boolean isForceCheckpointing() {
		return forceCheckpointing;
	}

	/**
	 * Checks whether checkpointing is forced, despite currently non-checkpointable iteration feedback.
	 *
	 * @param forceCheckpointing The flag to force checkpointing.
	 *
	 * @deprecated This will be removed once iterations properly participate in checkpointing.
	 */
	@Deprecated
	@PublicEvolving
	public void setForceCheckpointing(boolean forceCheckpointing) {
		this.forceCheckpointing = forceCheckpointing;
	}

	/**
	 * This determines the behaviour when meeting checkpoint errors.
	 * If this returns true, which is equivalent to get tolerableCheckpointFailureNumber as zero, job manager would
	 * fail the whole job once it received a decline checkpoint message.
	 * If this returns false, which is equivalent to get tolerableCheckpointFailureNumber as the maximum of integer (means unlimited),
	 * job manager would not fail the whole job no matter how many declined checkpoints it received.
	 *
	 * @deprecated Use {@link #getTolerableCheckpointFailureNumber()}.
	 */
	@Deprecated
	public boolean isFailOnCheckpointingErrors() {
		return failOnCheckpointingErrors;
	}

	/**
	 * Sets the expected behaviour for tasks in case that they encounter an error when checkpointing.
	 * If this is set as true, which is equivalent to set tolerableCheckpointFailureNumber as zero, job manager would
	 * fail the whole job once it received a decline checkpoint message.
	 * If this is set as false, which is equivalent to set tolerableCheckpointFailureNumber as the maximum of integer (means unlimited),
	 * job manager would not fail the whole job no matter how many declined checkpoints it received.
	 *
	 * <p>{@link #setTolerableCheckpointFailureNumber(int)} would always overrule this deprecated method if they have conflicts.
	 *
	 * @deprecated Use {@link #setTolerableCheckpointFailureNumber(int)}.
	 */
	@Deprecated
	public void setFailOnCheckpointingErrors(boolean failOnCheckpointingErrors) {
	}

	/**
	 * Get the tolerable checkpoint failure number which used by the checkpoint failure manager
	 * to determine when we need to fail the job.
	 *
	 * <p>If the {@link #tolerableCheckpointFailureNumber} has not been configured, this method would return 0
	 * which means the checkpoint failure manager would not tolerate any declined checkpoint failure.
	 */
	public int getTolerableCheckpointFailureNumber() {
		if (tolerableCheckpointFailureNumber == UNDEFINED_TOLERABLE_CHECKPOINT_NUMBER) {
			return 0;
		}
		return tolerableCheckpointFailureNumber;
	}

	/**
	 * Set the tolerable checkpoint failure number, the default value is 0 that means
	 * we do not tolerance any checkpoint failure.
	 */
	public void setTolerableCheckpointFailureNumber(int tolerableCheckpointFailureNumber) {
		if (tolerableCheckpointFailureNumber < 0) {
			throw new IllegalArgumentException("The tolerable failure checkpoint number must be non-negative.");
		}
		this.tolerableCheckpointFailureNumber = tolerableCheckpointFailureNumber;
	}

	/**
	 * Cleanup behaviour for externalized checkpoints when the job is cancelled.
	 */
	@PublicEvolving
	public enum ExternalizedCheckpointCleanup {

		/**
		 * Delete externalized checkpoints on job cancellation.
		 *
		 * <p>All checkpoint state will be deleted when you cancel the owning
		 * job, both the meta data and actual program state. Therefore, you
		 * cannot resume from externalized checkpoints after the job has been
		 * cancelled.
		 *
		 * <p>Note that checkpoint state is always kept if the job terminates
		 * with state {@link JobStatus#FAILED}.
		 */
		DELETE_ON_CANCELLATION(true),

		/**
		 * Retain externalized checkpoints on job cancellation.
		 *
		 * <p>All checkpoint state is kept when you cancel the owning job. You
		 * have to manually delete both the checkpoint meta data and actual
		 * program state after cancelling the job.
		 *
		 * <p>Note that checkpoint state is always kept if the job terminates
		 * with state {@link JobStatus#FAILED}.
		 */
		RETAIN_ON_CANCELLATION(false);

		private final boolean deleteOnCancellation;

		ExternalizedCheckpointCleanup(boolean deleteOnCancellation) {
			this.deleteOnCancellation = deleteOnCancellation;
		}

		/**
		 * Returns whether persistent checkpoints shall be discarded on
		 * cancellation of the job.
		 *
		 * @return <code>true</code> if persistent checkpoints shall be discarded
		 * on cancellation of the job.
		 */
		public boolean deleteOnCancellation() {
			return deleteOnCancellation;
		}
	}

	public void configure(ReadableConfig configuration) {
	}
}
