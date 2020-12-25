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
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.AccumulatorHelper;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.accumulators.AccumulatorSnapshot;
import org.apache.flink.runtime.accumulators.StringifiedAccumulatorResult;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.FutureUtils.ConjunctFuture;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.ResultPartitionAvailabilityChecker;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.query.KvStateLocationRegistry;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.adapter.DefaultExecutionTopology;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.SchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.SchedulingTopology;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.types.Either;
import org.apache.flink.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class ExecutionGraph implements AccessExecutionGraph {

	/** The log object used for debugging. */
	static final Logger LOG = LoggerFactory.getLogger(ExecutionGraph.class);

	// --------------------------------------------------------------------------------------------

	/** Job specific information like the job id, job name, job configuration, etc. */
	private final JobInformation jobInformation;

	/** Serialized job information or a blob key pointing to the offloaded job information. */
	private final Either<SerializedValue<JobInformation>, PermanentBlobKey> jobInformationOrBlobKey;

	/** The executor which is used to execute futures. */
	private final ScheduledExecutorService futureExecutor;

	/** The executor which is used to execute blocking io operations. */
	private final Executor ioExecutor;

	/** Executor that runs tasks in the job manager's main thread. */
	@Nonnull
	private ComponentMainThreadExecutor jobMasterMainThreadExecutor;

	/** {@code true} if all source tasks are stoppable. */
	private boolean isStoppable = true;

	/** All job vertices that are part of this graph. */
	private final Map<JobVertexID, ExecutionJobVertex> tasks;

	/** All vertices, in the order in which they were created. **/
	private final List<ExecutionJobVertex> verticesInCreationOrder;

	/** All intermediate results that are part of this graph. */
	private final Map<IntermediateDataSetID, IntermediateResult> intermediateResults;

	/** The currently executed tasks, for callbacks. */
	private final Map<ExecutionAttemptID, Execution> currentExecutions;

	/** Listeners that receive messages when the entire job switches it status
	 * (such as from RUNNING to FINISHED). */
	private final List<JobStatusListener> jobStatusListeners;

	/** The implementation that decides how to recover the failures of tasks. */
	private final FailoverStrategy failoverStrategy;

	/** Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()} when
	 * the execution graph transitioned into a certain state. The index into this array is the
	 * ordinal of the enum value, i.e. the timestamp when the graph went into state "RUNNING" is
	 * at {@code stateTimestamps[RUNNING.ordinal()]}. */
	private final long[] stateTimestamps;

	/** The timeout for all messages that require a response/acknowledgement. */
	private final Time rpcTimeout;

	/** The timeout for slot allocations. */
	private final Time allocationTimeout;

	/** The slot provider strategy to use for allocating slots for tasks as they are needed. */
	private final SlotProviderStrategy slotProviderStrategy;

	/** The classloader for the user code. Needed for calls into user code classes. */
	private final ClassLoader userClassLoader;

	/** Registered KvState instances reported by the TaskManagers. */
	private final KvStateLocationRegistry kvStateLocationRegistry;

	/** Blob writer used to offload RPC messages. */
	private final BlobWriter blobWriter;

	private boolean legacyScheduling = true;

	/** The total number of vertices currently in the execution graph. */
	private int numVerticesTotal;

	private final PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory;

	private PartitionReleaseStrategy partitionReleaseStrategy;

	private DefaultExecutionTopology executionTopology;

	@Nullable
	private InternalFailuresListener internalTaskFailuresListener;

	// ------ Configuration of the Execution -------

	/** The mode of scheduling. Decides how to select the initial set of tasks to be deployed.
	 * May indicate to deploy all sources, or to deploy everything, or to deploy via backtracking
	 * from results than need to be materialized. */
	private final ScheduleMode scheduleMode;

	/** The maximum number of prior execution attempts kept in history. */
	private final int maxPriorAttemptsHistoryLength;

	// ------ Execution status and progress. These values are volatile, and accessed under the lock -------

	private int verticesFinished;

	/** Current status of the job execution. */
	private volatile JobStatus state = JobStatus.CREATED;

	/** A future that completes once the job has reached a terminal state. */
	private final CompletableFuture<JobStatus> terminationFuture = new CompletableFuture<>();

	/** On each global recovery, this version is incremented. The version breaks conflicts
	 * between concurrent restart attempts by local failover strategies. */
	private long globalModVersion;

	/** The exception that caused the job to fail. This is set to the first root exception
	 * that was not recoverable and triggered job failure. */
	private Throwable failureCause;

	/** The extended failure cause information for the job. This exists in addition to 'failureCause',
	 * to let 'failureCause' be a strong reference to the exception, while this info holds no
	 * strong reference to any user-defined classes.*/
	private ErrorInfo failureInfo;

	private final JobMasterPartitionTracker partitionTracker;

	private final ResultPartitionAvailabilityChecker resultPartitionAvailabilityChecker;

	/**
	 * Future for an ongoing or completed scheduling action.
	 */
	@Nullable
	private CompletableFuture<Void> schedulingFuture;

	// ------ Fields that are relevant to the execution and need to be cleared before archiving  -------

	private String jsonPlan;

	/** Shuffle master to register partitions for task deployment. */
	private final ShuffleMaster<?> shuffleMaster;

	// --------------------------------------------------------------------------------------------
	//   Constructors
	// --------------------------------------------------------------------------------------------

	public ExecutionGraph(
			JobInformation jobInformation,
			ScheduledExecutorService futureExecutor,
			Executor ioExecutor,
			Time rpcTimeout,
			int maxPriorAttemptsHistoryLength,
			FailoverStrategy.Factory failoverStrategyFactory,
			SlotProvider slotProvider,
			ClassLoader userClassLoader,
			BlobWriter blobWriter,
			Time allocationTimeout,
			PartitionReleaseStrategy.Factory partitionReleaseStrategyFactory,
			ShuffleMaster<?> shuffleMaster,
			JobMasterPartitionTracker partitionTracker,
			ScheduleMode scheduleMode) throws IOException {

		this.jobInformation = Preconditions.checkNotNull(jobInformation);

		this.blobWriter = Preconditions.checkNotNull(blobWriter);

		this.scheduleMode = checkNotNull(scheduleMode);

		this.jobInformationOrBlobKey = BlobWriter.serializeAndTryOffload(jobInformation, jobInformation.getJobId(), blobWriter);

		this.futureExecutor = Preconditions.checkNotNull(futureExecutor);
		this.ioExecutor = Preconditions.checkNotNull(ioExecutor);

		this.slotProviderStrategy = SlotProviderStrategy.from(
			scheduleMode,
			slotProvider,
			allocationTimeout);
		this.userClassLoader = Preconditions.checkNotNull(userClassLoader, "userClassLoader");

		this.tasks = new HashMap<>(16);
		this.intermediateResults = new HashMap<>(16);
		this.verticesInCreationOrder = new ArrayList<>(16);
		this.currentExecutions = new HashMap<>(16);

		this.jobStatusListeners  = new ArrayList<>();

		this.stateTimestamps = new long[JobStatus.values().length];
		this.stateTimestamps[JobStatus.CREATED.ordinal()] = System.currentTimeMillis();

		this.rpcTimeout = checkNotNull(rpcTimeout);
		this.allocationTimeout = checkNotNull(allocationTimeout);

		this.partitionReleaseStrategyFactory = checkNotNull(partitionReleaseStrategyFactory);

		this.kvStateLocationRegistry = new KvStateLocationRegistry(jobInformation.getJobId(), getAllVertices());

		this.globalModVersion = 1L;

		// the failover strategy must be instantiated last, so that the execution graph
		// is ready by the time the failover strategy sees it
		this.failoverStrategy = checkNotNull(failoverStrategyFactory.create(this), "null failover strategy");

		this.maxPriorAttemptsHistoryLength = maxPriorAttemptsHistoryLength;

		this.schedulingFuture = null;
		this.jobMasterMainThreadExecutor =
			new ComponentMainThreadExecutor.DummyComponentMainThreadExecutor(
				"ExecutionGraph is not initialized with proper main thread executor. " +
					"Call to ExecutionGraph.start(...) required.");

		this.shuffleMaster = checkNotNull(shuffleMaster);

		this.partitionTracker = checkNotNull(partitionTracker);

		this.resultPartitionAvailabilityChecker = new ExecutionGraphResultPartitionAvailabilityChecker(
			this::createResultPartitionId,
			partitionTracker);
	}

	public void start(@Nonnull ComponentMainThreadExecutor jobMasterMainThreadExecutor) {
		this.jobMasterMainThreadExecutor = jobMasterMainThreadExecutor;
	}

	// --------------------------------------------------------------------------------------------
	//  Configuration of Data-flow wide execution settings
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets the number of job vertices currently held by this execution graph.
	 * @return The current number of job vertices.
	 */
	public int getNumberOfExecutionJobVertices() {
		return this.verticesInCreationOrder.size();
	}

	public SchedulingTopology getSchedulingTopology() {
		return executionTopology;
	}

	public ScheduleMode getScheduleMode() {
		return scheduleMode;
	}

	public Time getAllocationTimeout() {
		return allocationTimeout;
	}

	@Nonnull
	public ComponentMainThreadExecutor getJobMasterMainThreadExecutor() {
		return jobMasterMainThreadExecutor;
	}

	@Override
	public boolean isArchived() {
		return false;
	}

	public KvStateLocationRegistry getKvStateLocationRegistry() {
		return kvStateLocationRegistry;
	}

	// --------------------------------------------------------------------------------------------
	//  Properties and Status of the Execution Graph
	// --------------------------------------------------------------------------------------------

	public void setJsonPlan(String jsonPlan) {
		this.jsonPlan = jsonPlan;
	}

	@Override
	public String getJsonPlan() {
		return jsonPlan;
	}

	public SlotProviderStrategy getSlotProviderStrategy() {
		return slotProviderStrategy;
	}

	public Either<SerializedValue<JobInformation>, PermanentBlobKey> getJobInformationOrBlobKey() {
		return jobInformationOrBlobKey;
	}

	@Override
	public JobID getJobID() {
		return jobInformation.getJobId();
	}

	@Override
	public String getJobName() {
		return jobInformation.getJobName();
	}

	@Override
	public boolean isStoppable() {
		return this.isStoppable;
	}

	public Configuration getJobConfiguration() {
		return jobInformation.getJobConfiguration();
	}

	public ClassLoader getUserClassLoader() {
		return this.userClassLoader;
	}

	@Override
	public JobStatus getState() {
		return state;
	}

	public Throwable getFailureCause() {
		return failureCause;
	}

	public ErrorInfo getFailureInfo() {
		return failureInfo;
	}

	@Override
	public ExecutionJobVertex getJobVertex(JobVertexID id) {
		return this.tasks.get(id);
	}

	@Override
	public Map<JobVertexID, ExecutionJobVertex> getAllVertices() {
		return Collections.unmodifiableMap(this.tasks);
	}

	@Override
	public Iterable<ExecutionJobVertex> getVerticesTopologically() {
		// we return a specific iterator that does not fail with concurrent modifications
		// the list is append only, so it is safe for that
		final int numElements = this.verticesInCreationOrder.size();

		return new Iterable<ExecutionJobVertex>() {
			@Override
			public Iterator<ExecutionJobVertex> iterator() {
				return new Iterator<ExecutionJobVertex>() {
					private int pos = 0;

					@Override
					public boolean hasNext() {
						return pos < numElements;
					}

					@Override
					public ExecutionJobVertex next() {
						if (hasNext()) {
							return verticesInCreationOrder.get(pos++);
						} else {
							throw new NoSuchElementException();
						}
					}

					@Override
					public void remove() {
						throw new UnsupportedOperationException();
					}
				};
			}
		};
	}

	public int getTotalNumberOfVertices() {
		return numVerticesTotal;
	}

	public Map<IntermediateDataSetID, IntermediateResult> getAllIntermediateResults() {
		return Collections.unmodifiableMap(this.intermediateResults);
	}

	@Override
	public Iterable<ExecutionVertex> getAllExecutionVertices() {
		return new Iterable<ExecutionVertex>() {
			@Override
			public Iterator<ExecutionVertex> iterator() {
				return new AllVerticesIterator(getVerticesTopologically().iterator());
			}
		};
	}

	@Override
	public long getStatusTimestamp(JobStatus status) {
		return this.stateTimestamps[status.ordinal()];
	}

	public final BlobWriter getBlobWriter() {
		return blobWriter;
	}

	/**
	 * Returns the ExecutionContext associated with this ExecutionGraph.
	 *
	 * @return ExecutionContext associated with this ExecutionGraph
	 */
	public Executor getFutureExecutor() {
		return futureExecutor;
	}

	/**
	 * Merges all accumulator results from the tasks previously executed in the Executions.
	 * @return The accumulator map
	 */
	public Map<String, OptionalFailure<Accumulator<?, ?>>> aggregateUserAccumulators() {

		Map<String, OptionalFailure<Accumulator<?, ?>>> userAccumulators = new HashMap<>();

		for (ExecutionVertex vertex : getAllExecutionVertices()) {
			Map<String, Accumulator<?, ?>> next = vertex.getCurrentExecutionAttempt().getUserAccumulators();
			if (next != null) {
				AccumulatorHelper.mergeInto(userAccumulators, next);
			}
		}

		return userAccumulators;
	}

	/**
	 * Gets a serialized accumulator map.
	 * @return The accumulator map with serialized accumulator values.
	 */
	@Override
	public Map<String, SerializedValue<OptionalFailure<Object>>> getAccumulatorsSerialized() {
		return aggregateUserAccumulators()
			.entrySet()
			.stream()
			.collect(Collectors.toMap(
				Map.Entry::getKey,
				entry -> serializeAccumulator(entry.getKey(), entry.getValue())));
	}

	private static SerializedValue<OptionalFailure<Object>> serializeAccumulator(String name, OptionalFailure<Accumulator<?, ?>> accumulator) {
		try {
			if (accumulator.isFailure()) {
				return new SerializedValue<>(OptionalFailure.ofFailure(accumulator.getFailureCause()));
			}
			return new SerializedValue<>(OptionalFailure.of(accumulator.getUnchecked().getLocalValue()));
		} catch (IOException ioe) {
			LOG.error("Could not serialize accumulator " + name + '.', ioe);
			try {
				return new SerializedValue<>(OptionalFailure.ofFailure(ioe));
			} catch (IOException e) {
				throw new RuntimeException("It should never happen that we cannot serialize the accumulator serialization exception.", e);
			}
		}
	}

	/**
	 * Returns the a stringified version of the user-defined accumulators.
	 * @return an Array containing the StringifiedAccumulatorResult objects
	 */
	@Override
	public StringifiedAccumulatorResult[] getAccumulatorResultsStringified() {
		Map<String, OptionalFailure<Accumulator<?, ?>>> accumulatorMap = aggregateUserAccumulators();
		return StringifiedAccumulatorResult.stringifyAccumulatorResults(accumulatorMap);
	}

	public void enableNgScheduling(final InternalFailuresListener internalTaskFailuresListener) {
		this.internalTaskFailuresListener = internalTaskFailuresListener;
		this.legacyScheduling = false;
	}

	// --------------------------------------------------------------------------------------------
	//  Actions
	// --------------------------------------------------------------------------------------------

	public void attachJobGraph(List<JobVertex> topologiallySorted) throws JobException {

		assertRunningInJobMasterMainThread();

		LOG.debug("Attaching {} topologically sorted vertices to existing job graph with {} " +
				"vertices and {} intermediate results.",
			topologiallySorted.size(),
			tasks.size(),
			intermediateResults.size());

		final ArrayList<ExecutionJobVertex> newExecJobVertices = new ArrayList<>(topologiallySorted.size());
		final long createTimestamp = System.currentTimeMillis();

		for (JobVertex jobVertex : topologiallySorted) {

			if (jobVertex.isInputVertex() && !jobVertex.isStoppable()) {
				this.isStoppable = false;
			}

			// create the execution job vertex and attach it to the graph
			ExecutionJobVertex ejv = new ExecutionJobVertex(
					this,
					jobVertex,
					1,
					maxPriorAttemptsHistoryLength,
					rpcTimeout,
					globalModVersion,
					createTimestamp);

			ejv.connectToPredecessors(this.intermediateResults);

			ExecutionJobVertex previousTask = this.tasks.putIfAbsent(jobVertex.getID(), ejv);
			if (previousTask != null) {
				throw new JobException(String.format("Encountered two job vertices with ID %s : previous=[%s] / new=[%s]",
					jobVertex.getID(), ejv, previousTask));
			}

			for (IntermediateResult res : ejv.getProducedDataSets()) {
				IntermediateResult previousDataSet = this.intermediateResults.putIfAbsent(res.getId(), res);
				if (previousDataSet != null) {
					throw new JobException(String.format("Encountered two intermediate data set with ID %s : previous=[%s] / new=[%s]",
						res.getId(), res, previousDataSet));
				}
			}

			this.verticesInCreationOrder.add(ejv);
			this.numVerticesTotal += ejv.getParallelism();
			newExecJobVertices.add(ejv);
		}

		// the topology assigning should happen before notifying new vertices to failoverStrategy
		executionTopology = new DefaultExecutionTopology(this);

		failoverStrategy.notifyNewVertices(newExecJobVertices);

		partitionReleaseStrategy = partitionReleaseStrategyFactory.createInstance(getSchedulingTopology());
	}

	public boolean isLegacyScheduling() {
		return legacyScheduling;
	}

	public void transitionToRunning() {
		if (!transitionState(JobStatus.CREATED, JobStatus.RUNNING)) {
			throw new IllegalStateException("Job may only be scheduled from state " + JobStatus.CREATED);
		}
	}

	public void cancel() {
	}

	private ConjunctFuture<Void> cancelVerticesAsync() {
		final ArrayList<CompletableFuture<?>> futures = new ArrayList<>(verticesInCreationOrder.size());

		// cancel all tasks (that still need cancelling)
		for (ExecutionJobVertex ejv : verticesInCreationOrder) {
			futures.add(ejv.cancelWithFuture());
		}

		// we build a future that is complete once all vertices have reached a terminal state
		return FutureUtils.waitForAll(futures);
	}

	/**
	 * Fails the execution graph globally. This failure will not be recovered by a specific
	 * failover strategy, but results in a full restart of all tasks.
	 *
	 * <p>This global failure is meant to be triggered in cases where the consistency of the
	 * execution graph' state cannot be guaranteed any more (for example when catching unexpected
	 * exceptions that indicate a bug or an unexpected call race), and where a full restart is the
	 * safe way to get consistency back.
	 *
	 * @param t The exception that caused the failure.
	 */
	public void failGlobal(Throwable t) {
		if (!isLegacyScheduling()) {
			internalTaskFailuresListener.notifyGlobalFailure(t);
			return;
		}

		assertRunningInJobMasterMainThread();

		while (true) {
			JobStatus current = state;
			// stay in these states
			if (current == JobStatus.FAILING ||
				current == JobStatus.SUSPENDED ||
				current.isGloballyTerminalState()) {
				return;
			} else if (transitionState(current, JobStatus.FAILING, t)) {
				initFailureCause(t);

				// make sure no concurrent local or global actions interfere with the failover
				final long globalVersionForRestart = incrementGlobalModVersion();

				final CompletableFuture<Void> ongoingSchedulingFuture = schedulingFuture;

				// cancel ongoing scheduling action
				if (ongoingSchedulingFuture != null) {
					ongoingSchedulingFuture.cancel(false);
				}

				// we build a future that is complete once all vertices have reached a terminal state
				final ConjunctFuture<Void> allTerminal = cancelVerticesAsync();
				FutureUtils.assertNoException(allTerminal.handle(
					(Void ignored, Throwable throwable) -> {
						if (throwable != null) {
							transitionState(
								JobStatus.FAILING,
								JobStatus.FAILED,
								new FlinkException("Could not cancel all execution job vertices properly.", throwable));
						} else {
							allVerticesInTerminalState(globalVersionForRestart);
						}
						return null;
					}));

				return;
			}

			// else: concurrent change to execution state, retry
		}
	}

	/**
	 * Returns the serializable {@link ArchivedExecutionConfig}.
	 *
	 * @return ArchivedExecutionConfig which may be null in case of errors
	 */
	@Override
	public ArchivedExecutionConfig getArchivedExecutionConfig() {
		// create a summary of all relevant data accessed in the web interface's JobConfigHandler
		try {
			ExecutionConfig executionConfig = jobInformation.getSerializedExecutionConfig().deserializeValue(userClassLoader);
			if (executionConfig != null) {
				return executionConfig.archive();
			}
		} catch (IOException | ClassNotFoundException e) {
			LOG.error("Couldn't create ArchivedExecutionConfig for job {} ", getJobID(), e);
		}
		return null;
	}

	/**
	 * Returns the termination future of this {@link ExecutionGraph}. The termination future
	 * is completed with the terminal {@link JobStatus} once the ExecutionGraph reaches this
	 * terminal state and all {@link Execution} have been terminated.
	 *
	 * @return Termination future of this {@link ExecutionGraph}.
	 */
	public CompletableFuture<JobStatus> getTerminationFuture() {
		return terminationFuture;
	}

	/**
	 * Gets the failover strategy used by the execution graph to recover from failures of tasks.
	 */
	public FailoverStrategy getFailoverStrategy() {
		return this.failoverStrategy;
	}

	/**
	 * Gets the current global modification version of the ExecutionGraph.
	 * The global modification version is incremented with each global action (cancel/fail/restart)
	 * and is used to disambiguate concurrent modifications between local and global
	 * failover actions.
	 */
	public long getGlobalModVersion() {
		return globalModVersion;
	}

	// ------------------------------------------------------------------------
	//  State Transitions
	// ------------------------------------------------------------------------

	public boolean transitionState(JobStatus current, JobStatus newState) {
		return transitionState(current, newState, null);
	}

	private void transitionState(JobStatus newState, Throwable error) {
		transitionState(state, newState, error);
	}

	private boolean transitionState(JobStatus current, JobStatus newState, Throwable error) {
		assertRunningInJobMasterMainThread();
		// consistency check
		if (current.isTerminalState()) {
			String message = "Job is trying to leave terminal state " + current;
			LOG.error(message);
			throw new IllegalStateException(message);
		}

		// now do the actual state transition
		if (state == current) {
			state = newState;
			LOG.info("Job {} ({}) switched from state {} to {}.", getJobName(), getJobID(), current, newState, error);

			stateTimestamps[newState.ordinal()] = System.currentTimeMillis();
			notifyJobStatusChange(newState, error);
			return true;
		}
		else {
			return false;
		}
	}

	private long incrementGlobalModVersion() {
		return ++globalModVersion;
	}

	public void initFailureCause(Throwable t) {
		this.failureCause = t;
		this.failureInfo = new ErrorInfo(t, System.currentTimeMillis());
	}

	// ------------------------------------------------------------------------
	//  Job Status Progress
	// ------------------------------------------------------------------------

	/**
	 * Called whenever a vertex reaches state FINISHED (completed successfully).
	 * Once all vertices are in the FINISHED state, the program is successfully done.
	 */
	void vertexFinished() {
		assertRunningInJobMasterMainThread();
		final int numFinished = ++verticesFinished;
		if (numFinished == numVerticesTotal) {
			// done :-)

			// check whether we are still in "RUNNING" and trigger the final cleanup
			if (state == JobStatus.RUNNING) {
				// we do the final cleanup in the I/O executor, because it may involve
				// some heavier work

				try {
					for (ExecutionJobVertex ejv : verticesInCreationOrder) {
						ejv.getJobVertex().finalizeOnMaster(getUserClassLoader());
					}
				}
				catch (Throwable t) {
					ExceptionUtils.rethrowIfFatalError(t);
					failGlobal(new Exception("Failed to finalize execution on master", t));
					return;
				}

				// if we do not make this state transition, then a concurrent
				// cancellation or failure happened
				if (transitionState(JobStatus.RUNNING, JobStatus.FINISHED)) {
					onTerminalState(JobStatus.FINISHED);
				}
			}
		}
	}

	/**
	 * This method is a callback during cancellation/failover and called when all tasks
	 * have reached a terminal state (cancelled/failed/finished).
	 */
	private void allVerticesInTerminalState(long expectedGlobalVersionForRestart) {

		assertRunningInJobMasterMainThread();

		// we are done, transition to the final state
		JobStatus current;
		while (true) {
			current = this.state;

			if (current == JobStatus.RUNNING) {
				failGlobal(new Exception("ExecutionGraph went into allVerticesInTerminalState() from RUNNING"));
			}
			else if (current == JobStatus.CANCELLING) {
				if (transitionState(current, JobStatus.CANCELED)) {
					onTerminalState(JobStatus.CANCELED);
					break;
				}
			}
			else if (current.isGloballyTerminalState()) {
				LOG.warn("Job has entered globally terminal state without waiting for all " +
					"job vertices to reach final state.");
				break;
			}
			else {
				failGlobal(new Exception("ExecutionGraph went into final state from state " + current));
				break;
			}
		}
		// done transitioning the state
	}

	public void failJob(Throwable cause) {
		if (state == JobStatus.FAILING || state.isGloballyTerminalState()) {
			return;
		}

		transitionState(JobStatus.FAILING, cause);
		initFailureCause(cause);

		FutureUtils.assertNoException(
			cancelVerticesAsync().whenComplete((aVoid, throwable) -> {
				transitionState(JobStatus.FAILED, cause);
				onTerminalState(JobStatus.FAILED);
			}));
	}

	private void onTerminalState(JobStatus status) {
		terminationFuture.complete(status);
	}

	// --------------------------------------------------------------------------------------------
	//  Callbacks and Callback Utilities
	// --------------------------------------------------------------------------------------------

	/**
	 * Updates the state of one of the ExecutionVertex's Execution attempts.
	 * If the new status if "FINISHED", this also updates the accumulators.
	 *
	 * @param state The state update.
	 * @return True, if the task update was properly applied, false, if the execution attempt was not found.
	 */
	public boolean updateState(TaskExecutionState state) {
		assertRunningInJobMasterMainThread();
		final Execution attempt = currentExecutions.get(state.getID());

		if (attempt != null) {
			try {
				final boolean stateUpdated = updateStateInternal(state, attempt);
				maybeReleasePartitions(attempt);
				return stateUpdated;
			}
			catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalErrorOrOOM(t);

				// failures during updates leave the ExecutionGraph inconsistent
				failGlobal(t);
				return false;
			}
		}
		else {
			return false;
		}
	}

	private boolean updateStateInternal(final TaskExecutionState state, final Execution attempt) {
		Map<String, Accumulator<?, ?>> accumulators;

		switch (state.getExecutionState()) {
			case RUNNING:
				return attempt.switchToRunning();

			case FINISHED:
				// this deserialization is exception-free
				accumulators = deserializeAccumulators(state);
				attempt.markFinished(accumulators);
				return true;

			case CANCELED:
				// this deserialization is exception-free
				accumulators = deserializeAccumulators(state);
				attempt.completeCancelling(accumulators, false);
				return true;

			default:
				// we mark as failed and return false, which triggers the TaskManager
				// to remove the task
				attempt.fail(new Exception("TaskManager sent illegal state update: " + state.getExecutionState()));
				return false;
		}
	}

	private void maybeReleasePartitions(final Execution attempt) {
		final ExecutionVertexID finishedExecutionVertex = attempt.getVertex().getID();

		if (attempt.getState() == ExecutionState.FINISHED) {
			final List<IntermediateResultPartitionID> releasablePartitions = partitionReleaseStrategy.vertexFinished(finishedExecutionVertex);
			releasePartitions(releasablePartitions);
		} else {
			partitionReleaseStrategy.vertexUnfinished(finishedExecutionVertex);
		}
	}

	private void releasePartitions(final List<IntermediateResultPartitionID> releasablePartitions) {
		if (releasablePartitions.size() > 0) {
			final List<ResultPartitionID> partitionIds = releasablePartitions.stream()
				.map(this::createResultPartitionId)
				.collect(Collectors.toList());

			partitionTracker.stopTrackingAndReleasePartitions(partitionIds);
		}
	}

	ResultPartitionID createResultPartitionId(final IntermediateResultPartitionID resultPartitionId) {
		final SchedulingResultPartition schedulingResultPartition =
			getSchedulingTopology().getResultPartition(resultPartitionId);
		final SchedulingExecutionVertex producer = schedulingResultPartition.getProducer();
		final ExecutionVertexID producerId = producer.getId();
		final JobVertexID jobVertexId = producerId.getJobVertexId();
		final ExecutionJobVertex jobVertex = getJobVertex(jobVertexId);
		checkNotNull(jobVertex, "Unknown job vertex %s", jobVertexId);

		final ExecutionVertex[] taskVertices = jobVertex.getTaskVertices();
		final int subtaskIndex = producerId.getSubtaskIndex();
		checkState(subtaskIndex < taskVertices.length, "Invalid subtask index %d for job vertex %s", subtaskIndex, jobVertexId);

		final ExecutionVertex taskVertex = taskVertices[subtaskIndex];
		final Execution execution = taskVertex.getCurrentExecutionAttempt();
		return new ResultPartitionID(resultPartitionId, execution.getAttemptId());
	}

	/**
	 * Deserializes accumulators from a task state update.
	 *
	 * <p>This method never throws an exception!
	 *
	 * @param state The task execution state from which to deserialize the accumulators.
	 * @return The deserialized accumulators, of null, if there are no accumulators or an error occurred.
	 */
	private Map<String, Accumulator<?, ?>> deserializeAccumulators(TaskExecutionState state) {
		AccumulatorSnapshot serializedAccumulators = state.getAccumulators();

		if (serializedAccumulators != null) {
			try {
				return serializedAccumulators.deserializeUserAccumulators(userClassLoader);
			}
			catch (Throwable t) {
				// we catch Throwable here to include all form of linking errors that may
				// occur if user classes are missing in the classpath
				LOG.error("Failed to deserialize final accumulator results.", t);
			}
		}
		return null;
	}

	/**
	 * Schedule or updates consumers of the given result partition.
	 *
	 * @param partitionId specifying the result partition whose consumer shall be scheduled or updated
	 * @throws ExecutionGraphException if the schedule or update consumers operation could not be executed
	 */
	public void scheduleOrUpdateConsumers(ResultPartitionID partitionId) throws ExecutionGraphException {

		assertRunningInJobMasterMainThread();

		final Execution execution = currentExecutions.get(partitionId.getProducerId());

		if (execution == null) {
			throw new ExecutionGraphException("Cannot find execution for execution Id " +
				partitionId.getPartitionId() + '.');
		}
		else if (execution.getVertex() == null){
			throw new ExecutionGraphException("Execution with execution Id " +
				partitionId.getPartitionId() + " has no vertex assigned.");
		} else {
			execution.getVertex().scheduleOrUpdateConsumers(partitionId);
		}
	}

	public Map<ExecutionAttemptID, Execution> getRegisteredExecutions() {
		return Collections.unmodifiableMap(currentExecutions);
	}

	void registerExecution(Execution exec) {
		assertRunningInJobMasterMainThread();
		Execution previous = currentExecutions.putIfAbsent(exec.getAttemptId(), exec);
		if (previous != null) {
			failGlobal(new Exception("Trying to register execution " + exec + " for already used ID " + exec.getAttemptId()));
		}
	}

	void deregisterExecution(Execution exec) {
		assertRunningInJobMasterMainThread();
		Execution contained = currentExecutions.remove(exec.getAttemptId());

		if (contained != null && contained != exec) {
			failGlobal(new Exception("De-registering execution " + exec + " failed. Found for same ID execution " + contained));
		}
	}

	/**
	 * Updates the accumulators during the runtime of a job. Final accumulator results are transferred
	 * through the UpdateTaskExecutionState message.
	 * @param accumulatorSnapshot The serialized flink and user-defined accumulators
	 */
	public void updateAccumulators(AccumulatorSnapshot accumulatorSnapshot) {
		Map<String, Accumulator<?, ?>> userAccumulators;
		try {
			userAccumulators = accumulatorSnapshot.deserializeUserAccumulators(userClassLoader);

			ExecutionAttemptID execID = accumulatorSnapshot.getExecutionAttemptID();
			Execution execution = currentExecutions.get(execID);
			if (execution != null) {
				execution.setAccumulators(userAccumulators);
			} else {
				LOG.debug("Received accumulator result for unknown execution {}.", execID);
			}
		} catch (Exception e) {
			LOG.error("Cannot update accumulators for job {}.", getJobID(), e);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Listeners & Observers
	// --------------------------------------------------------------------------------------------

	public void registerJobStatusListener(JobStatusListener listener) {
		if (listener != null) {
			jobStatusListeners.add(listener);
		}
	}

	private void notifyJobStatusChange(JobStatus newState, Throwable error) {
		if (jobStatusListeners.size() > 0) {
			final long timestamp = System.currentTimeMillis();
			final Throwable serializedError = error == null ? null : new SerializedThrowable(error);

			for (JobStatusListener listener : jobStatusListeners) {
				try {
					listener.jobStatusChanges(getJobID(), newState, timestamp, serializedError);
				} catch (Throwable t) {
					LOG.warn("Error while notifying JobStatusListener", t);
				}
			}
		}
	}

	void notifyExecutionChange(
			final Execution execution,
			final ExecutionState newExecutionState,
			final Throwable error) {

		if (!isLegacyScheduling()) {
			return;
		}

		// see what this means for us. currently, the first FAILED state means -> FAILED
		if (newExecutionState == ExecutionState.FAILED) {
			final Throwable ex = error != null ? error : new FlinkException("Unknown Error (missing cause)");

			// by filtering out late failure calls, we can save some work in
			// avoiding redundant local failover
			if (execution.getGlobalModVersion() == globalModVersion) {
				try {
					failoverStrategy.onTaskFailure(execution, ex);
				}
				catch (Throwable t) {
					// bug in the failover strategy - fall back to global failover
					LOG.warn("Error in failover strategy - falling back to global restart", t);
					failGlobal(ex);
				}
			}
		}
	}

	void assertRunningInJobMasterMainThread() {
		if (!(jobMasterMainThreadExecutor instanceof ComponentMainThreadExecutor.DummyComponentMainThreadExecutor)) {
			jobMasterMainThreadExecutor.assertRunningInMainThread();
		}
	}

	void notifySchedulerNgAboutInternalTaskFailure(final ExecutionAttemptID attemptId, final Throwable t) {
		if (internalTaskFailuresListener != null) {
			internalTaskFailuresListener.notifyTaskFailure(attemptId, t);
		}
	}

	ShuffleMaster<?> getShuffleMaster() {
		return shuffleMaster;
	}

	public JobMasterPartitionTracker getPartitionTracker() {
		return partitionTracker;
	}

	public ResultPartitionAvailabilityChecker getResultPartitionAvailabilityChecker() {
		return resultPartitionAvailabilityChecker;
	}

	PartitionReleaseStrategy getPartitionReleaseStrategy() {
		return partitionReleaseStrategy;
	}
}
