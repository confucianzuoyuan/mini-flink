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

package org.apache.flink.runtime.taskmanager;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.accumulators.AccumulatorRegistry;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.deployment.InputGateDeploymentDescriptor;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskInformation;
import org.apache.flink.runtime.filecache.FileCache;
import org.apache.flink.runtime.io.network.TaskEventDispatcher;
import org.apache.flink.runtime.io.network.api.writer.ResultPartitionWriter;
import org.apache.flink.runtime.io.network.partition.PartitionProducerStateProvider;
import org.apache.flink.runtime.io.network.partition.ResultPartitionConsumableNotifier;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.InputSplitProvider;
import org.apache.flink.runtime.jobgraph.tasks.TaskOperatorEventGateway;
import org.apache.flink.runtime.memory.MemoryManager;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleIOOwnerContext;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.taskexecutor.BackPressureSampleableTask;
import org.apache.flink.runtime.taskexecutor.KvStateService;
import org.apache.flink.runtime.taskexecutor.PartitionProducerStateChecker;
import org.apache.flink.runtime.taskexecutor.slot.TaskSlotPayload;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class Task implements Runnable, TaskSlotPayload, TaskActions, PartitionProducerStateProvider, BackPressureSampleableTask {

	/** The class logger. */
	private static final Logger LOG = LoggerFactory.getLogger(Task.class);

	/** The thread group that contains all task threads. */
	private static final ThreadGroup TASK_THREADS_GROUP = new ThreadGroup("Flink Task Threads");

	/** For atomic state updates. */
	private static final AtomicReferenceFieldUpdater<Task, ExecutionState> STATE_UPDATER =
			AtomicReferenceFieldUpdater.newUpdater(Task.class, ExecutionState.class, "executionState");

	// ------------------------------------------------------------------------
	//  Constant fields that are part of the initial Task construction
	// ------------------------------------------------------------------------

	/** The job that the task belongs to. */
	private final JobID jobId;

	/** The vertex in the JobGraph whose code the task executes. */
	private final JobVertexID vertexId;

	/** The execution attempt of the parallel subtask. */
	private final ExecutionAttemptID executionId;

	/** ID which identifies the slot in which the task is supposed to run. */
	private final AllocationID allocationId;

	/** TaskInfo object for this task. */
	private final TaskInfo taskInfo;

	/** The name of the task, including subtask indexes. */
	private final String taskNameWithSubtask;

	/** The job-wide configuration object. */
	private final Configuration jobConfiguration;

	/** The task-specific configuration. */
	private final Configuration taskConfiguration;

	/** The jar files used by this task. */
	private final Collection<PermanentBlobKey> requiredJarFiles;

	/** The classpaths used by this task. */
	private final Collection<URL> requiredClasspaths;

	/** The name of the class that holds the invokable code. */
	private final String nameOfInvokableClass;

	/** Access to task manager configuration and host names. */
	private final TaskManagerRuntimeInfo taskManagerConfig;

	/** The memory manager to be used by this task. */
	private final MemoryManager memoryManager;

	private final TaskEventDispatcher taskEventDispatcher;

	/** The manager for state of operators running in this task/slot. */
	private final TaskStateManager taskStateManager;

	/** Serialized version of the job specific execution configuration (see {@link ExecutionConfig}). */
	private final SerializedValue<ExecutionConfig> serializedExecutionConfig;

	private final ResultPartitionWriter[] consumableNotifyingPartitionWriters;

	private final IndexedInputGate[] inputGates;

	/** Connection to the task manager. */
	private final TaskManagerActions taskManagerActions;

	/** Input split provider for the task. */
	private final InputSplitProvider inputSplitProvider;

	/** The gateway for operators to send messages to the operator coordinators on the Job Manager. */
	private final TaskOperatorEventGateway operatorCoordinatorEventGateway;

	/** The library cache, from which the task can request its class loader. */
	private final LibraryCacheManager.ClassLoaderHandle classLoaderHandle;

	/** The cache for user-defined files that the invokable requires. */
	private final FileCache fileCache;

	/** The service for kvState registration of this task. */
	private final KvStateService kvStateService;

	/** The registry of this task which enables live reporting of accumulators. */
	private final AccumulatorRegistry accumulatorRegistry;

	/** The thread that executes the task. */
	private final Thread executingThread;

	/** Partition producer state checker to request partition states from. */
	private final PartitionProducerStateChecker partitionProducerStateChecker;

	/** Executor to run future callbacks. */
	private final Executor executor;

	/** Future that is completed once {@link #run()} exits. */
	private final CompletableFuture<ExecutionState> terminationFuture = new CompletableFuture<>();

	// ------------------------------------------------------------------------
	//  Fields that control the task execution. All these fields are volatile
	//  (which means that they introduce memory barriers), to establish
	//  proper happens-before semantics on parallel modification
	// ------------------------------------------------------------------------

	/** atomic flag that makes sure the invokable is canceled exactly once upon error. */
	private final AtomicBoolean invokableHasBeenCanceled;

	/** The invokable of this task, if initialized. All accesses must copy the reference and
	 * check for null, as this field is cleared as part of the disposal logic. */
	@Nullable
	private volatile AbstractInvokable invokable;

	/** The current execution state of the task. */
	private volatile ExecutionState executionState = ExecutionState.CREATED;

	/** The observed exception, in case the task execution failed. */
	private volatile Throwable failureCause;

	/** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
	private long taskCancellationInterval;

	/** Initialized from the Flink configuration. May also be set at the ExecutionConfig */
	private long taskCancellationTimeout;

	/** This class loader should be set as the context class loader for threads that may dynamically load user code. */
	private ClassLoader userCodeClassLoader;

	/**
	 * <p><b>IMPORTANT:</b> This constructor may not start any work that would need to
	 * be undone in the case of a failing task deployment.</p>
	 */
	public Task(
		JobInformation jobInformation,
		TaskInformation taskInformation,
		ExecutionAttemptID executionAttemptID,
		AllocationID slotAllocationId,
		int subtaskIndex,
		int attemptNumber,
		List<ResultPartitionDeploymentDescriptor> resultPartitionDeploymentDescriptors,
		List<InputGateDeploymentDescriptor> inputGateDeploymentDescriptors,
		int targetSlotNumber,
		MemoryManager memManager,
		ShuffleEnvironment<?, ?> shuffleEnvironment,
		KvStateService kvStateService,
		TaskEventDispatcher taskEventDispatcher,
		TaskStateManager taskStateManager,
		TaskManagerActions taskManagerActions,
		InputSplitProvider inputSplitProvider,
		TaskOperatorEventGateway operatorCoordinatorEventGateway,
		LibraryCacheManager.ClassLoaderHandle classLoaderHandle,
		FileCache fileCache,
		TaskManagerRuntimeInfo taskManagerConfig,
		ResultPartitionConsumableNotifier resultPartitionConsumableNotifier,
		PartitionProducerStateChecker partitionProducerStateChecker,
		Executor executor) {

		this.taskInfo = new TaskInfo(
				taskInformation.getTaskName(),
				taskInformation.getMaxNumberOfSubtasks(),
				subtaskIndex,
				taskInformation.getNumberOfSubtasks(),
				attemptNumber,
				String.valueOf(slotAllocationId));

		this.jobId = jobInformation.getJobId();
		this.vertexId = taskInformation.getJobVertexId();
		this.executionId  = Preconditions.checkNotNull(executionAttemptID);
		this.allocationId = Preconditions.checkNotNull(slotAllocationId);
		this.taskNameWithSubtask = taskInfo.getTaskNameWithSubtasks();
		this.jobConfiguration = jobInformation.getJobConfiguration();
		this.taskConfiguration = taskInformation.getTaskConfiguration();
		this.requiredJarFiles = jobInformation.getRequiredJarFileBlobKeys();
		this.requiredClasspaths = jobInformation.getRequiredClasspathURLs();
		this.nameOfInvokableClass = taskInformation.getInvokableClassName();
		this.serializedExecutionConfig = jobInformation.getSerializedExecutionConfig();

		Configuration tmConfig = taskManagerConfig.getConfiguration();
		this.taskCancellationInterval = tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_INTERVAL);
		this.taskCancellationTimeout = tmConfig.getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT);

		this.memoryManager = Preconditions.checkNotNull(memManager);
		this.taskEventDispatcher = Preconditions.checkNotNull(taskEventDispatcher);
		this.taskStateManager = Preconditions.checkNotNull(taskStateManager);
		this.accumulatorRegistry = new AccumulatorRegistry(jobId, executionId);

		this.inputSplitProvider = Preconditions.checkNotNull(inputSplitProvider);
		this.operatorCoordinatorEventGateway = Preconditions.checkNotNull(operatorCoordinatorEventGateway);
		this.taskManagerActions = checkNotNull(taskManagerActions);

		this.classLoaderHandle = Preconditions.checkNotNull(classLoaderHandle);
		this.fileCache = Preconditions.checkNotNull(fileCache);
		this.kvStateService = Preconditions.checkNotNull(kvStateService);
		this.taskManagerConfig = Preconditions.checkNotNull(taskManagerConfig);

		this.partitionProducerStateChecker = Preconditions.checkNotNull(partitionProducerStateChecker);
		this.executor = Preconditions.checkNotNull(executor);

		// create the reader and writer structures

		final String taskNameWithSubtaskAndId = taskNameWithSubtask + " (" + executionId + ')';

		final ShuffleIOOwnerContext taskShuffleContext = shuffleEnvironment
			.createShuffleIOOwnerContext(taskNameWithSubtaskAndId, executionId);

		// produced intermediate result partitions
		final ResultPartitionWriter[] resultPartitionWriters = shuffleEnvironment.createResultPartitionWriters(
			taskShuffleContext,
			resultPartitionDeploymentDescriptors).toArray(new ResultPartitionWriter[] {});

		this.consumableNotifyingPartitionWriters = ConsumableNotifyingResultPartitionWriterDecorator.decorate(
			resultPartitionDeploymentDescriptors,
			resultPartitionWriters,
			this,
			jobId,
			resultPartitionConsumableNotifier);

		// consumed intermediate result partitions
		final IndexedInputGate[] gates = shuffleEnvironment.createInputGates(
				taskShuffleContext,
				this,
				inputGateDeploymentDescriptors)
			.toArray(new IndexedInputGate[0]);

		this.inputGates = new IndexedInputGate[gates.length];
		int counter = 0;
		for (IndexedInputGate gate : gates) {
			inputGates[counter++] = new InputGateWithMetrics(gate);
		}

		invokableHasBeenCanceled = new AtomicBoolean(false);

		// finally, create the executing thread, but do not start it
		// 当执行executingThread.start()时，
		// 线程中执行的是this，也就是当前实例
		// 而当前实例实现了Runnable，所以直接调用run()
		executingThread = new Thread(TASK_THREADS_GROUP, this, taskNameWithSubtask);
	}

	// ------------------------------------------------------------------------
	//  Accessors
	// ------------------------------------------------------------------------

	@Override
	public JobID getJobID() {
		return jobId;
	}

	public JobVertexID getJobVertexId() {
		return vertexId;
	}

	@Override
	public ExecutionAttemptID getExecutionId() {
		return executionId;
	}

	@Override
	public AllocationID getAllocationId() {
		return allocationId;
	}

	public TaskInfo getTaskInfo() {
		return taskInfo;
	}

	public Configuration getJobConfiguration() {
		return jobConfiguration;
	}

	public Configuration getTaskConfiguration() {
		return this.taskConfiguration;
	}

	public AccumulatorRegistry getAccumulatorRegistry() {
		return accumulatorRegistry;
	}

	public Thread getExecutingThread() {
		return executingThread;
	}

	@Override
	public CompletableFuture<ExecutionState> getTerminationFuture() {
		return terminationFuture;
	}

	@Override
	public boolean isBackPressured() {
		if (invokable == null || consumableNotifyingPartitionWriters.length == 0 || !isRunning()) {
			return false;
		}
		final CompletableFuture<?>[] outputFutures = new CompletableFuture[consumableNotifyingPartitionWriters.length];
		for (int i = 0; i < outputFutures.length; ++i) {
			outputFutures[i] = consumableNotifyingPartitionWriters[i].getAvailableFuture();
		}
		return !CompletableFuture.allOf(outputFutures).isDone();
	}

	// ------------------------------------------------------------------------
	//  Task Execution
	// ------------------------------------------------------------------------

	/**
	 * Returns the current execution state of the task.
	 * @return The current execution state of the task.
	 */
	public ExecutionState getExecutionState() {
		return this.executionState;
	}

	/**
	 * Checks whether the task has failed, is canceled, or is being canceled at the moment.
	 * @return True is the task in state FAILED, CANCELING, or CANCELED, false otherwise.
	 */
	public boolean isCanceledOrFailed() {
		return executionState == ExecutionState.CANCELING ||
				executionState == ExecutionState.CANCELED ||
				executionState == ExecutionState.FAILED;
	}

	@Override
	public boolean isRunning() {
		return executionState == ExecutionState.RUNNING;
	}

	/**
	 * If the task has failed, this method gets the exception that caused this task to fail.
	 * Otherwise this method returns null.
	 *
	 * @return The exception that caused the task to fail, or null, if the task has not failed.
	 */
	public Throwable getFailureCause() {
		return failureCause;
	}

	/**
	 * Starts the task's thread.
	 */
	public void startTaskThread() {
		executingThread.start();
	}

	/**
	 * The core work method that bootstraps the task and executes its code.
	 */
	@Override
	public void run() {
		try {
			doRun();
		} finally {
			terminationFuture.complete(executionState);
		}
	}

	private void doRun() {
		// ----------------------------
		//  Initial State transition
		// ----------------------------
		while (true) {
			ExecutionState current = this.executionState;
			if (current == ExecutionState.CREATED) {
				if (transitionState(ExecutionState.CREATED, ExecutionState.DEPLOYING)) {
					// success, we can start our work
					break;
				}
			}
			else {
				throw new IllegalStateException("Invalid state for beginning of operation of task " + this + '.');
			}
		}

		// all resource acquisitions and registrations from here on
		// need to be undone in the end
		Map<String, Future<Path>> distributedCacheEntries = new HashMap<>();
		AbstractInvokable invokable = null;

		try {
			// ----------------------------
			//  Task Bootstrap - We periodically
			//  check for canceling as a shortcut
			// ----------------------------

			// first of all, get a user-code classloader
			// this may involve downloading the job's JAR files and/or classes
			LOG.info("Loading JAR files for task {}.", this);

			userCodeClassLoader = createUserCodeClassloader();
			final ExecutionConfig executionConfig = serializedExecutionConfig.deserializeValue(userCodeClassLoader);

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			// register the task with the network stack
			// this operation may fail if the system does not have enough
			// memory to run the necessary data exchanges
			// the registration must also strictly be undone
			// ----------------------------------------------------------------

			LOG.info("Registering task at network: {}.", this);

			setupPartitionsAndGates(consumableNotifyingPartitionWriters, inputGates);

			for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
				taskEventDispatcher.registerPartition(partitionWriter.getPartitionId());
			}

			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  call the user code initialization methods
			// ----------------------------------------------------------------

			TaskKvStateRegistry kvStateRegistry = kvStateService.createKvStateTaskRegistry(jobId, getJobVertexId());

			Environment env = new RuntimeEnvironment(
				jobId,
				vertexId,
				executionId,
				executionConfig,
				taskInfo,
				jobConfiguration,
				taskConfiguration,
				userCodeClassLoader,
				memoryManager,
				taskStateManager,
				accumulatorRegistry,
				kvStateRegistry,
				inputSplitProvider,
				distributedCacheEntries,
				consumableNotifyingPartitionWriters,
				inputGates,
				taskEventDispatcher,
				operatorCoordinatorEventGateway,
				taskManagerConfig,
				this);

			// Make sure the user code classloader is accessible thread-locally.
			// We are setting the correct context class loader before instantiating the invokable
			// so that it is available to the invokable during its entire lifetime.
			executingThread.setContextClassLoader(userCodeClassLoader);

			// now load and instantiate the task's invokable code
			invokable = loadAndInstantiateInvokable(userCodeClassLoader, nameOfInvokableClass, env);

			// ----------------------------------------------------------------
			//  actual task core work
			// ----------------------------------------------------------------

			// we must make strictly sure that the invokable is accessible to the cancel() call
			// by the time we switched to running.
			this.invokable = invokable;

			// switch to the RUNNING state, if that fails, we have been canceled/failed in the meantime
			if (!transitionState(ExecutionState.DEPLOYING, ExecutionState.RUNNING)) {
				throw new CancelTaskException();
			}

			// notify everyone that we switched to running
			taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, ExecutionState.RUNNING));

			// make sure the user code classloader is accessible thread-locally
			executingThread.setContextClassLoader(userCodeClassLoader);

			invokable.invoke();

			// make sure, we enter the catch block if the task leaves the invoke() method due
			// to the fact that it has been canceled
			if (isCanceledOrFailed()) {
				throw new CancelTaskException();
			}

			// ----------------------------------------------------------------
			//  finalization of a successful execution
			// ----------------------------------------------------------------

			// finish the produced partitions. if this fails, we consider the execution failed.
			for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
				if (partitionWriter != null) {
					partitionWriter.finish();
				}
			}

			// try to mark the task as finished
			// if that fails, the task was canceled/failed in the meantime
			if (!transitionState(ExecutionState.RUNNING, ExecutionState.FINISHED)) {
				throw new CancelTaskException();
			}
		}
		catch (Throwable t) {
		}
		finally {
			try {
				LOG.info("Freeing task resources for {} ({}).", taskNameWithSubtask, executionId);

				// clear the reference to the invokable. this helps guard against holding references
				// to the invokable and its structures in cases where this Task object is still referenced
				this.invokable = null;

				// free the network resources
				releaseResources();

				// free memory resources
				if (invokable != null) {
					memoryManager.releaseAll(invokable);
				}

				// remove all of the tasks resources
				fileCache.releaseJob(jobId, executionId);

				notifyFinalState();
			}
			catch (Throwable t) {
				// an error in the resource cleanup is fatal
				String message = String.format("FATAL - exception in resource cleanup of task %s (%s).", taskNameWithSubtask, executionId);
				LOG.error(message, t);
				notifyFatalError(message, t);
			}


		}
	}

	@VisibleForTesting
	public static void setupPartitionsAndGates(
		ResultPartitionWriter[] producedPartitions, InputGate[] inputGates) throws IOException {

		for (ResultPartitionWriter partition : producedPartitions) {
			partition.setup();
		}

		// InputGates must be initialized after the partitions, since during InputGate#setup
		// we are requesting partitions
		for (InputGate gate : inputGates) {
			gate.setup();
		}
	}

	/**
	 * Releases resources before task exits. We should also fail the partition to release if the task
	 * has failed, is canceled, or is being canceled at the moment.
	 */
	private void releaseResources() {
		LOG.debug("Release task {} network resources (state: {}).", taskNameWithSubtask, getExecutionState());

		for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
			taskEventDispatcher.unregisterPartition(partitionWriter.getPartitionId());
			if (isCanceledOrFailed()) {
				partitionWriter.fail(getFailureCause());
			}
		}

		closeNetworkResources();
		try {
			taskStateManager.close();
		} catch (Exception e) {
			LOG.error("Failed to close task state manager for task {}.", taskNameWithSubtask, e);
		}
	}

	private void closeNetworkResources() {
		for (ResultPartitionWriter partitionWriter : consumableNotifyingPartitionWriters) {
			try {
				partitionWriter.close();
			} catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalError(t);
				LOG.error("Failed to release result partition for task {}.", taskNameWithSubtask, t);
			}
		}

		for (InputGate inputGate : inputGates) {
			try {
				inputGate.close();
			} catch (Throwable t) {
				ExceptionUtils.rethrowIfFatalError(t);
				LOG.error("Failed to release input gate for task {}.", taskNameWithSubtask, t);
			}
		}
	}

	private ClassLoader createUserCodeClassloader() throws Exception {
		long startDownloadTime = System.currentTimeMillis();

		// triggers the download of all missing jar files from the job manager
		final ClassLoader userCodeClassLoader = classLoaderHandle.getOrResolveClassLoader(requiredJarFiles, requiredClasspaths);

		LOG.debug("Getting user code class loader for task {} at library cache manager took {} milliseconds",
				executionId, System.currentTimeMillis() - startDownloadTime);

		return userCodeClassLoader;
	}

	private void notifyFinalState() {
		checkState(executionState.isTerminal());
		taskManagerActions.updateTaskExecutionState(new TaskExecutionState(jobId, executionId, executionState, failureCause));
	}

	private void notifyFatalError(String message, Throwable cause) {
		taskManagerActions.notifyFatalError(message, cause);
	}

	private boolean transitionState(ExecutionState currentState, ExecutionState newState) {
		return transitionState(currentState, newState, null);
	}

	private boolean transitionState(ExecutionState currentState, ExecutionState newState, Throwable cause) {
		if (STATE_UPDATER.compareAndSet(this, currentState, newState)) {
			return true;
		} else {
			return false;
		}
	}

	@Override
	public void failExternally(Throwable cause) {
	}

	public void deliverOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> evt) throws FlinkException {
	}

	@Override
	public String toString() {
		return String.format("%s (%s) [%s]", taskNameWithSubtask, executionId, executionState);
	}

	private static AbstractInvokable loadAndInstantiateInvokable(
		ClassLoader classLoader,
		String className,
		Environment environment) throws Throwable {

		final Class<? extends AbstractInvokable> invokableClass;
		try {
			invokableClass = Class.forName(className, true, classLoader)
				.asSubclass(AbstractInvokable.class);
		} catch (Throwable t) {
			throw new Exception("Could not load the task's invokable class.", t);
		}

		Constructor<? extends AbstractInvokable> statelessCtor;

		try {
			statelessCtor = invokableClass.getConstructor(Environment.class);
		} catch (NoSuchMethodException ee) {
			throw new FlinkException("Task misses proper constructor", ee);
		}

		// instantiate the class
		try {
			//noinspection ConstantConditions  --> cannot happen
			return statelessCtor.newInstance(environment);
		} catch (InvocationTargetException e) {
			// directly forward exceptions from the eager initialization
			throw e.getTargetException();
		} catch (Exception e) {
			throw new FlinkException("Could not instantiate the task's invokable class.", e);
		}
	}

}
