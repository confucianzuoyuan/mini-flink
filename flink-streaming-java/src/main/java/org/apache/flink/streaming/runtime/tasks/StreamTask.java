package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateReader;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.network.api.writer.*;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendLoader;
import org.apache.flink.runtime.taskmanager.DispatcherThreadFactory;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.runtime.util.FatalExitExceptionHandler;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamEdge;
import org.apache.flink.streaming.api.operators.MailboxExecutor;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializerImpl;
import org.apache.flink.streaming.runtime.io.StreamInputProcessor;
import org.apache.flink.streaming.runtime.partitioner.ConfigurableStreamPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.streamstatus.StreamStatusMaintainer;
import org.apache.flink.streaming.runtime.tasks.mailbox.*;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * Base class for all streaming tasks. A task is the unit of local processing that is deployed
 * and executed by the TaskManagers. Each task runs one or more {@link StreamOperator}s which form
 * the Task's operator chain. Operators that are chained together execute synchronously in the
 * same thread and hence on the same stream partition. A common case for these chains
 * are successive map/flatmap/filter tasks.
 *
 * <p>The task chain contains one "head" operator and multiple chained operators.
 * The StreamTask is specialized for the type of the head operator: one-input and two-input tasks,
 * as well as for sources, iteration heads and iteration tails.
 *
 * <p>The Task class deals with the setup of the streams read by the head operator, and the streams
 * produced by the operators at the ends of the operator chain. Note that the chain may fork and
 * thus have multiple ends.
 *
 * <p>The life cycle of the task is set up as follows:
 * <pre>{@code
 *  -- setInitialState -> provides state of all operators in the chain
 *
 *  -- invoke()
 *        |
 *        +----> Create basic utils (config, etc) and load the chain of operators
 *        +----> operators.setup()
 *        +----> task specific init()
 *        +----> initialize-operator-states()
 *        +----> open-operators()
 *        +----> run()
 *        +----> close-operators()
 *        +----> dispose-operators()
 *        +----> common cleanup
 *        +----> task specific cleanup()
 * }</pre>
 *
 * <p>The {@code StreamTask} has a lock object called {@code lock}. All calls to methods on a
 * {@code StreamOperator} must be synchronized on this lock object to ensure that no methods
 * are called concurrently.
 *
 * @param <OUT>
 * @param <OP>
 */
@Internal
public abstract class StreamTask<OUT, OP extends StreamOperator<OUT>>
		extends AbstractInvokable {

	/** The thread group that holds all trigger timer threads. */
	public static final ThreadGroup TRIGGER_THREAD_GROUP = new ThreadGroup("Triggers");

	/** The logger used by the StreamTask and its subclasses. */
	protected static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

	// ------------------------------------------------------------------------

	/**
	 * All actions outside of the task {@link #mailboxProcessor mailbox} (i.e. performed by another thread) must be executed through this executor
	 * to ensure that we don't have concurrent method calls that void consistent checkpoints.
	 * <p>CheckpointLock is superseded by {@link MailboxExecutor}, with
	 * {@link StreamTaskActionExecutor.SynchronizedStreamTaskActionExecutor SynchronizedStreamTaskActionExecutor}
	 * to provide lock to {@link SourceStreamTask}. </p>
	 */
	private final StreamTaskActionExecutor actionExecutor;

	/**
	 * The input processor. Initialized in {@link #init()} method.
	 */
	@Nullable
	protected StreamInputProcessor inputProcessor;

	/** the head operator that consumes the input streams of this task. */
	protected OP headOperator;

	/** The chain of operators executed by this task. */
	protected OperatorChain<OUT, OP> operatorChain;

	/** The configuration of this streaming task. */
	protected final StreamConfig configuration;

	/** Our state backend. We use this to create checkpoint streams and a keyed state backend. */
	protected final StateBackend stateBackend;

	/**
	 * The internal {@link TimerService} used to define the current
	 * processing time (default = {@code System.currentTimeMillis()}) and
	 * register timers for tasks to be executed in the future.
	 */
	protected final TimerService timerService;

	/** The currently active background materialization threads. */
	private final CloseableRegistry cancelables = new CloseableRegistry();

	private final StreamTaskAsyncExceptionHandler asyncExceptionHandler;

	/**
	 * Flag to mark the task "in operation", in which case check needs to be initialized to true,
	 * so that early cancel() before invoke() behaves correctly.
	 */
	private volatile boolean isRunning;

	/** Flag to mark this task as canceled. */
	private volatile boolean canceled;

	private boolean disposedOperators;

	/** Thread pool for async snapshot workers. */
	private final ExecutorService asyncOperationsThreadPool;

	private final RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> recordWriter;

	protected final MailboxProcessor mailboxProcessor;

	final MailboxExecutor mainMailboxExecutor;

	/**
	 * TODO it might be replaced by the global IO executor on TaskManager level future.
	 */
	private final ExecutorService channelIOExecutor;

	// ------------------------------------------------------------------------

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 */
	protected StreamTask(Environment env) throws Exception {
		this(env, null);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * @param env The task environment for this task.
	 * @param timerService Optionally, a specific timer service to use.
	 */
	protected StreamTask(Environment env, @Nullable TimerService timerService) throws Exception {
		this(env, timerService, FatalExitExceptionHandler.INSTANCE);
	}

	protected StreamTask(
			Environment environment,
			@Nullable TimerService timerService,
			Thread.UncaughtExceptionHandler uncaughtExceptionHandler) throws Exception {
		this(environment, timerService, uncaughtExceptionHandler, StreamTaskActionExecutor.IMMEDIATE);
	}

	/**
	 * Constructor for initialization, possibly with initial state (recovery / savepoint / etc).
	 *
	 * <p>This constructor accepts a special {@link TimerService}. By default (and if
	 * null is passes for the timer service) a {@link SystemProcessingTimeService DefaultTimerService}
	 * will be used.
	 *
	 * @param environment The task environment for this task.
	 * @param timerService Optionally, a specific timer service to use.
	 * @param uncaughtExceptionHandler to handle uncaught exceptions in the async operations thread pool
	 * @param actionExecutor a mean to wrap all actions performed by this task thread. Currently, only SynchronizedActionExecutor can be used to preserve locking semantics.
	 */
	protected StreamTask(
			Environment environment,
			@Nullable TimerService timerService,
			Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
			StreamTaskActionExecutor actionExecutor) throws Exception {
		this(environment, timerService, uncaughtExceptionHandler, actionExecutor, new TaskMailboxImpl(Thread.currentThread()));
	}

	protected StreamTask(
			Environment environment,
			@Nullable TimerService timerService,
			Thread.UncaughtExceptionHandler uncaughtExceptionHandler,
			StreamTaskActionExecutor actionExecutor,
			TaskMailbox mailbox) throws Exception {

		super(environment);

		this.configuration = new StreamConfig(getTaskConfiguration());
		this.recordWriter = createRecordWriterDelegate(configuration, environment);
		this.actionExecutor = Preconditions.checkNotNull(actionExecutor);
		this.mailboxProcessor = new MailboxProcessor(this::processInput, mailbox, actionExecutor);
		this.mainMailboxExecutor = mailboxProcessor.getMainMailboxExecutor();
		this.asyncExceptionHandler = new StreamTaskAsyncExceptionHandler(environment);
		this.asyncOperationsThreadPool = Executors.newCachedThreadPool(
			new ExecutorThreadFactory("AsyncOperations", uncaughtExceptionHandler));

		this.stateBackend = createStateBackend();

		// if the clock is not already set, then assign a default TimeServiceProvider
		if (timerService == null) {
			ThreadFactory timerThreadFactory = new DispatcherThreadFactory(TRIGGER_THREAD_GROUP, "Time Trigger for " + getName());
			this.timerService = new SystemProcessingTimeService(this::handleTimerException, timerThreadFactory);
		} else {
			this.timerService = timerService;
		}

		this.channelIOExecutor = Executors.newSingleThreadExecutor(new ExecutorThreadFactory("channel-state-unspilling"));
	}

	// ------------------------------------------------------------------------
	//  Life cycle methods for specific implementations
	// ------------------------------------------------------------------------

	protected abstract void init() throws Exception;

	protected void cancelTask() throws Exception {
	}

	protected void cleanup() throws Exception {
		if (inputProcessor != null) {
			inputProcessor.close();
		}
	}

	protected void processInput(MailboxDefaultAction.Controller controller) throws Exception {
		InputStatus status = inputProcessor.processInput();
		if (status == InputStatus.MORE_AVAILABLE && recordWriter.isAvailable()) {
			return;
		}
		if (status == InputStatus.END_OF_INPUT) {
			controller.allActionsCompleted();
			return;
		}
		CompletableFuture<?> jointFuture = getInputOutputJointFuture(status);
		MailboxDefaultAction.Suspension suspendedDefaultAction = controller.suspendDefaultAction();
		jointFuture.thenRun(suspendedDefaultAction::resume);
	}

	@VisibleForTesting
	CompletableFuture<?> getInputOutputJointFuture(InputStatus status) {
		if (status == InputStatus.NOTHING_AVAILABLE && !recordWriter.isAvailable()) {
			return CompletableFuture.allOf(inputProcessor.getAvailableFuture(), recordWriter.getAvailableFuture());
		} else if (status == InputStatus.NOTHING_AVAILABLE) {
			return inputProcessor.getAvailableFuture();
		} else {
			return recordWriter.getAvailableFuture();
		}
	}

	// ------------------------------------------------------------------------
	//  Core work methods of the Stream Task
	// ------------------------------------------------------------------------

	public StreamTaskStateInitializer createStreamTaskStateInitializer() {
		return new StreamTaskStateInitializerImpl(
			getEnvironment(),
			stateBackend);
	}

	protected void beforeInvoke() throws Exception {
		disposedOperators = false;
		LOG.debug("Initializing {}.", getName());

		operatorChain = new OperatorChain<>(this, recordWriter);
		headOperator = operatorChain.getHeadOperator();

		// task specific initialization
		init();

		// save the work of reloading state, etc, if the task is already canceled
		if (canceled) {
			throw new CancelTaskException();
		}

		// -------- Invoke --------
		LOG.debug("Invoking {}", getName());

		// we need to make sure that any triggers scheduled in open() cannot be
		// executed before all operators are opened
		actionExecutor.runThrowing(() -> {
			// both the following operations are protected by the lock
			// so that we avoid race conditions in the case that initializeState()
			// registers a timer, that fires before the open() is called.
			operatorChain.initializeStateAndOpenOperators(createStreamTaskStateInitializer());

			readRecoveredChannelState();
		});

		isRunning = true;
	}

	private void readRecoveredChannelState() throws IOException, InterruptedException {
		ChannelStateReader reader = getEnvironment().getTaskStateManager().getChannelStateReader();
		if (!reader.hasChannelStates()) {
			requestPartitions();
		}
	}

	private void requestPartitions() throws IOException {
		InputGate[] inputGates = getEnvironment().getAllInputGates();
		if (inputGates != null) {
			for (InputGate inputGate : inputGates) {
				inputGate.requestPartitions();
			}
		}
	}

	@Override
	public final void invoke() throws Exception {
		try {
			beforeInvoke();

			// final check to exit early before starting to run
			if (canceled) {
				throw new CancelTaskException();
			}

			// let the task do its work
			runMailboxLoop();

			// if this left the run() method cleanly despite the fact that this was canceled,
			// make sure the "clean shutdown" is not attempted
			if (canceled) {
				throw new CancelTaskException();
			}

			afterInvoke();
		}
		catch (Exception invokeException) {
			try {
				cleanUpInvoke();
			}
			catch (Throwable cleanUpException) {
				throw (Exception) ExceptionUtils.firstOrSuppressed(cleanUpException, invokeException);
			}
			throw invokeException;
		}
		cleanUpInvoke();
	}

	private void runMailboxLoop() throws Exception {
		mailboxProcessor.runMailboxLoop();
	}

	protected void afterInvoke() throws Exception {
		LOG.debug("Finished task {}", getName());

		final CompletableFuture<Void> timersFinishedFuture = new CompletableFuture<>();

		// close all operators in a chain effect way
		operatorChain.closeOperators(actionExecutor);

		// make sure no further checkpoint and notification actions happen.
		// at the same time, this makes sure that during any "regular" exit where still
		actionExecutor.runThrowing(() -> {

			// make sure no new timers can come
			FutureUtils.forward(timerService.quiesce(), timersFinishedFuture);

			// let mailbox execution reject all new letters from this point
			mailboxProcessor.prepareClose();

			// only set the StreamTask to not running after all operators have been closed!
			// See FLINK-7430
			isRunning = false;
		});
		// processes the remaining mails; no new mails can be enqueued
		mailboxProcessor.drain();

		// make sure all timers finish
		timersFinishedFuture.get();

		LOG.debug("Closed operators for task {}", getName());

		// make sure all buffered data is flushed
		operatorChain.flushOutputs();

		// make an attempt to dispose the operators such that failures in the dispose call
		// still let the computation fail
		disposeAllOperators(false);
		disposedOperators = true;
	}

	protected void cleanUpInvoke() throws Exception {
		// clean up everything we initialized
		isRunning = false;

		// Now that we are outside the user code, we do not want to be interrupted further
		// upon cancellation. The shutdown logic below needs to make sure it does not issue calls
		// that block and stall shutdown.
		// Additionally, the cancellation watch dog will issue a hard-cancel (kill the TaskManager
		// process) as a backup in case some shutdown procedure blocks outside our control.
		setShouldInterruptOnCancel(false);

		// clear any previously issued interrupt for a more graceful shutdown
		Thread.interrupted();

		// stop all timers and threads
		tryShutdownTimerService();

		// stop all asynchronous checkpoint threads
		try {
			cancelables.close();
			shutdownAsyncThreads();
		} catch (Throwable t) {
			// catch and log the exception to not replace the original exception
			LOG.error("Could not shut down async checkpoint threads", t);
		}

		// we must! perform this cleanup
		try {
			cleanup();
		} catch (Throwable t) {
			// catch and log the exception to not replace the original exception
			LOG.error("Error during cleanup of stream task", t);
		}

		// if the operators were not disposed before, do a hard dispose
		disposeAllOperators(true);

		// release the output resources. this method should never fail.
		if (operatorChain != null) {
			// beware: without synchronization, #performCheckpoint() may run in
			//         parallel and this call is not thread-safe
			actionExecutor.run(() -> operatorChain.releaseOutputs());
		} else {
			// failed to allocate operatorChain, clean up record writers
			recordWriter.close();
		}

		try {
			channelIOExecutor.shutdown();
		} catch (Throwable t) {
			LOG.error("Error during shutdown the channel state unspill executor", t);
		}

		mailboxProcessor.close();
	}

	@Override
	public final void cancel() throws Exception {
		isRunning = false;
		canceled = true;

		// the "cancel task" call must come first, but the cancelables must be
		// closed no matter what
		try {
			cancelTask();
		}
		finally {
			mailboxProcessor.allActionsCompleted();
			cancelables.close();
		}
	}

	public MailboxExecutorFactory getMailboxExecutorFactory() {
		return this.mailboxProcessor::getMailboxExecutor;
	}

	public final boolean isRunning() {
		return isRunning;
	}

	public final boolean isCanceled() {
		return canceled;
	}

	private void shutdownAsyncThreads() throws Exception {
		if (!asyncOperationsThreadPool.isShutdown()) {
			asyncOperationsThreadPool.shutdownNow();
		}
	}

	/**
	 * Execute @link StreamOperator#dispose()} of each operator in the chain of this
	 * {@link StreamTask}. Disposing happens from <b>tail to head</b> operator in the chain.
	 */
	private void disposeAllOperators(boolean logOnlyErrors) throws Exception {
		if (operatorChain != null && !disposedOperators) {
			for (StreamOperatorWrapper<?, ?> operatorWrapper : operatorChain.getAllOperators(true)) {
				StreamOperator<?> operator = operatorWrapper.getStreamOperator();
				if (!logOnlyErrors) {
					operator.dispose();
				}
				else {
					try {
						operator.dispose();
					}
					catch (Exception e) {
						LOG.error("Error during disposal of stream operator.", e);
					}
				}
			}
			disposedOperators = true;
		}
	}

	/**
	 * The finalize method shuts down the timer. This is a fail-safe shutdown, in case the original
	 * shutdown method was never called.
	 *
	 * <p>This should not be relied upon! It will cause shutdown to happen much later than if manual
	 * shutdown is attempted, and cause threads to linger for longer than needed.
	 */
	@Override
	protected void finalize() throws Throwable {
		super.finalize();
		if (!timerService.isTerminated()) {
			LOG.info("Timer service is shutting down.");
			timerService.shutdownService();
		}

		cancelables.close();
	}

	// ------------------------------------------------------------------------
	//  Access to properties and utilities
	// ------------------------------------------------------------------------

	/**
	 * Gets the name of the task, in the form "taskname (2/5)".
	 * @return The name of the task.
	 */
	public final String getName() {
		return getEnvironment().getTaskInfo().getTaskNameWithSubtasks();
	}

	/**
	 * Gets the name of the task, appended with the subtask indicator and execution id.
	 *
	 * @return The name of the task, with subtask indicator and execution id.
	 */
	String getTaskNameWithSubtaskAndId() {
		return getEnvironment().getTaskInfo().getTaskNameWithSubtasks() +
			" (" + getEnvironment().getExecutionId() + ')';
	}

	public StreamConfig getConfiguration() {
		return configuration;
	}

	public StreamStatusMaintainer getStreamStatusMaintainer() {
		return operatorChain;
	}

	private void tryShutdownTimerService() {

		if (!timerService.isTerminated()) {

			try {
				final long timeoutMs = getEnvironment().getTaskManagerInfo().getConfiguration().
					getLong(TaskManagerOptions.TASK_CANCELLATION_TIMEOUT_TIMERS);

				if (!timerService.shutdownServiceUninterruptible(timeoutMs)) {
					LOG.warn("Timer service shutdown exceeded time limit of {} ms while waiting for pending " +
						"timers. Will continue with shutdown procedure.", timeoutMs);
				}
			} catch (Throwable t) {
				// catch and log the exception to not replace the original exception
				LOG.error("Could not shut down timer service", t);
			}
		}
	}

	// ------------------------------------------------------------------------
	//  Operator Events
	// ------------------------------------------------------------------------

	@Override
	public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event) throws FlinkException {
	}

	// ------------------------------------------------------------------------
	//  State backend
	// ------------------------------------------------------------------------

	private StateBackend createStateBackend() throws Exception {
		final StateBackend fromApplication = configuration.getStateBackend(getUserCodeClassLoader());

		return StateBackendLoader.fromApplicationOrConfigOrDefault(
				fromApplication,
				getEnvironment().getTaskManagerInfo().getConfiguration(),
				getUserCodeClassLoader(),
				LOG);
	}

	public ProcessingTimeServiceFactory getProcessingTimeServiceFactory() {
		return mailboxExecutor -> new ProcessingTimeServiceImpl(
			timerService,
			callback -> deferCallbackToMailbox(mailboxExecutor, callback));
	}

	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return getName();
	}

	// ------------------------------------------------------------------------

	/**
	 * Utility class to encapsulate the handling of asynchronous exceptions.
	 */
	static class StreamTaskAsyncExceptionHandler {
		private final Environment environment;

		StreamTaskAsyncExceptionHandler(Environment environment) {
			this.environment = environment;
		}

		void handleAsyncException(String message, Throwable exception) {
			environment.failExternally(new AsynchronousException(message, exception));
		}
	}

	public final CloseableRegistry getCancelables() {
		return cancelables;
	}

	// ------------------------------------------------------------------------

	@VisibleForTesting
	public static <OUT> RecordWriterDelegate<SerializationDelegate<StreamRecord<OUT>>> createRecordWriterDelegate(
			StreamConfig configuration,
			Environment environment) {
		List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWrites = createRecordWriters(
			configuration,
			environment);
		if (recordWrites.size() == 1) {
			return new SingleRecordWriter<>(recordWrites.get(0));
		} else if (recordWrites.size() == 0) {
			return new NonRecordWriter<>();
		} else {
			return new MultipleRecordWriters<>(recordWrites);
		}
	}

	private static <OUT> List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> createRecordWriters(
			StreamConfig configuration,
			Environment environment) {
		List<RecordWriter<SerializationDelegate<StreamRecord<OUT>>>> recordWriters = new ArrayList<>();
		List<StreamEdge> outEdgesInOrder = configuration.getOutEdgesInOrder(environment.getUserClassLoader());
		Map<Integer, StreamConfig> chainedConfigs = configuration.getTransitiveChainedTaskConfigsWithSelf(environment.getUserClassLoader());

		for (int i = 0; i < outEdgesInOrder.size(); i++) {
			StreamEdge edge = outEdgesInOrder.get(i);
			recordWriters.add(
				createRecordWriter(
					edge,
					i,
					environment,
					environment.getTaskInfo().getTaskName(),
					chainedConfigs.get(edge.getSourceId()).getBufferTimeout()));
		}
		return recordWriters;
	}

	private static <OUT> RecordWriter<SerializationDelegate<StreamRecord<OUT>>> createRecordWriter(
			StreamEdge edge,
			int outputIndex,
			Environment environment,
			String taskName,
			long bufferTimeout) {
		@SuppressWarnings("unchecked")
		StreamPartitioner<OUT> outputPartitioner = (StreamPartitioner<OUT>) edge.getPartitioner();

		LOG.debug("Using partitioner {} for output {} of task {}", outputPartitioner, outputIndex, taskName);

		ResultPartitionWriter bufferWriter = environment.getWriter(outputIndex);

		// we initialize the partitioner here with the number of key groups (aka max. parallelism)
		if (outputPartitioner instanceof ConfigurableStreamPartitioner) {
			int numKeyGroups = bufferWriter.getNumTargetKeyGroups();
			if (0 < numKeyGroups) {
				((ConfigurableStreamPartitioner) outputPartitioner).configure(numKeyGroups);
			}
		}

		RecordWriter<SerializationDelegate<StreamRecord<OUT>>> output = new RecordWriterBuilder<SerializationDelegate<StreamRecord<OUT>>>()
			.setChannelSelector(outputPartitioner)
			.setTimeout(bufferTimeout)
			.setTaskName(taskName)
			.build(bufferWriter);
		return output;
	}

	private void handleTimerException(Exception ex) {
//		handleAsyncException("Caught exception while processing timer.", new TimerException(ex));
	}

	@VisibleForTesting
	ProcessingTimeCallback deferCallbackToMailbox(MailboxExecutor mailboxExecutor, ProcessingTimeCallback callback) {
		return timestamp -> {
			mailboxExecutor.execute(
				() -> invokeProcessingTimeCallback(callback, timestamp),
				"Timer callback for %s @ %d",
				callback,
				timestamp);
		};
	}

	private void invokeProcessingTimeCallback(ProcessingTimeCallback callback, long timestamp) {
		try {
			callback.onProcessingTime(timestamp);
		} catch (Throwable t) {
//			handleAsyncException("Caught exception while processing timer.", new TimerException(t));
		}
	}
}
