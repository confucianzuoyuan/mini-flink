package org.apache.flink.streaming.api.environment;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.execution.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.functions.source.FromElementsFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Public
public class StreamExecutionEnvironment {

	/** The default name to use for a streaming job if no other name has been specified. */
	public static final String DEFAULT_JOB_NAME = "Flink Streaming Job";

	/** The default buffer timeout (max delay of records in the network stack). */
	private static final long DEFAULT_NETWORK_BUFFER_TIMEOUT = 100L;

	/**
	 * The environment of the context (local by default, cluster if invoked through command line).
	 */
	private static StreamExecutionEnvironmentFactory contextEnvironmentFactory = null;

	/** The ThreadLocal used to store {@link StreamExecutionEnvironmentFactory}. */
	private static final ThreadLocal<StreamExecutionEnvironmentFactory> threadLocalContextEnvironmentFactory = new ThreadLocal<>();

	/** The default parallelism used when creating a local environment. */
	private static int defaultLocalParallelism = Runtime.getRuntime().availableProcessors();

	// ------------------------------------------------------------------------

	/** The execution configuration for this environment. */
	private final ExecutionConfig config = new ExecutionConfig();

	protected final List<Transformation<?>> transformations = new ArrayList<>();

	private long bufferTimeout = DEFAULT_NETWORK_BUFFER_TIMEOUT;

	protected boolean isChainingEnabled = true;

	private final PipelineExecutorServiceLoader executorServiceLoader;

	private final Configuration configuration;

	private final ClassLoader userClassloader;

	private final List<JobListener> jobListeners = new ArrayList<>();

	// --------------------------------------------------------------------------------------------
	// Constructor and Properties
	// --------------------------------------------------------------------------------------------

	public StreamExecutionEnvironment() {
		this(new Configuration());
		// unfortunately, StreamExecutionEnvironment always (implicitly) had a public constructor.
		// This constructor is not useful because the execution environment cannot be used for
		// execution. We're keeping this to appease the binary compatibiliy checks.
	}

	@PublicEvolving
	public StreamExecutionEnvironment(final Configuration configuration) {
		this(configuration, null);
	}

	@PublicEvolving
	public StreamExecutionEnvironment(
			final Configuration configuration,
			final ClassLoader userClassloader) {
		this(new DefaultExecutorServiceLoader(), configuration, userClassloader);
	}

	@PublicEvolving
	public StreamExecutionEnvironment(
			final PipelineExecutorServiceLoader executorServiceLoader,
			final Configuration configuration,
			final ClassLoader userClassloader) {
		this.executorServiceLoader = checkNotNull(executorServiceLoader);
		this.configuration = checkNotNull(configuration);
		this.userClassloader = userClassloader == null ? getClass().getClassLoader() : userClassloader;

		this.configure(this.configuration, this.userClassloader);
	}

	protected Configuration getConfiguration() {
		return this.configuration;
	}

	protected ClassLoader getUserClassloader() {
		return userClassloader;
	}

	/**
	 * Gets the config object.
	 */
	public ExecutionConfig getConfig() {
		return config;
	}

	/**
	 * Gets the config JobListeners.
	 */
	@PublicEvolving
	public List<JobListener> getJobListeners() {
		return jobListeners;
	}

	public StreamExecutionEnvironment setParallelism(int parallelism) {
		config.setParallelism(parallelism);
		return this;
	}

	public int getParallelism() {
		return config.getParallelism();
	}

	public StreamExecutionEnvironment setBufferTimeout(long timeoutMillis) {
		if (timeoutMillis < -1) {
			throw new IllegalArgumentException("Timeout of buffer must be non-negative or -1");
		}

		this.bufferTimeout = timeoutMillis;
		return this;
	}

	public long getBufferTimeout() {
		return this.bufferTimeout;
	}

	@PublicEvolving
	public void configure(ReadableConfig configuration, ClassLoader classLoader) {
		config.configure(configuration, classLoader);
	}

	@SafeVarargs
	public final <OUT> DataStreamSource<OUT> fromElements(OUT... data) {
		if (data.length == 0) {
			throw new IllegalArgumentException("fromElements needs at least one element as argument");
		}

		TypeInformation<OUT> typeInfo;
		try {
			typeInfo = TypeExtractor.getForObject(data[0]);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not create TypeInformation for type " + data[0].getClass().getName()
					+ "; please specify the TypeInformation manually via "
					+ "StreamExecutionEnvironment#fromElements(Collection, TypeInformation)", e);
		}
		return fromCollection(Arrays.asList(data), typeInfo);
	}

	public <OUT> DataStreamSource<OUT> fromCollection(Collection<OUT> data, TypeInformation<OUT> typeInfo) {
		SourceFunction<OUT> function;
		try {
			function = new FromElementsFunction<>(typeInfo.createSerializer(getConfig()), data);
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		return addSource(function, "Collection Source", typeInfo).setParallelism(1);
	}

	public <OUT> DataStreamSource<OUT> addSource(SourceFunction<OUT> function, String sourceName, TypeInformation<OUT> typeInfo) {

		TypeInformation<OUT> resolvedTypeInfo = getTypeInfo(function, sourceName, SourceFunction.class, typeInfo);

		boolean isParallel = function instanceof ParallelSourceFunction;

//		clean(function);

		final StreamSource<OUT, ?> sourceOperator = new StreamSource<>(function);
		return new DataStreamSource<>(this, resolvedTypeInfo, sourceOperator, isParallel, sourceName);
	}

	public JobExecutionResult execute() throws Exception {
		return execute(DEFAULT_JOB_NAME);
	}

	public JobExecutionResult execute(String jobName) throws Exception {
		// getStreamGraph：获取StreamGraph
		// execute：执行
		return execute(getStreamGraph(jobName));
	}

	@Internal
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		final JobClient jobClient = executeAsync(streamGraph);

		try {
			final JobExecutionResult jobExecutionResult;

			jobExecutionResult = jobClient.getJobExecutionResult(userClassloader).get();

			jobListeners.forEach(jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));

			return jobExecutionResult;
		} catch (Throwable t) {
			return null;
		}
	}

	@Internal
	public JobClient executeAsync(StreamGraph streamGraph) throws Exception {
		final PipelineExecutorFactory executorFactory =
			executorServiceLoader.getExecutorFactory(configuration);

		CompletableFuture<JobClient> jobClientFuture = executorFactory
			// 获取执行器：LocalExecutor
			.getExecutor(configuration)
			// 执行
			.execute(streamGraph, configuration);

		JobClient jobClient = jobClientFuture.get();
		jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));
		return jobClient;
	}

	@Internal
	public StreamGraph getStreamGraph(String jobName) {
		// generate：生成StreamGraph
		return getStreamGraphGenerator().setJobName(jobName).generate();
	}

	private StreamGraphGenerator getStreamGraphGenerator() {
		return new StreamGraphGenerator(transformations, config)
			.setChaining(isChainingEnabled)
			.setDefaultBufferTimeout(bufferTimeout);
	}

	@Internal
	public <F> F clean(F f) {
		if (getConfig().isClosureCleanerEnabled()) {
			ClosureCleaner.clean(f, getConfig().getClosureCleanerLevel(), true);
		}
		ClosureCleaner.ensureSerializable(f);
		return f;
	}

	@Internal
	public void addOperator(Transformation<?> transformation) {
		this.transformations.add(transformation);
	}

	public static <T> Optional<T> resolveFactory(ThreadLocal<T> threadLocalFactory, @Nullable T staticFactory) {
		final T localFactory = threadLocalFactory.get();
		final T factory = localFactory == null ? staticFactory : localFactory;

		return Optional.ofNullable(factory);
	}

	public static StreamExecutionEnvironment getExecutionEnvironment() {
		return resolveFactory(threadLocalContextEnvironmentFactory, contextEnvironmentFactory)
			.map(StreamExecutionEnvironmentFactory::createExecutionEnvironment)
			.orElseGet(StreamExecutionEnvironment::createLocalEnvironment);
	}

	public static LocalStreamEnvironment createLocalEnvironment() {
		return createLocalEnvironment(defaultLocalParallelism);
	}

	public static LocalStreamEnvironment createLocalEnvironment(int parallelism) {
		return createLocalEnvironment(parallelism, new Configuration());
	}

	public static LocalStreamEnvironment createLocalEnvironment(int parallelism, Configuration configuration) {
		final LocalStreamEnvironment currentEnvironment;

		currentEnvironment = new LocalStreamEnvironment(configuration);
		currentEnvironment.setParallelism(parallelism);

		return currentEnvironment;
	}

	protected static void resetContextEnvironment() {
		contextEnvironmentFactory = null;
		threadLocalContextEnvironmentFactory.remove();
	}

	// Private helpers.
	@SuppressWarnings("unchecked")
	private <OUT, T extends TypeInformation<OUT>> T getTypeInfo(
			Object source,
			String sourceName,
			Class<?> baseSourceClass,
			TypeInformation<OUT> typeInfo) {
		TypeInformation<OUT> resolvedTypeInfo = typeInfo;
		if (source instanceof ResultTypeQueryable) {
			resolvedTypeInfo = ((ResultTypeQueryable<OUT>) source).getProducedType();
		}
		if (resolvedTypeInfo == null) {
			try {
				resolvedTypeInfo = TypeExtractor.createTypeInfo(
						baseSourceClass,
						source.getClass(), 0, null, null);
			} catch (final InvalidTypesException e) {
				resolvedTypeInfo = (TypeInformation<OUT>) new MissingTypeInfo(sourceName, e);
			}
		}
		return (T) resolvedTypeInfo;
	}
}
