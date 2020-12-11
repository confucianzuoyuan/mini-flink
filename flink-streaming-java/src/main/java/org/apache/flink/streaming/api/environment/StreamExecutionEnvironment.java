/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import org.apache.flink.api.java.Utils;
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
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

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

	/**
	 * Creates a new {@link StreamExecutionEnvironment} that will use the given {@link
	 * Configuration} to configure the {@link PipelineExecutor}.
	 */
	@PublicEvolving
	public StreamExecutionEnvironment(final Configuration configuration) {
		this(configuration, null);
	}

	/**
	 * Creates a new {@link StreamExecutionEnvironment} that will use the given {@link
	 * Configuration} to configure the {@link PipelineExecutor}.
	 *
	 * <p>In addition, this constructor allows specifying the user code {@link ClassLoader}.
	 */
	@PublicEvolving
	public StreamExecutionEnvironment(
			final Configuration configuration,
			final ClassLoader userClassloader) {
		this(new DefaultExecutorServiceLoader(), configuration, userClassloader);
	}

	/**
	 * Creates a new {@link StreamExecutionEnvironment} that will use the given {@link
	 * Configuration} to configure the {@link PipelineExecutor}.
	 *
	 * <p>In addition, this constructor allows specifying the {@link PipelineExecutorServiceLoader} and
	 * user code {@link ClassLoader}.
	 */
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
		Preconditions.checkNotNull(data, "Collection must not be null");

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

		clean(function);

		final StreamSource<OUT, ?> sourceOperator = new StreamSource<>(function);
		return new DataStreamSource<>(this, resolvedTypeInfo, sourceOperator, isParallel, sourceName);
	}

	public JobExecutionResult execute() throws Exception {
		return execute(DEFAULT_JOB_NAME);
	}

	public JobExecutionResult execute(String jobName) throws Exception {
		return execute(getStreamGraph(jobName));
	}

	@Internal
	public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
		final JobClient jobClient = executeAsync(streamGraph);

		try {
			final JobExecutionResult jobExecutionResult;

			jobExecutionResult = jobClient.getJobExecutionResult(userClassloader).get();
			System.out.println(jobExecutionResult.toString());

			jobListeners.forEach(jobListener -> jobListener.onJobExecuted(jobExecutionResult, null));

			return jobExecutionResult;
		} catch (Throwable t) {
			return null;
		}
	}

	/**
	 * Clear all registered {@link JobListener}s.
	 */
	@PublicEvolving
	public void clearJobListeners() {
		this.jobListeners.clear();
	}

	/**
	 * Triggers the program asynchronously. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 *
	 * <p>The program execution will be logged and displayed with a generated
	 * default name.
	 *
	 * @return A {@link JobClient} that can be used to communicate with the submitted job, completed on submission succeeded.
	 * @throws Exception which occurs during job execution.
	 */
	@PublicEvolving
	public final JobClient executeAsync() throws Exception {
		return executeAsync(DEFAULT_JOB_NAME);
	}

	/**
	 * Triggers the program execution asynchronously. The environment will execute all parts of
	 * the program that have resulted in a "sink" operation. Sink operations are
	 * for example printing results or forwarding them to a message queue.
	 *
	 * <p>The program execution will be logged and displayed with the provided name
	 *
	 * @param jobName desired name of the job
	 * @return A {@link JobClient} that can be used to communicate with the submitted job, completed on submission succeeded.
	 * @throws Exception which occurs during job execution.
	 */
	@PublicEvolving
	public JobClient executeAsync(String jobName) throws Exception {
		return executeAsync(getStreamGraph(checkNotNull(jobName)));
	}

	@Internal
	public JobClient executeAsync(StreamGraph streamGraph) throws Exception {
		final PipelineExecutorFactory executorFactory =
			executorServiceLoader.getExecutorFactory(configuration);

		CompletableFuture<JobClient> jobClientFuture = executorFactory
			.getExecutor(configuration)
			.execute(streamGraph, configuration);

		JobClient jobClient = jobClientFuture.get();
		jobListeners.forEach(jobListener -> jobListener.onJobSubmitted(jobClient, null));
		return jobClient;
	}

	/**
	 * Getter of the {@link org.apache.flink.streaming.api.graph.StreamGraph} of the streaming job. This call
	 * clears previously registered {@link Transformation transformations}.
	 *
	 * @return The streamgraph representing the transformations
	 */
	@Internal
	public StreamGraph getStreamGraph() {
		return getStreamGraph(DEFAULT_JOB_NAME);
	}

	/**
	 * Getter of the {@link org.apache.flink.streaming.api.graph.StreamGraph} of the streaming job. This call
	 * clears previously registered {@link Transformation transformations}.
	 *
	 * @param jobName Desired name of the job
	 * @return The streamgraph representing the transformations
	 */
	@Internal
	public StreamGraph getStreamGraph(String jobName) {
		return getStreamGraph(jobName, true);
	}

	/**
	 * Getter of the {@link org.apache.flink.streaming.api.graph.StreamGraph StreamGraph} of the streaming job
	 * with the option to clear previously registered {@link Transformation transformations}. Clearing the
	 * transformations allows, for example, to not re-execute the same operations when calling
	 * {@link #execute()} multiple times.
	 *
	 * @param jobName Desired name of the job
	 * @param clearTransformations Whether or not to clear previously registered transformations
	 * @return The streamgraph representing the transformations
	 */
	@Internal
	public StreamGraph getStreamGraph(String jobName, boolean clearTransformations) {
		StreamGraph streamGraph = getStreamGraphGenerator().setJobName(jobName).generate();
		if (clearTransformations) {
			this.transformations.clear();
		}
		System.out.println(streamGraph.getStreamingPlanAsJSON());
		return streamGraph;
	}

	private StreamGraphGenerator getStreamGraphGenerator() {
		if (transformations.size() <= 0) {
			throw new IllegalStateException("No operators defined in streaming topology. Cannot execute.");
		}
		return new StreamGraphGenerator(transformations, config)
			.setChaining(isChainingEnabled)
			.setDefaultBufferTimeout(bufferTimeout);
	}

	/**
	 * Creates the plan with which the system will execute the program, and
	 * returns it as a String using a JSON representation of the execution data
	 * flow graph. Note that this needs to be called, before the plan is
	 * executed.
	 *
	 * @return The execution plan of the program, as a JSON String.
	 */
	public String getExecutionPlan() {
		return getStreamGraph(DEFAULT_JOB_NAME, false).getStreamingPlanAsJSON();
	}

	/**
	 * Returns a "closure-cleaned" version of the given function. Cleans only if closure cleaning
	 * is not disabled in the {@link org.apache.flink.api.common.ExecutionConfig}
	 */
	@Internal
	public <F> F clean(F f) {
		if (getConfig().isClosureCleanerEnabled()) {
			ClosureCleaner.clean(f, getConfig().getClosureCleanerLevel(), true);
		}
		ClosureCleaner.ensureSerializable(f);
		return f;
	}

	/**
	 * Adds an operator to the list of operators that should be executed when calling
	 * {@link #execute}.
	 *
	 * <p>When calling {@link #execute()} only the operators that where previously added to the list
	 * are executed.
	 *
	 * <p>This is not meant to be used by users. The API methods that create operators must call
	 * this method.
	 */
	@Internal
	public void addOperator(Transformation<?> transformation) {
		Preconditions.checkNotNull(transformation, "transformation must not be null.");
		this.transformations.add(transformation);
	}

	// --------------------------------------------------------------------------------------------
	//  Factory methods for ExecutionEnvironments
	// --------------------------------------------------------------------------------------------

	/**
	 * Creates an execution environment that represents the context in which the
	 * program is currently executed. If the program is invoked standalone, this
	 * method returns a local execution environment, as returned by
	 * {@link #createLocalEnvironment()}.
	 *
	 * @return The execution environment of the context in which the program is
	 * executed.
	 */
	public static StreamExecutionEnvironment getExecutionEnvironment() {
		return Utils.resolveFactory(threadLocalContextEnvironmentFactory, contextEnvironmentFactory)
			.map(StreamExecutionEnvironmentFactory::createExecutionEnvironment)
			.orElseGet(StreamExecutionEnvironment::createLocalEnvironment);
	}

	/**
	 * Creates a {@link LocalStreamEnvironment}. The local execution environment
	 * will run the program in a multi-threaded fashion in the same JVM as the
	 * environment was created in. The default parallelism of the local
	 * environment is the number of hardware contexts (CPU cores / threads),
	 * unless it was specified differently by {@link #setParallelism(int)}.
	 *
	 * @return A local execution environment.
	 */
	public static LocalStreamEnvironment createLocalEnvironment() {
		return createLocalEnvironment(defaultLocalParallelism);
	}

	/**
	 * Creates a {@link LocalStreamEnvironment}. The local execution environment
	 * will run the program in a multi-threaded fashion in the same JVM as the
	 * environment was created in. It will use the parallelism specified in the
	 * parameter.
	 *
	 * @param parallelism
	 * 		The parallelism for the local environment.
	 * @return A local execution environment with the specified parallelism.
	 */
	public static LocalStreamEnvironment createLocalEnvironment(int parallelism) {
		return createLocalEnvironment(parallelism, new Configuration());
	}

	/**
	 * Creates a {@link LocalStreamEnvironment}. The local execution environment
	 * will run the program in a multi-threaded fashion in the same JVM as the
	 * environment was created in. It will use the parallelism specified in the
	 * parameter.
	 *
	 * @param parallelism
	 * 		The parallelism for the local environment.
	 * 	@param configuration
	 * 		Pass a custom configuration into the cluster
	 * @return A local execution environment with the specified parallelism.
	 */
	public static LocalStreamEnvironment createLocalEnvironment(int parallelism, Configuration configuration) {
		final LocalStreamEnvironment currentEnvironment;

		currentEnvironment = new LocalStreamEnvironment(configuration);
		currentEnvironment.setParallelism(parallelism);

		return currentEnvironment;
	}

	/**
	 * Gets the default parallelism that will be used for the local execution environment created by
	 * {@link #createLocalEnvironment()}.
	 *
	 * @return The default local parallelism
	 */
	@PublicEvolving
	public static int getDefaultLocalParallelism() {
		return defaultLocalParallelism;
	}

	/**
	 * Sets the default parallelism that will be used for the local execution
	 * environment created by {@link #createLocalEnvironment()}.
	 *
	 * @param parallelism The parallelism to use as the default local parallelism.
	 */
	@PublicEvolving
	public static void setDefaultLocalParallelism(int parallelism) {
		defaultLocalParallelism = parallelism;
	}

	// --------------------------------------------------------------------------------------------
	//  Methods to control the context and local environments for execution from packaged programs
	// --------------------------------------------------------------------------------------------

	protected static void initializeContextEnvironment(StreamExecutionEnvironmentFactory ctx) {
		contextEnvironmentFactory = ctx;
		threadLocalContextEnvironmentFactory.set(contextEnvironmentFactory);
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
