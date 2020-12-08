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

package org.apache.flink.runtime.clusterframework;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.shaded.guava18.com.google.common.escape.Escaper;
import org.apache.flink.shaded.guava18.com.google.common.escape.Escapers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * Tools for starting JobManager and TaskManager processes, including the
 * Actor Systems used to run the JobManager and TaskManager actors.
 */
public class BootstrapTools {
	/**
	 * Internal option which says if default value is used for {@link CoreOptions#TMP_DIRS}.
	 */
	private static final ConfigOption<Boolean> USE_LOCAL_DEFAULT_TMP_DIRS = key("internal.io.tmpdirs.use-local-default")
		.defaultValue(false);

	private static final Logger LOG = LoggerFactory.getLogger(BootstrapTools.class);

	private static final Escaper UNIX_SINGLE_QUOTE_ESCAPER = Escapers.builder()
		.addEscape('\'', "'\\''")
		.build();

	private static final Escaper WINDOWS_DOUBLE_QUOTE_ESCAPER = Escapers.builder()
		.addEscape('"', "\\\"")
		.addEscape('^', "\"^^\"")
		.build();

	public static ActorSystem startLocalActorSystem(
		Configuration configuration,
		String actorSystemName,
		Logger logger,
		ActorSystemExecutorConfiguration actorSystemExecutorConfiguration,
		Config customConfig) throws Exception {

		logger.info("Trying to start local actor system");

		try {
			Config akkaConfig = AkkaUtils.getAkkaConfig(
				configuration,
				scala.Option.empty(),
				scala.Option.empty(),
				actorSystemExecutorConfiguration.getAkkaConfig());

			if (customConfig != null) {
				akkaConfig = customConfig.withFallback(akkaConfig);
			}

			return startActorSystem(akkaConfig, actorSystemName, logger);
		}
		catch (Throwable t) {
			throw new Exception("Could not create actor system", t);
		}
	}

	/**
	 * Starts an Actor System with given Akka config.
	 * @param akkaConfig Config of the started ActorSystem.
	 * @param actorSystemName Name of the started ActorSystem.
	 * @param logger The logger to output log information.
	 * @return The ActorSystem which has been started.
	 */
	private static ActorSystem startActorSystem(Config akkaConfig, String actorSystemName, Logger logger) {
		logger.debug("Using akka configuration\n {}", akkaConfig);
		ActorSystem actorSystem = AkkaUtils.createActorSystem(actorSystemName, akkaConfig);

		logger.info("Actor system started at {}", AkkaUtils.getAddress(actorSystem));
		return actorSystem;
	}

	private static final String DYNAMIC_PROPERTIES_OPT = "D";

	// ------------------------------------------------------------------------

	/** Private constructor to prevent instantiation. */
	private BootstrapTools() {}

	/**
	 * Configuration interface for {@link ActorSystem} underlying executor.
	 */
	public interface ActorSystemExecutorConfiguration {

		/**
		 * Create the executor {@link Config} for the respective executor.
		 *
		 * @return Akka config for the respective executor
		 */
		Config getAkkaConfig();
	}

	/**
	 * Configuration for a fork join executor.
	 */
	public static class ForkJoinExecutorConfiguration implements ActorSystemExecutorConfiguration {

		private final double parallelismFactor;

		private final int minParallelism;

		private final int maxParallelism;

		public ForkJoinExecutorConfiguration(double parallelismFactor, int minParallelism, int maxParallelism) {
			this.parallelismFactor = parallelismFactor;
			this.minParallelism = minParallelism;
			this.maxParallelism = maxParallelism;
		}

		public double getParallelismFactor() {
			return parallelismFactor;
		}

		public int getMinParallelism() {
			return minParallelism;
		}

		public int getMaxParallelism() {
			return maxParallelism;
		}

		@Override
		public Config getAkkaConfig() {
			return AkkaUtils.getForkJoinExecutorConfig(this);
		}

		public static ForkJoinExecutorConfiguration fromConfiguration(final Configuration configuration) {
			final double parallelismFactor = configuration.getDouble(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_FACTOR);
			final int minParallelism = configuration.getInteger(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MIN);
			final int maxParallelism = configuration.getInteger(AkkaOptions.FORK_JOIN_EXECUTOR_PARALLELISM_MAX);

			return new ForkJoinExecutorConfiguration(parallelismFactor, minParallelism, maxParallelism);
		}
	}

	/**
	 * Configuration for a fixed thread pool executor.
	 */
	public static class FixedThreadPoolExecutorConfiguration implements ActorSystemExecutorConfiguration {

		private final int minNumThreads;

		private final int maxNumThreads;

		private final int threadPriority;

		public FixedThreadPoolExecutorConfiguration(int minNumThreads, int maxNumThreads, int threadPriority) {
			if (threadPriority < Thread.MIN_PRIORITY || threadPriority > Thread.MAX_PRIORITY) {
				throw new IllegalArgumentException(
					String.format(
						"The thread priority must be within (%s, %s) but it was %s.",
						Thread.MIN_PRIORITY,
						Thread.MAX_PRIORITY,
						threadPriority));
			}

			this.minNumThreads = minNumThreads;
			this.maxNumThreads = maxNumThreads;
			this.threadPriority = threadPriority;
		}

		public int getMinNumThreads() {
			return minNumThreads;
		}

		public int getMaxNumThreads() {
			return maxNumThreads;
		}

		public int getThreadPriority() {
			return threadPriority;
		}

		@Override
		public Config getAkkaConfig() {
			return AkkaUtils.getThreadPoolExecutorConfig(this);
		}
	}

}
