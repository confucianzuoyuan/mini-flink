package org.apache.flink.runtime.clusterframework;

import akka.actor.ActorSystem;
import com.typesafe.config.Config;
import org.apache.flink.configuration.AkkaOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.slf4j.Logger;

public class BootstrapTools {
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

	private static ActorSystem startActorSystem(Config akkaConfig, String actorSystemName, Logger logger) {
		logger.debug("Using akka configuration\n {}", akkaConfig);
		ActorSystem actorSystem = AkkaUtils.createActorSystem(actorSystemName, akkaConfig);

		logger.info("Actor system started at {}", AkkaUtils.getAddress(actorSystem));
		return actorSystem;
	}

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
