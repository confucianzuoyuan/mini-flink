package org.apache.flink.client.deployment.executors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.client.program.PerJobMiniClusterFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.PipelineExecutor;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;

import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

@Internal
public class LocalExecutor implements PipelineExecutor {

	public static final String NAME = "local";

	private final Configuration configuration;
	private final Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory;

	public static LocalExecutor create(Configuration configuration) {
		return new LocalExecutor(configuration, MiniCluster::new);
	}

	private LocalExecutor(Configuration configuration, Function<MiniClusterConfiguration, MiniCluster> miniClusterFactory) {
		this.configuration = configuration;
		this.miniClusterFactory = miniClusterFactory;
	}

	@Override
	public CompletableFuture<JobClient> execute(Pipeline pipeline, Configuration configuration) throws Exception {
		Configuration effectiveConfig = new Configuration();
		effectiveConfig.addAll(this.configuration);
		effectiveConfig.addAll(configuration);

		// 获取JobGraph
		// StreamGraph -> JobGraph
		final JobGraph jobGraph = PipelineExecutorUtils.getJobGraph(pipeline, effectiveConfig);

		// 提交JobGraph
		// 本地执行时，每一个作业启动一个MiniCluster
		return PerJobMiniClusterFactory.createWithFactory(effectiveConfig, miniClusterFactory).submitJob(jobGraph);
	}
}
