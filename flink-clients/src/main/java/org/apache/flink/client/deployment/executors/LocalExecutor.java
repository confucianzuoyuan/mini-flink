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

import java.net.MalformedURLException;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * An {@link PipelineExecutor} for executing a {@link Pipeline} locally.
 */
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

		final JobGraph jobGraph = getJobGraph(pipeline, effectiveConfig);

		return PerJobMiniClusterFactory.createWithFactory(effectiveConfig, miniClusterFactory).submitJob(jobGraph);
	}

	private JobGraph getJobGraph(Pipeline pipeline, Configuration configuration) throws MalformedURLException {
		return PipelineExecutorUtils.getJobGraph(pipeline, configuration);
	}
}
