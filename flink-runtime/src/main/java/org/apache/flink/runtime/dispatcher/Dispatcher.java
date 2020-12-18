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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.PermanentlyFencedRpcEndpoint;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.akka.AkkaRpcServiceUtils;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.BiConsumerWithException;
import org.apache.flink.util.function.FunctionUtils;
import org.apache.flink.util.function.FunctionWithException;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.BiFunction;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for the Dispatcher component. The Dispatcher component is responsible
 * for receiving job submissions, persisting them, spawning JobManagers to execute
 * the jobs and to recover them in case of a master failure. Furthermore, it knows
 * about the state of the Flink session cluster.
 */
public abstract class Dispatcher extends PermanentlyFencedRpcEndpoint<DispatcherId> implements DispatcherGateway {

	public static final String DISPATCHER_NAME = "dispatcher";

	private final Configuration configuration;

	private final JobGraphWriter jobGraphWriter;
	private final RunningJobsRegistry runningJobsRegistry;

	private final HighAvailabilityServices highAvailabilityServices;
	private final GatewayRetriever<ResourceManagerGateway> resourceManagerGatewayRetriever;
	private final JobManagerSharedServices jobManagerSharedServices;
	private final HeartbeatServices heartbeatServices;
	private final BlobServer blobServer;

	private final FatalErrorHandler fatalErrorHandler;

	private final Map<JobID, CompletableFuture<JobManagerRunner>> jobManagerRunnerFutures;

	private final DispatcherBootstrap dispatcherBootstrap;

	private final ArchivedExecutionGraphStore archivedExecutionGraphStore;

	private final JobManagerRunnerFactory jobManagerRunnerFactory;

	private final HistoryServerArchivist historyServerArchivist;

	private final Map<JobID, CompletableFuture<Void>> jobManagerTerminationFutures;

	protected final CompletableFuture<ApplicationStatus> shutDownFuture;

	public Dispatcher(
			RpcService rpcService,
			DispatcherId fencingToken,
			DispatcherBootstrap dispatcherBootstrap,
			DispatcherServices dispatcherServices) throws Exception {
		super(rpcService, AkkaRpcServiceUtils.createRandomName(DISPATCHER_NAME), fencingToken);
		checkNotNull(dispatcherServices);

		this.configuration = dispatcherServices.getConfiguration();
		this.highAvailabilityServices = dispatcherServices.getHighAvailabilityServices();
		this.resourceManagerGatewayRetriever = dispatcherServices.getResourceManagerGatewayRetriever();
		this.heartbeatServices = dispatcherServices.getHeartbeatServices();
		this.blobServer = dispatcherServices.getBlobServer();
		this.fatalErrorHandler = dispatcherServices.getFatalErrorHandler();
		this.jobGraphWriter = dispatcherServices.getJobGraphWriter();

		this.jobManagerSharedServices = JobManagerSharedServices.fromConfiguration(
			configuration,
			blobServer,
			fatalErrorHandler);

		this.runningJobsRegistry = highAvailabilityServices.getRunningJobsRegistry();

		jobManagerRunnerFutures = new HashMap<>(16);

		this.historyServerArchivist = dispatcherServices.getHistoryServerArchivist();

		this.archivedExecutionGraphStore = dispatcherServices.getArchivedExecutionGraphStore();

		this.jobManagerRunnerFactory = dispatcherServices.getJobManagerRunnerFactory();

		this.jobManagerTerminationFutures = new HashMap<>(2);

		this.shutDownFuture = new CompletableFuture<>();

		this.dispatcherBootstrap = checkNotNull(dispatcherBootstrap);
	}

	//------------------------------------------------------
	// Getters
	//------------------------------------------------------

	public CompletableFuture<ApplicationStatus> getShutDownFuture() {
		return shutDownFuture;
	}

	//------------------------------------------------------
	// Lifecycle methods
	//------------------------------------------------------

	@Override
	public void onStart() throws Exception {
		dispatcherBootstrap.initialize(this, this.getRpcService().getScheduledExecutor());
	}

	void runRecoveredJob(final JobGraph recoveredJob) {
		checkNotNull(recoveredJob);
		FutureUtils.assertNoException(runJob(recoveredJob)
			.handle(handleRecoveredJobStartError(recoveredJob.getJobID())));
	}

	private BiFunction<Void, Throwable, Void> handleRecoveredJobStartError(JobID jobId) {
		return (ignored, throwable) -> {
			if (throwable != null) {
				onFatalError(new DispatcherException(String.format("Could not start recovered job %s.", jobId), throwable));
			}

			return null;
		};
	}

	@Override
	public CompletableFuture<Void> onStop() {
		log.info("Stopping dispatcher {}.", getAddress());

		final CompletableFuture<Void> allJobManagerRunnersTerminationFuture = terminateJobManagerRunnersAndGetTerminationFuture();

		return FutureUtils.runAfterwards(
			allJobManagerRunnersTerminationFuture,
			() -> {
				dispatcherBootstrap.stop();

				stopDispatcherServices();

				log.info("Stopped dispatcher {}.", getAddress());
			});
	}

	private void stopDispatcherServices() throws Exception {
		Exception exception = null;
		try {
			jobManagerSharedServices.shutdown();
		} catch (Exception e) {
			exception = e;
		}

		ExceptionUtils.tryRethrowException(exception);
	}

	//------------------------------------------------------
	// RPCs
	//------------------------------------------------------

	@Override
	public CompletableFuture<Acknowledge> submitJob(JobGraph jobGraph, Time timeout) {
		return internalSubmitJob(jobGraph);
	}

	private CompletableFuture<Acknowledge> internalSubmitJob(JobGraph jobGraph) {
		// 调用persistAndRunJob方法
		final CompletableFuture<Acknowledge> persistAndRunFuture = waitForTerminatingJobManager(jobGraph.getJobID(), jobGraph, this::persistAndRunJob)
			.thenApply(ignored -> Acknowledge.get());

		return persistAndRunFuture.handleAsync((acknowledge, throwable) -> acknowledge, getRpcService().getExecutor());
	}

	private CompletableFuture<Void> persistAndRunJob(JobGraph jobGraph) throws Exception {
		jobGraphWriter.putJobGraph(jobGraph);

		// 调用runJob
		final CompletableFuture<Void> runJobFuture = runJob(jobGraph);

		return runJobFuture.whenComplete(BiConsumerWithException.unchecked((Object ignored, Throwable throwable) -> {
			if (throwable != null) {
				jobGraphWriter.removeJobGraph(jobGraph.getJobID());
			}
		}));
	}

	private CompletableFuture<Void> runJob(JobGraph jobGraph) {
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = createJobManagerRunner(jobGraph);

		jobManagerRunnerFutures.put(jobGraph.getJobID(), jobManagerRunnerFuture);

		return jobManagerRunnerFuture
			// 启动作业管理器
			.thenApply(FunctionUtils.uncheckedFunction(this::startJobManagerRunner))
			.thenApply(FunctionUtils.nullFn())
			.whenCompleteAsync(
				(ignored, throwable) -> {
					if (throwable != null) {
						jobManagerRunnerFutures.remove(jobGraph.getJobID());
					}
				},
				getMainThreadExecutor());
	}

	private CompletableFuture<JobManagerRunner> createJobManagerRunner(JobGraph jobGraph) {
		final RpcService rpcService = getRpcService();

		return CompletableFuture.supplyAsync(
			() -> {
				try {
					return jobManagerRunnerFactory.createJobManagerRunner(
						jobGraph,
						configuration,
						rpcService,
						highAvailabilityServices,
						heartbeatServices,
						jobManagerSharedServices,
						fatalErrorHandler);
				} catch (Exception e) {
					throw new CompletionException(new JobExecutionException(jobGraph.getJobID(), "Could not instantiate JobManager.", e));
				}
			},
			rpcService.getExecutor());
	}

	private JobManagerRunner startJobManagerRunner(JobManagerRunner jobManagerRunner) throws Exception {
		jobManagerRunner.getResultFuture().handleAsync(
			(ArchivedExecutionGraph archivedExecutionGraph, Throwable throwable) -> {
				return null;
			}, getMainThreadExecutor());

		// 启动作业管理器
		jobManagerRunner.start();

		return jobManagerRunner;
	}

	@Override
	public CompletableFuture<JobResult> requestJobResult(JobID jobId, Time timeout) {
		final CompletableFuture<JobManagerRunner> jobManagerRunnerFuture = jobManagerRunnerFutures.get(jobId);

		if (jobManagerRunnerFuture == null) {
			final ArchivedExecutionGraph archivedExecutionGraph = archivedExecutionGraphStore.get(jobId);

			if (archivedExecutionGraph == null) {
				return FutureUtils.completedExceptionally(new FlinkJobNotFoundException(jobId));
			} else {
				return CompletableFuture.completedFuture(JobResult.createFrom(archivedExecutionGraph));
			}
		} else {
			return jobManagerRunnerFuture.thenCompose(JobManagerRunner::getResultFuture).thenApply(JobResult::createFrom);
		}
	}

	@Override
	public CompletableFuture<Integer> getBlobServerPort(Time timeout) {
		return CompletableFuture.completedFuture(blobServer.getPort());
	}

	@Override
	public CompletableFuture<Acknowledge> shutDownCluster() {
		return shutDownCluster(ApplicationStatus.SUCCEEDED);
	}

	@Override
	public CompletableFuture<Acknowledge> shutDownCluster(final ApplicationStatus applicationStatus) {
		shutDownFuture.complete(applicationStatus);
		return CompletableFuture.completedFuture(Acknowledge.get());
	}

	private CompletableFuture<Void> terminateJobManagerRunnersAndGetTerminationFuture() {
		final Collection<CompletableFuture<Void>> values = jobManagerTerminationFutures.values();
		return FutureUtils.completeAll(values);
	}

	protected void onFatalError(Throwable throwable) {
		fatalErrorHandler.onFatalError(throwable);
	}

	protected void jobNotFinished(JobID jobId) {
	}

	private CompletableFuture<Void> waitForTerminatingJobManager(JobID jobId, JobGraph jobGraph, FunctionWithException<JobGraph, CompletableFuture<Void>, ?> action) {
		final CompletableFuture<Void> jobManagerTerminationFuture = getJobTerminationFuture(jobId)
			.exceptionally((Throwable throwable) -> {
				throw new CompletionException(
					new DispatcherException(
						String.format("Termination of previous JobManager for job %s failed. Cannot submit job under the same job id.", jobId),
						throwable)); });

		return jobManagerTerminationFuture.thenComposeAsync(
			FunctionUtils.uncheckedFunction((ignored) -> {
				jobManagerTerminationFutures.remove(jobId);
				return action.apply(jobGraph);
			}),
			getMainThreadExecutor());
	}

	CompletableFuture<Void> getJobTerminationFuture(JobID jobId) {
		if (jobManagerRunnerFutures.containsKey(jobId)) {
			return FutureUtils.completedExceptionally(new DispatcherException(String.format("Job with job id %s is still running.", jobId)));
		} else {
			return jobManagerTerminationFutures.getOrDefault(jobId, CompletableFuture.completedFuture(null));
		}
	}
}
