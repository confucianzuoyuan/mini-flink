/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.dispatcher.runner;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherId;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.JobGraphStore;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/**
 * Process which encapsulates the job recovery logic and life cycle management of a
 * {@link Dispatcher}.
 */
public class SessionDispatcherLeaderProcess extends AbstractDispatcherLeaderProcess implements JobGraphStore.JobGraphListener {

	private final DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory;

	private final JobGraphStore jobGraphStore;

	private final Executor ioExecutor;

	private CompletableFuture<Void> onGoingRecoveryOperation = FutureUtils.completedVoidFuture();

	private SessionDispatcherLeaderProcess(
			UUID leaderSessionId,
			DispatcherGatewayServiceFactory dispatcherGatewayServiceFactory,
			JobGraphStore jobGraphStore,
			Executor ioExecutor,
			FatalErrorHandler fatalErrorHandler) {
		super(leaderSessionId, fatalErrorHandler);

		this.dispatcherGatewayServiceFactory = dispatcherGatewayServiceFactory;
		this.jobGraphStore = jobGraphStore;
		this.ioExecutor = ioExecutor;
	}

	@Override
	protected void onStart() {
		startServices();

		onGoingRecoveryOperation = recoverJobsAsync()
			.thenAccept(this::createDispatcherIfRunning)
			.handle(this::onErrorIfRunning);
	}

	private void startServices() {
		try {
			jobGraphStore.start(this);
		} catch (Exception e) {
			throw new FlinkRuntimeException(
				String.format(
					"Could not start %s when trying to start the %s.",
					jobGraphStore.getClass().getSimpleName(),
					getClass().getSimpleName()),
				e);
		}
	}

	private void createDispatcherIfRunning(Collection<JobGraph> jobGraphs) {
		runIfStateIs(State.RUNNING, () -> createDispatcher(jobGraphs));
	}

	private void createDispatcher(Collection<JobGraph> jobGraphs) {

		final DispatcherGatewayService dispatcherService = dispatcherGatewayServiceFactory.create(
			DispatcherId.fromUuid(getLeaderSessionId()),
			jobGraphs,
			jobGraphStore);

		completeDispatcherSetup(dispatcherService);
	}

	private CompletableFuture<Collection<JobGraph>> recoverJobsAsync() {
		return CompletableFuture.supplyAsync(
			this::recoverJobsIfRunning,
			ioExecutor);
	}

	private Collection<JobGraph> recoverJobsIfRunning() {
		return supplyUnsynchronizedIfRunning(this::recoverJobs).orElse(Collections.emptyList());

	}

	private Collection<JobGraph> recoverJobs() {
		log.info("Recover all persisted job graphs.");
		final Collection<JobID> jobIds = getJobIds();
		final Collection<JobGraph> recoveredJobGraphs = new ArrayList<>();

		for (JobID jobId : jobIds) {
			recoveredJobGraphs.add(recoverJob(jobId));
		}

		log.info("Successfully recovered {} persisted job graphs.", recoveredJobGraphs.size());

		return recoveredJobGraphs;
	}

	private Collection<JobID> getJobIds() {
		try {
			return jobGraphStore.getJobIds();
		} catch (Exception e) {
			throw new FlinkRuntimeException(
				"Could not retrieve job ids of persisted jobs.",
				e);
		}
	}

	private JobGraph recoverJob(JobID jobId) {
		log.info("Trying to recover job with job id {}.", jobId);
		try {
			return jobGraphStore.recoverJobGraph(jobId);
		} catch (Exception e) {
			throw new FlinkRuntimeException(
				String.format("Could not recover job with job id %s.", jobId),
				e);
		}
	}

	@Override
	protected CompletableFuture<Void> onClose() {
		return CompletableFuture.runAsync(
			this::stopServices,
			ioExecutor);
	}

	private void stopServices() {
		try {
			jobGraphStore.stop();
		} catch (Exception e) {
			ExceptionUtils.rethrow(e);
		}
	}

	// ---------------------------------------------------------------
	// Factory methods
	// ---------------------------------------------------------------

	public static SessionDispatcherLeaderProcess create(
			UUID leaderSessionId,
			DispatcherGatewayServiceFactory dispatcherFactory,
			JobGraphStore jobGraphStore,
			Executor ioExecutor,
			FatalErrorHandler fatalErrorHandler) {
		return new SessionDispatcherLeaderProcess(
			leaderSessionId,
			dispatcherFactory,
			jobGraphStore,
			ioExecutor,
			fatalErrorHandler);
	}
}
