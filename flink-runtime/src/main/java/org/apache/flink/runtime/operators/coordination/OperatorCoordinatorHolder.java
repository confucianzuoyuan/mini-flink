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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

public class OperatorCoordinatorHolder implements OperatorCoordinator {

	private final OperatorCoordinator coordinator;
	private final OperatorID operatorId;
	private final LazyInitializedCoordinatorContext context;
	private final OperatorEventValve eventValve;

	private final int operatorParallelism;
	private final int operatorMaxParallelism;

	private Consumer<Throwable> globalFailureHandler;
	private ComponentMainThreadExecutor mainThreadExecutor;

	private OperatorCoordinatorHolder(
			final OperatorID operatorId,
			final OperatorCoordinator coordinator,
			final LazyInitializedCoordinatorContext context,
			final OperatorEventValve eventValve,
			final int operatorParallelism,
			final int operatorMaxParallelism) {

		this.operatorId = checkNotNull(operatorId);
		this.coordinator = checkNotNull(coordinator);
		this.context = checkNotNull(context);
		this.eventValve = checkNotNull(eventValve);
		this.operatorParallelism = operatorParallelism;
		this.operatorMaxParallelism = operatorMaxParallelism;
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	public OperatorCoordinator coordinator() {
		return coordinator;
	}

	// ------------------------------------------------------------------------
	//  OperatorCoordinator Interface
	// ------------------------------------------------------------------------

	@Override
	public void start() throws Exception {
		mainThreadExecutor.assertRunningInMainThread();
		checkState(context.isInitialized(), "Coordinator Context is not yet initialized");
		coordinator.start();
	}

	@Override
	public void close() throws Exception {
		coordinator.close();
		context.unInitialize();
	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {
		mainThreadExecutor.assertRunningInMainThread();
		coordinator.subtaskFailed(subtask, reason);
		eventValve.resetForTask(subtask);
	}

	// ------------------------------------------------------------------------
	//  Nested Classes
	// ------------------------------------------------------------------------

	/**
	 * An implementation of the {@link OperatorCoordinator.Context}.
	 *
	 * <p>All methods are safe to be called from other threads than the Scheduler's and the JobMaster's
	 * main threads.
	 *
	 * <p>Implementation note: Ideally, we would like to operate purely against the scheduler
	 * interface, but it is not exposing enough information at the moment.
	 */
	private static final class LazyInitializedCoordinatorContext implements OperatorCoordinator.Context {

		private final OperatorID operatorId;
		private final OperatorEventValve eventValve;
		private final String operatorName;
		private final int operatorParallelism;

		private Consumer<Throwable> globalFailureHandler;
		private Executor schedulerExecutor;

		public LazyInitializedCoordinatorContext(
				final OperatorID operatorId,
				final OperatorEventValve eventValve,
				final String operatorName,
				final int operatorParallelism) {
			this.operatorId = checkNotNull(operatorId);
			this.eventValve = checkNotNull(eventValve);
			this.operatorName = checkNotNull(operatorName);
			this.operatorParallelism = operatorParallelism;
		}

		void lazyInitialize(Consumer<Throwable> globalFailureHandler, Executor schedulerExecutor) {
			this.globalFailureHandler = checkNotNull(globalFailureHandler);
			this.schedulerExecutor = checkNotNull(schedulerExecutor);
		}

		void unInitialize() {
			this.globalFailureHandler = null;
			this.schedulerExecutor = null;
		}

		boolean isInitialized() {
			return schedulerExecutor != null;
		}

		private void checkInitialized() {
			checkState(isInitialized(), "Context was not yet initialized");
		}

		@Override
		public OperatorID getOperatorId() {
			return operatorId;
		}

		@Override
		public CompletableFuture<Acknowledge> sendEvent(final OperatorEvent evt, final int targetSubtask) {
			checkInitialized();

			if (targetSubtask < 0 || targetSubtask >= currentParallelism()) {
				throw new IllegalArgumentException(
					String.format("subtask index %d out of bounds [0, %d).", targetSubtask, currentParallelism()));
			}

			final SerializedValue<OperatorEvent> serializedEvent;
			try {
				serializedEvent = new SerializedValue<>(evt);
			}
			catch (IOException e) {
				// we do not expect that this exception is handled by the caller, so we make it
				// unchecked so that it can bubble up
				throw new FlinkRuntimeException("Cannot serialize operator event", e);
			}

			return eventValve.sendEvent(serializedEvent, targetSubtask);
		}

		@Override
		public void failJob(final Throwable cause) {
			checkInitialized();

			final FlinkException e = new FlinkException("Global failure triggered by OperatorCoordinator for '" +
				operatorName + "' (operator " + operatorId + ").", cause);

			schedulerExecutor.execute(() -> globalFailureHandler.accept(e));
		}

		@Override
		public int currentParallelism() {
			return operatorParallelism;
		}
	}
}
