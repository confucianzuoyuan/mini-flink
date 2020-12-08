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

package org.apache.flink.runtime.jobgraph.tasks;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;

import static org.apache.flink.util.Preconditions.checkNotNull;

public abstract class AbstractInvokable {

	/** The environment assigned to this invokable. */
	private final Environment environment;

	/** Flag whether cancellation should interrupt the executing thread. */
	private volatile boolean shouldInterruptOnCancel = true;

	/**
	 * Create an Invokable task and set its environment.
	 *
	 * @param environment The environment assigned to this invokable.
	 */
	public AbstractInvokable(Environment environment) {
		this.environment = checkNotNull(environment);
	}

	public abstract void invoke() throws Exception;

	public void cancel() throws Exception {
		// The default implementation does nothing.
	}

	public void setShouldInterruptOnCancel(boolean shouldInterruptOnCancel) {
		this.shouldInterruptOnCancel = shouldInterruptOnCancel;
	}

	/**
	 * Checks whether the task should be interrupted during cancellation.
	 * This method is check both for the initial interrupt, as well as for the
	 * repeated interrupt. Setting the interruption to false via
	 * {@link #setShouldInterruptOnCancel(boolean)} is a way to stop further interrupts
	 * from happening.
	 */
	public boolean shouldInterruptOnCancel() {
		return shouldInterruptOnCancel;
	}

	// ------------------------------------------------------------------------
	//  Access to Environment and Configuration
	// ------------------------------------------------------------------------

	/**
	 * Returns the environment of this task.
	 *
	 * @return The environment of this task.
	 */
	public final Environment getEnvironment() {
		return this.environment;
	}

	/**
	 * Returns the user code class loader of this invokable.
	 *
	 * @return user code class loader of this invokable.
	 */
	public final ClassLoader getUserCodeClassLoader() {
		return getEnvironment().getUserClassLoader();
	}

	/**
	 * Returns the current number of subtasks the respective task is split into.
	 *
	 * @return the current number of subtasks the respective task is split into
	 */
	public int getCurrentNumberOfSubtasks() {
		return this.environment.getTaskInfo().getNumberOfParallelSubtasks();
	}

	/**
	 * Returns the task configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobVertex}.
	 *
	 * @return the task configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobVertex}
	 */
	public final Configuration getTaskConfiguration() {
		return this.environment.getTaskConfiguration();
	}

	/**
	 * Returns the job configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobGraph}.
	 *
	 * @return the job configuration object which was attached to the original {@link org.apache.flink.runtime.jobgraph.JobGraph}
	 */
	public Configuration getJobConfiguration() {
		return this.environment.getJobConfiguration();
	}

	/**
	 * Returns the global ExecutionConfig.
	 */
	public ExecutionConfig getExecutionConfig() {
		return this.environment.getExecutionConfig();
	}

	public void dispatchOperatorEvent(OperatorID operator, SerializedValue<OperatorEvent> event) throws FlinkException {
		throw new UnsupportedOperationException("dispatchOperatorEvent not supported by " + getClass().getName());
	}
}
