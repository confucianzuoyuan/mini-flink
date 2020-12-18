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

package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;

import java.io.Serializable;
import java.util.Map;

/**
 * A RuntimeContext contains information about the context in which functions are executed. Each parallel instance
 * of the function will have a context through which it can access static contextual information (such as
 * the current parallelism) and other constructs like accumulators and broadcast variables.
 *
 * <p>A function can, during runtime, obtain the RuntimeContext via a call to
 * {@link AbstractRichFunction#getRuntimeContext()}.
 */
@Public
public interface RuntimeContext {

	/**
	 * Returns the name of the task in which the UDF runs, as assigned during plan construction.
	 *
	 * @return The name of the task in which the UDF runs.
	 */
	String getTaskName();

	/**
	 * Gets the parallelism with which the parallel task runs.
	 *
	 * @return The parallelism with which the parallel task runs.
	 */
	int getNumberOfParallelSubtasks();

	/**
	 * Gets the number of max-parallelism with which the parallel task runs.
	 *
	 * @return The max-parallelism with which the parallel task runs.
	 */
	@PublicEvolving
	int getMaxNumberOfParallelSubtasks();

	/**
	 * Gets the number of this parallel subtask. The numbering starts from 0 and goes up to
	 * parallelism-1 (parallelism as returned by {@link #getNumberOfParallelSubtasks()}).
	 *
	 * @return The index of the parallel subtask.
	 */
	int getIndexOfThisSubtask();

	/**
	 * Gets the attempt number of this parallel subtask. First attempt is numbered 0.
	 *
	 * @return Attempt number of the subtask.
	 */
	int getAttemptNumber();

	/**
	 * Returns the name of the task, appended with the subtask indicator, such as "MyTask (3/6)",
	 * where 3 would be ({@link #getIndexOfThisSubtask()} + 1), and 6 would be
	 * {@link #getNumberOfParallelSubtasks()}.
	 *
	 * @return The name of the task, with subtask indicator.
	 */
	String getTaskNameWithSubtasks();

	/**
	 * Returns the {@link org.apache.flink.api.common.ExecutionConfig} for the currently executing
	 * job.
	 */
	ExecutionConfig getExecutionConfig();

	/**
	 * Gets the ClassLoader to load classes that are not in system's classpath, but are part of the
	 * jar file of a user job.
	 *
	 * @return The ClassLoader for user code classes.
	 */
	ClassLoader getUserCodeClassLoader();

	// --------------------------------------------------------------------------------------------

	/**
	 * Add this accumulator. Throws an exception if the accumulator already exists in the same Task.
	 * Note that the Accumulator name must have an unique name across the Flink job. Otherwise you will
	 * get an error when incompatible accumulators from different Tasks are combined at the JobManager
	 * upon job completion.
	 */
	<V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator);

	/**
	 * Get an existing accumulator object. The accumulator must have been added
	 * previously in this local runtime context.
	 *
	 * Throws an exception if the accumulator does not exist or if the
	 * accumulator exists, but with different type.
	 */
	<V, A extends Serializable> Accumulator<V, A> getAccumulator(String name);

	/**
	 * Returns a map of all registered accumulators for this task.
	 * The returned map must not be modified.
	 * @deprecated Use getAccumulator(..) to obtain the value of an accumulator.
	 */
	@Deprecated
	@PublicEvolving
	Map<String, Accumulator<?, ?>> getAllAccumulators();

	/**
	 * Convenience function to create a counter object for integers.
	 */
	@PublicEvolving
	IntCounter getIntCounter(String name);

	/**
	 * Convenience function to create a counter object for longs.
	 */
	@PublicEvolving
	LongCounter getLongCounter(String name);

	/**
	 * Convenience function to create a counter object for doubles.
	 */
	@PublicEvolving
	DoubleCounter getDoubleCounter(String name);

	// --------------------------------------------------------------------------------------------


	// ------------------------------------------------------------------------
	//  Methods for accessing state
	// ------------------------------------------------------------------------

	/**
	 * Gets a handle to the system's key/value state. The key/value state is only accessible
	 * if the function is executed on a KeyedStream. On each access, the state exposes the value
	 * for the key of the element currently processed by the function.
	 * Each function may have multiple partitioned states, addressed with different names.
	 *
	 * <p>Because the scope of each value is the key of the currently processed element,
	 * and the elements are distributed by the Flink runtime, the system can transparently
	 * scale out and redistribute the state and KeyedStream.
	 *
	 * <p>The following code example shows how to implement a continuous counter that counts
	 * how many times elements of a certain key occur, and emits an updated count for that
	 * element on each occurrence.
	 *
	 * <pre>{@code
	 * DataStream<MyType> stream = ...;
	 * KeyedStream<MyType> keyedStream = stream.keyBy("id");
	 *
	 * keyedStream.map(new RichMapFunction<MyType, Tuple2<MyType, Long>>() {
	 *
	 *     private ValueState<Long> state;
	 *
	 *     public void open(Configuration cfg) {
	 *         state = getRuntimeContext().getState(
	 *                 new ValueStateDescriptor<Long>("count", LongSerializer.INSTANCE, 0L));
	 *     }
	 *
	 *     public Tuple2<MyType, Long> map(MyType value) {
	 *         long count = state.value() + 1;
	 *         state.update(count);
	 *         return new Tuple2<>(value, count);
	 *     }
	 * });
	 * }</pre>
	 *
	 * @param stateProperties The descriptor defining the properties of the stats.
	 *
	 * @param <T> The type of value stored in the state.
	 *
	 * @return The partitioned state object.
	 *
	 * @throws UnsupportedOperationException Thrown, if no partitioned state is available for the
	 *                                       function (function is not part of a KeyedStream).
	 */
	@PublicEvolving
	<T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties);

}
