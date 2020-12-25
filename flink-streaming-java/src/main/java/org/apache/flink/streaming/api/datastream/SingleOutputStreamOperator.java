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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * {@code SingleOutputStreamOperator} represents a user defined transformation
 * applied on a {@link DataStream} with one predefined output type.
 *
 * @param <T> The type of the elements in this stream.
 */
@Public
public class SingleOutputStreamOperator<T> extends DataStream<T> {

	/** Indicate this is a non-parallel operator and cannot set a non-1 degree of parallelism. **/
	protected boolean nonParallel = false;

	protected SingleOutputStreamOperator(StreamExecutionEnvironment environment, Transformation<T> transformation) {
		super(environment, transformation);
	}

	/**
	 * Gets the name of the current data stream. This name is
	 * used by the visualization and logging during runtime.
	 *
	 * @return Name of the stream.
	 */
	public String getName() {
		return transformation.getName();
	}

	/**
	 * Sets the name of the current data stream. This name is
	 * used by the visualization and logging during runtime.
	 *
	 * @return The named operator.
	 */
	public SingleOutputStreamOperator<T> name(String name){
		transformation.setName(name);
		return this;
	}

	/**
	 * Sets the parallelism for this operator.
	 *
	 * @param parallelism
	 *            The parallelism for this operator.
	 * @return The operator with set parallelism.
	 */
	public SingleOutputStreamOperator<T> setParallelism(int parallelism) {
		transformation.setParallelism(parallelism);

		return this;
	}


	/**
	 * Sets the buffering timeout for data produced by this operation.
	 * The timeout defines how long data may linger in a partially full buffer
	 * before being sent over the network.
	 *
	 * <p>Lower timeouts lead to lower tail latencies, but may affect throughput.
	 * Timeouts of 1 ms still sustain high throughput, even for jobs with high parallelism.
	 *
	 * <p>A value of '-1' means that the default buffer timeout should be used. A value
	 * of '0' indicates that no buffering should happen, and all records/events should be
	 * immediately sent through the network, without additional buffering.
	 *
	 * @param timeoutMillis
	 *            The maximum time between two output flushes.
	 * @return The operator with buffer timeout set.
	 */
	public SingleOutputStreamOperator<T> setBufferTimeout(long timeoutMillis) {
		checkArgument(timeoutMillis >= -1, "timeout must be >= -1");
		transformation.setBufferTimeout(timeoutMillis);
		return this;
	}

	/**
	 * Sets the {@link ChainingStrategy} for the given operator affecting the
	 * way operators will possibly be co-located on the same thread for
	 * increased performance.
	 *
	 * @param strategy
	 *            The selected {@link ChainingStrategy}
	 * @return The operator with the modified chaining strategy
	 */
	@PublicEvolving
	private SingleOutputStreamOperator<T> setChainingStrategy(ChainingStrategy strategy) {
		if (transformation instanceof PhysicalTransformation) {
			((PhysicalTransformation<T>) transformation).setChainingStrategy(strategy);
		} else {
			throw new UnsupportedOperationException("Cannot set chaining strategy on " + transformation);
		}
		return this;
	}

}
