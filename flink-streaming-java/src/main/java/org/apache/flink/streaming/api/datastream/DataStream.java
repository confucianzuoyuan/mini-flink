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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.Utils;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.InputTypeConfigurable;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.*;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.util.Preconditions;

@Public
public class DataStream<T> {

	protected final StreamExecutionEnvironment environment;

	protected final Transformation<T> transformation;

	/**
	 * Create a new {@link DataStream} in the given execution environment with
	 * partitioning set to forward by default.
	 *
	 * @param environment The StreamExecutionEnvironment
	 */
	public DataStream(StreamExecutionEnvironment environment, Transformation<T> transformation) {
		this.environment = environment;
		this.transformation = transformation;
	}

	/**
	 * Returns the ID of the {@link DataStream} in the current {@link StreamExecutionEnvironment}.
	 *
	 * @return ID of the DataStream
	 */
	@Internal
	public int getId() {
		return transformation.getId();
	}

	/**
	 * Gets the parallelism for this operator.
	 *
	 * @return The parallelism set for this operator.
	 */
	public int getParallelism() {
		return transformation.getParallelism();
	}

	/**
	 * Gets the minimum resources for this operator.
	 *
	 * @return The minimum resources set for this operator.
	 */
	@PublicEvolving
	public ResourceSpec getMinResources() {
		return transformation.getMinResources();
	}

	/**
	 * Gets the preferred resources for this operator.
	 *
	 * @return The preferred resources set for this operator.
	 */
	@PublicEvolving
	public ResourceSpec getPreferredResources() {
		return transformation.getPreferredResources();
	}

	/**
	 * Gets the type of the stream.
	 *
	 * @return The type of the datastream.
	 */
	public TypeInformation<T> getType() {
		return transformation.getOutputType();
	}

	/**
	 * Invokes the {@link org.apache.flink.api.java.ClosureCleaner}
	 * on the given function if closure cleaning is enabled in the {@link ExecutionConfig}.
	 *
	 * @return The cleaned Function
	 */
	protected <F> F clean(F f) {
		return getExecutionEnvironment().clean(f);
	}

	/**
	 * Returns the {@link StreamExecutionEnvironment} that was used to create this
	 * {@link DataStream}.
	 *
	 * @return The Execution Environment
	 */
	public StreamExecutionEnvironment getExecutionEnvironment() {
		return environment;
	}

	public ExecutionConfig getExecutionConfig() {
		return environment.getConfig();
	}

	public <K> KeyedStream<T, K> keyBy(KeySelector<T, K> key) {
		Preconditions.checkNotNull(key);
		return new KeyedStream<>(this, clean(key));
	}

	public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper) {

		TypeInformation<R> outType = TypeExtractor.getMapReturnTypes(clean(mapper), getType(),
				Utils.getCallLocationName(), true);

		return map(mapper, outType);
	}

	public <R> SingleOutputStreamOperator<R> map(MapFunction<T, R> mapper, TypeInformation<R> outputType) {
		return transform("Map", outputType, new StreamMap<>(clean(mapper)));
	}

	public <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper) {

		TypeInformation<R> outType = TypeExtractor.getFlatMapReturnTypes(clean(flatMapper),
				getType(), Utils.getCallLocationName(), true);

		return flatMap(flatMapper, outType);
	}

	public <R> SingleOutputStreamOperator<R> flatMap(FlatMapFunction<T, R> flatMapper, TypeInformation<R> outputType) {
		return transform("Flat Map", outputType, new StreamFlatMap<>(clean(flatMapper)));
	}

	public SingleOutputStreamOperator<T> filter(FilterFunction<T> filter) {
		return transform("Filter", getType(), new StreamFilter<>(clean(filter)));
	}



	// ------------------------------------------------------------------------
	//  Data sinks
	// ------------------------------------------------------------------------

	/**
	 * Writes a DataStream to the standard output stream (stdout).
	 *
	 * <p>For each element of the DataStream the result of {@link Object#toString()} is written.
	 *
	 * <p>NOTE: This will print to stdout on the machine where the code is executed, i.e. the Flink
	 * worker.
	 *
	 * @return The closed DataStream.
	 */
	@PublicEvolving
	public DataStreamSink<T> print() {
		PrintSinkFunction<T> printFunction = new PrintSinkFunction<>();
		return addSink(printFunction).name("Print to Std. Out");
	}

	@PublicEvolving
	public <R> SingleOutputStreamOperator<R> transform(
			String operatorName,
			TypeInformation<R> outTypeInfo,
			OneInputStreamOperator<T, R> operator) {

		return doTransform(operatorName, outTypeInfo, SimpleOperatorFactory.of(operator));
	}

	protected <R> SingleOutputStreamOperator<R> doTransform(
			String operatorName,
			TypeInformation<R> outTypeInfo,
			StreamOperatorFactory<R> operatorFactory) {

		// read the output type of the input Transform to coax out errors about MissingTypeInfo
		transformation.getOutputType();

		OneInputTransformation<T, R> resultTransform = new OneInputTransformation<>(
				this.transformation,
				operatorName,
				operatorFactory,
				outTypeInfo,
				environment.getParallelism());

		@SuppressWarnings({"unchecked", "rawtypes"})
		SingleOutputStreamOperator<R> returnStream = new SingleOutputStreamOperator(environment, resultTransform);

		getExecutionEnvironment().addOperator(resultTransform);

		return returnStream;
	}

	/**
	 * Adds the given sink to this DataStream. Only streams with sinks added
	 * will be executed once the {@link StreamExecutionEnvironment#execute()}
	 * method is called.
	 *
	 * @param sinkFunction
	 *            The object containing the sink's invoke function.
	 * @return The closed DataStream.
	 */
	public DataStreamSink<T> addSink(SinkFunction<T> sinkFunction) {

		// read the output type of the input Transform to coax out errors about MissingTypeInfo
		transformation.getOutputType();

		// configure the type if needed
		if (sinkFunction instanceof InputTypeConfigurable) {
			((InputTypeConfigurable) sinkFunction).setInputType(getType(), getExecutionConfig());
		}

		StreamSink<T> sinkOperator = new StreamSink<>(clean(sinkFunction));

		DataStreamSink<T> sink = new DataStreamSink<>(this, sinkOperator);

		getExecutionEnvironment().addOperator(sink.getTransformation());
		return sink;
	}

	/**
	 * Returns the {@link Transformation} that represents the operation that logically creates
	 * this {@link DataStream}.
	 *
	 * @return The Transformation
	 */
	@Internal
	public Transformation<T> getTransformation() {
		return transformation;
	}
}
