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

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.InvalidProgramException;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamGraphGenerator;
import org.apache.flink.streaming.api.operators.StreamGroupedReduce;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

@Public
public class KeyedStream<T, KEY> extends DataStream<T> {

	/**
	 * The key selector that can get the key by which the stream if partitioned from the elements.
	 */
	private final KeySelector<T, KEY> keySelector;

	/** The type of the key by which the stream is partitioned. */
	private final TypeInformation<KEY> keyType;

	/**
	 * Creates a new {@link KeyedStream} using the given {@link KeySelector}
	 * to partition operator state by key.
	 *
	 * @param dataStream
	 *            Base stream of data
	 * @param keySelector
	 *            Function for determining state partitions
	 */
	public KeyedStream(DataStream<T> dataStream, KeySelector<T, KEY> keySelector) {
		this(dataStream, keySelector, TypeExtractor.getKeySelectorTypes(keySelector, dataStream.getType()));
	}

	/**
	 * Creates a new {@link KeyedStream} using the given {@link KeySelector}
	 * to partition operator state by key.
	 *
	 * @param dataStream
	 *            Base stream of data
	 * @param keySelector
	 *            Function for determining state partitions
	 */
	public KeyedStream(DataStream<T> dataStream, KeySelector<T, KEY> keySelector, TypeInformation<KEY> keyType) {
		this(
			dataStream,
			new PartitionTransformation<>(
				dataStream.getTransformation(),
				new KeyGroupStreamPartitioner<>(keySelector, StreamGraphGenerator.DEFAULT_LOWER_BOUND_MAX_PARALLELISM)),
			keySelector,
			keyType);
	}

	/**
	 * Creates a new {@link KeyedStream} using the given {@link KeySelector} and {@link TypeInformation}
	 * to partition operator state by key, where the partitioning is defined by a {@link PartitionTransformation}.
	 *
	 * @param stream
	 *            Base stream of data
	 * @param partitionTransformation
	 *            Function that determines how the keys are distributed to downstream operator(s)
	 * @param keySelector
	 *            Function to extract keys from the base stream
	 * @param keyType
	 *            Defines the type of the extracted keys
	 */
	@Internal
	KeyedStream(
		DataStream<T> stream,
		PartitionTransformation<T> partitionTransformation,
		KeySelector<T, KEY> keySelector,
		TypeInformation<KEY> keyType) {

		super(stream.getExecutionEnvironment(), partitionTransformation);
		this.keySelector = clean(keySelector);
		this.keyType = validateKeyType(keyType);
	}

	/**
	 * Validates that a given type of element (as encoded by the provided {@link TypeInformation}) can be
	 * used as a key in the {@code DataStream.keyBy()} operation. This is done by searching depth-first the
	 * key type and checking if each of the composite types satisfies the required conditions
	 * (see {@link #validateKeyTypeIsHashable(TypeInformation)}).
	 *
	 * @param keyType The {@link TypeInformation} of the key.
	 */
	private TypeInformation<KEY> validateKeyType(TypeInformation<KEY> keyType) {
		Stack<TypeInformation<?>> stack = new Stack<>();
		stack.push(keyType);

		List<TypeInformation<?>> unsupportedTypes = new ArrayList<>();

		while (!stack.isEmpty()) {
			TypeInformation<?> typeInfo = stack.pop();

			if (!validateKeyTypeIsHashable(typeInfo)) {
				unsupportedTypes.add(typeInfo);
			}

			if (typeInfo instanceof TupleTypeInfoBase) {
				for (int i = 0; i < typeInfo.getArity(); i++) {
					stack.push(((TupleTypeInfoBase) typeInfo).getTypeAt(i));
				}
			}
		}

		if (!unsupportedTypes.isEmpty()) {
			throw new InvalidProgramException("Type " + keyType + " cannot be used as key. Contained " +
					"UNSUPPORTED key types: " + StringUtils.join(unsupportedTypes, ", ") + ". Look " +
					"at the keyBy() documentation for the conditions a type has to satisfy in order to be " +
					"eligible for a key.");
		}

		return keyType;
	}

	private boolean validateKeyTypeIsHashable(TypeInformation<?> type) {
		try {
			return !type.getTypeClass().getMethod("hashCode").getDeclaringClass().equals(Object.class);
		} catch (NoSuchMethodException ignored) {
			// this should never happen as we are just searching for the hashCode() method.
		}
		return false;
	}

	// ------------------------------------------------------------------------
	//  properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the key selector that can get the key by which the stream if partitioned from the elements.
	 * @return The key selector for the key.
	 */
	@Internal
	public KeySelector<T, KEY> getKeySelector() {
		return this.keySelector;
	}

	/**
	 * Gets the type of the key by which the stream is partitioned.
	 * @return The type of the key by which the stream is partitioned.
	 */
	@Internal
	public TypeInformation<KEY> getKeyType() {
		return keyType;
	}

	// ------------------------------------------------------------------------
	//  basic transformations
	// ------------------------------------------------------------------------

	@Override
	protected <R> SingleOutputStreamOperator<R> doTransform(
			final String operatorName,
			final TypeInformation<R> outTypeInfo,
			final StreamOperatorFactory<R> operatorFactory) {

		SingleOutputStreamOperator<R> returnStream = super.doTransform(operatorName, outTypeInfo, operatorFactory);

		// inject the key selector and key type
		OneInputTransformation<T, R> transform = (OneInputTransformation<T, R>) returnStream.getTransformation();
		transform.setStateKeySelector(keySelector);
		transform.setStateKeyType(keyType);

		return returnStream;
	}

	@Override
	public DataStreamSink<T> addSink(SinkFunction<T> sinkFunction) {
		DataStreamSink<T> result = super.addSink(sinkFunction);
		result.getTransformation().setStateKeySelector(keySelector);
		result.getTransformation().setStateKeyType(keyType);
		return result;
	}

	public SingleOutputStreamOperator<T> reduce(ReduceFunction<T> reducer) {
		return transform("Keyed Reduce", getType(), new StreamGroupedReduce<T>(
				clean(reducer), getType().createSerializer(getExecutionConfig())));
	}
}
