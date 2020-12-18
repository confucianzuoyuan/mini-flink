package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

@Internal
public class StreamGroupedReduce<IN> extends AbstractUdfStreamOperator<IN, ReduceFunction<IN>>
		implements OneInputStreamOperator<IN, IN> {

	private static final long serialVersionUID = 1L;

	private static final String STATE_NAME = "_op_state";

	private transient ValueState<IN> values;

	private TypeSerializer<IN> serializer;

	public StreamGroupedReduce(ReduceFunction<IN> reducer, TypeSerializer<IN> serializer) {
		super(reducer);
		this.serializer = serializer;
	}

	@Override
	public void open() throws Exception {
		super.open();
		ValueStateDescriptor<IN> stateId = new ValueStateDescriptor<>(STATE_NAME, serializer);
		values = getPartitionedState(stateId);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		IN value = element.getValue();
		IN currentValue = values.value();

		if (currentValue != null) {
//			new ReduceFunction<Tuple2<String, Integer>>() {
//				@Override
//				public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
//					return Tuple2.of(value1.f0, value1.f1 + value2.f1);
//				}
//			}
			IN reduced = userFunction.reduce(currentValue, value);
			System.out.println("reduced: " + reduced);
			values.update(reduced);
			output.collect(element.replace(reduced));
		} else {
			values.update(value);
			output.collect(element.replace(value));
		}
	}
}
