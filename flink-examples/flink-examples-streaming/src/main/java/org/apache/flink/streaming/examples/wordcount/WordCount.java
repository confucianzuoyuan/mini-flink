package org.apache.flink.streaming.examples.wordcount;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setParallelism(1);
		// get input data
		DataStream<String> text = env.fromElements("hello world", "hello world");

		DataStream<Tuple2<String, Integer>> counts = text
			.flatMap(new Tokenizer())
			.setParallelism(1)
			.filter(r -> r.f0.equals("hello"))
			.setParallelism(1)
			.keyBy(r -> r.f0)
			.reduce(
				new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
						return Tuple2.of(value1.f0, value1.f1 + value2.f1);
					}
				}
			)
			.setParallelism(2);

		counts.print();

		env.execute("Streaming WordCount");
	}

	public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\W+");

			// emit the pairs
			for (String token : tokens) {
				if (token.length() > 0) {
					out.collect(new Tuple2<>(token, 1));
				}
			}
		}
	}

}
