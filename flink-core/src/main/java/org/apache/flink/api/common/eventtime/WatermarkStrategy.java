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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.annotation.Public;

import java.time.Duration;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Public
public interface WatermarkStrategy<T> extends
		TimestampAssignerSupplier<T>, WatermarkGeneratorSupplier<T> {

	// ------------------------------------------------------------------------
	//  Methods that implementors need to implement.
	// ------------------------------------------------------------------------

	/**
	 * Instantiates a WatermarkGenerator that generates watermarks according to this strategy.
	 */
	@Override
	WatermarkGenerator<T> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context);

	/**
	 * Instantiates a {@link TimestampAssigner} for assigning timestamps according to this
	 * strategy.
	 */
	@Override
	default TimestampAssigner<T> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
		// By default, this is {@link RecordTimestampAssigner},
		// for cases where records come out of a source with valid timestamps, for example from Kafka.
		return new RecordTimestampAssigner<>();
	}

	// ------------------------------------------------------------------------
	//  Builder methods for enriching a base WatermarkStrategy
	// ------------------------------------------------------------------------

	/**
	 * Creates a new {@code WatermarkStrategy} that wraps this strategy but instead uses the given
	 * {@link TimestampAssigner} (via a {@link TimestampAssignerSupplier}).
	 *
	 * <p>You can use this when a {@link TimestampAssigner} needs additional context, for example
	 * access to the metrics system.
	 *
	 * <pre>
	 * {@code WatermarkStrategy<Object> wmStrategy = WatermarkStrategy
	 *   .forMonotonousTimestamps()
	 *   .withTimestampAssigner((ctx) -> new MetricsReportingAssigner(ctx));
	 * }</pre>
	 */
	default WatermarkStrategy<T> withTimestampAssigner(TimestampAssignerSupplier<T> timestampAssigner) {
		checkNotNull(timestampAssigner, "timestampAssigner");
		return new WatermarkStrategyWithTimestampAssigner<>(this, timestampAssigner);
	}

	/**
	 * Creates a new {@code WatermarkStrategy} that wraps this strategy but instead uses the given
	 * {@link SerializableTimestampAssigner}.
	 *
	 * <p>You can use this in case you want to specify a {@link TimestampAssigner} via a lambda
	 * function.
	 *
	 * <pre>
	 * {@code WatermarkStrategy<CustomObject> wmStrategy = WatermarkStrategy
	 *   .forMonotonousTimestamps()
	 *   .withTimestampAssigner((event, timestamp) -> event.getTimestamp());
	 * }</pre>
	 */
	default WatermarkStrategy<T> withTimestampAssigner(SerializableTimestampAssigner<T> timestampAssigner) {
		checkNotNull(timestampAssigner, "timestampAssigner");
		return new WatermarkStrategyWithTimestampAssigner<>(this,
				TimestampAssignerSupplier.of(timestampAssigner));
	}

	// ------------------------------------------------------------------------
	//  Convenience methods for common watermark strategies
	// ------------------------------------------------------------------------

	/**
	 * Creates a watermark strategy for situations with monotonously ascending timestamps.
	 *
	 * <p>The watermarks are generated periodically and tightly follow the latest
	 * timestamp in the data. The delay introduced by this strategy is mainly the periodic interval
	 * in which the watermarks are generated.
	 *
	 * @see AscendingTimestampsWatermarks
	 */
	static <T> WatermarkStrategy<T> forMonotonousTimestamps() {
		return (ctx) -> new AscendingTimestampsWatermarks<>();
	}

	/**
	 * Creates a watermark strategy for situations where records are out of order, but you can place
	 * an upper bound on how far the events are out of order. An out-of-order bound B means that
	 * once the an event with timestamp T was encountered, no events older than {@code T - B} will
	 * follow any more.
	 *
	 * <p>The watermarks are generated periodically. The delay introduced by this watermark
	 * strategy is the periodic interval length, plus the out of orderness bound.
	 *
	 * @see BoundedOutOfOrdernessWatermarks
	 */
	static <T> WatermarkStrategy<T> forBoundedOutOfOrderness(Duration maxOutOfOrderness) {
		return (ctx) -> new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness);
	}

	/**
	 * Creates a watermark strategy based on an existing {@link WatermarkGeneratorSupplier}.
	 */
	static <T> WatermarkStrategy<T> forGenerator(WatermarkGeneratorSupplier<T> generatorSupplier) {
		return generatorSupplier::createWatermarkGenerator;
	}

	/**
	 * Creates a watermark strategy that generates no watermarks at all. This may be useful in
	 * scenarios that do pure processing-time based stream processing.
	 */
	static <T> WatermarkStrategy<T> noWatermarks() {
		return (ctx) -> new NoWatermarksGenerator<>();
	}

}
