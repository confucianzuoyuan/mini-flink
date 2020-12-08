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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.io.Serializable;

@Public
public interface SourceFunction<T> extends Function, Serializable {

	void run(SourceContext<T> ctx) throws Exception;

	void cancel();

	@Public // Interface might be extended in the future with additional methods.
	interface SourceContext<T> {

		void collect(T element);

		@PublicEvolving
		void collectWithTimestamp(T element, long timestamp);

		@PublicEvolving
		void emitWatermark(Watermark mark);

		@PublicEvolving
		void markAsTemporarilyIdle();

		Object getCheckpointLock();

		void close();
	}
}
