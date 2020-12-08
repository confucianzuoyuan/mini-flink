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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;

/**
 * Basic interface for the timestamp extraction and watermark generation logic for the
 * {@link org.apache.flink.api.connector.source.SourceReader}.
 *
 * <p>Implementations of this class may or may not actually perform certain tasks, like watermark
 * generation. For example, the batch-oriented implementation typically skips all watermark generation
 * logic.
 *
 * @param <T> The type of the emitted records.
 */
@Internal
public interface TimestampsAndWatermarks<T> {

	/**
	 * Creates the ReaderOutput for the source reader, than internally runs the timestamp extraction and
	 * watermark generation.
	 */
	ReaderOutput<T> createMainOutput(PushingAsyncDataInput.DataOutput<T> output);

}
