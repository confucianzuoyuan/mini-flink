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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.util.CloseableIterator;

import java.io.Closeable;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;

/**
 * Writes channel state during checkpoint/savepoint.
 */
@Internal
public interface ChannelStateWriter extends Closeable {

	/**
	 * Channel state write result.
	 */
	class ChannelStateWriteResult {
		final CompletableFuture<Collection<InputChannelStateHandle>> inputChannelStateHandles;
		final CompletableFuture<Collection<ResultSubpartitionStateHandle>> resultSubpartitionStateHandles;

		ChannelStateWriteResult() {
			this(new CompletableFuture<>(), new CompletableFuture<>());
		}

		ChannelStateWriteResult(
				CompletableFuture<Collection<InputChannelStateHandle>> inputChannelStateHandles,
				CompletableFuture<Collection<ResultSubpartitionStateHandle>> resultSubpartitionStateHandles) {
			this.inputChannelStateHandles = inputChannelStateHandles;
			this.resultSubpartitionStateHandles = resultSubpartitionStateHandles;
		}

		public CompletableFuture<Collection<InputChannelStateHandle>> getInputChannelStateHandles() {
			return inputChannelStateHandles;
		}

		public CompletableFuture<Collection<ResultSubpartitionStateHandle>> getResultSubpartitionStateHandles() {
			return resultSubpartitionStateHandles;
		}

		public static final ChannelStateWriteResult EMPTY = new ChannelStateWriteResult(
			CompletableFuture.completedFuture(Collections.emptyList()),
			CompletableFuture.completedFuture(Collections.emptyList())
		);

		public void fail(Throwable e) {
			inputChannelStateHandles.completeExceptionally(e);
			resultSubpartitionStateHandles.completeExceptionally(e);
		}

		boolean isDone() {
			return inputChannelStateHandles.isDone() && resultSubpartitionStateHandles.isDone();
		}
	}

	/**
	 * Sequence number for the buffers that were saved during the previous execution attempt; then restored; and now are
	 * to be saved again (as opposed to the buffers received from the upstream or from the operator).
	 */
	int SEQUENCE_NUMBER_RESTORED = -1;

	/**
	 * Signifies that buffer sequence number is unknown (e.g. if passing sequence numbers is not implemented).
	 */
	int SEQUENCE_NUMBER_UNKNOWN = -2;

	/**
	 * Initiate write of channel state for the given checkpoint id.
	 */
	void start(long checkpointId, CheckpointOptions checkpointOptions);


	void addInputData(long checkpointId, InputChannelInfo info, int startSeqNum, CloseableIterator<Buffer> data);

	void finishInput(long checkpointId);

	void finishOutput(long checkpointId);

	void abort(long checkpointId, Throwable cause, boolean cleanup);

	ChannelStateWriteResult getAndRemoveWriteResult(long checkpointId) throws IllegalArgumentException;
}
