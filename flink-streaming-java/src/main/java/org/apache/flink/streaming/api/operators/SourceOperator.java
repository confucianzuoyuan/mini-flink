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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.source.TimestampsAndWatermarks;
import org.apache.flink.streaming.runtime.io.PushingAsyncDataInput;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.CollectionUtil;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base source operator only used for integrating the source reader which is proposed by FLIP-27. It implements
 * the interface of {@link PushingAsyncDataInput} for naturally compatible with one input processing in runtime
 * stack.
 *
 * <p><b>Important Note on Serialization:</b> The SourceOperator inherits the {@link java.io.Serializable}
 * interface from the StreamOperator, but is in fact NOT serializable. The operator must only be instantiates
 * in the StreamTask from its factory.
 *
 * @param <OUT> The output type of the operator.
 */
@Internal
@SuppressWarnings("serial")
public class SourceOperator<OUT, SplitT extends SourceSplit>
		extends AbstractStreamOperator<OUT>
		implements OperatorEventHandler, PushingAsyncDataInput<OUT> {
	private static final long serialVersionUID = 1405537676017904695L;

	// Package private for unit test.
	static final ListStateDescriptor<byte[]> SPLITS_STATE_DESC =
			new ListStateDescriptor<>("SourceReaderState", BytePrimitiveArraySerializer.INSTANCE);

	/** The factory for the source reader. This is a workaround, because currently the SourceReader
	 * must be lazily initialized, which is mainly because the metrics groups that the reader relies on is
	 * lazily initialized. */
	private final Function<SourceReaderContext, SourceReader<OUT, SplitT>> readerFactory;

	/** The serializer for the splits, applied to the split types before storing them in the reader state. */
	private final SimpleVersionedSerializer<SplitT> splitSerializer;

	/** The factory for timestamps and watermark generators. */
	private final WatermarkStrategy<OUT> watermarkStrategy;

	// ---- lazily initialized fields (these fields are the "hot" fields) ----

	/** The source reader that does most of the work. */
	private SourceReader<OUT, SplitT> sourceReader;

	private ReaderOutput<OUT> currentMainOutput;

	private DataOutput<OUT> lastInvokedOutput;

	/** The state that holds the currently assigned splits. */
	private ListState<SplitT> readerState;

	/** The event time and watermarking logic. Ideally this would be eagerly passed into this operator,
	 * but we currently need to instantiate this lazily, because the metric groups exist only later. */
	private TimestampsAndWatermarks<OUT> eventTimeLogic;

	public SourceOperator(
			Function<SourceReaderContext, SourceReader<OUT, SplitT>> readerFactory,
			SimpleVersionedSerializer<SplitT> splitSerializer,
			WatermarkStrategy<OUT> watermarkStrategy,
			ProcessingTimeService timeService) {

		this.readerFactory = checkNotNull(readerFactory);
		this.splitSerializer = checkNotNull(splitSerializer);
		this.watermarkStrategy = checkNotNull(watermarkStrategy);
		this.processingTimeService = timeService;
	}

	@Override
	public void open() throws Exception {
		final SourceReaderContext context = new SourceReaderContext() {
			@Override
			public void sendSourceEventToCoordinator(SourceEvent event) {
			}
		};

		sourceReader = readerFactory.apply(context);

		// restore the state if necessary.
		final List<SplitT> splits = CollectionUtil.iterableToList(readerState.get());
		if (!splits.isEmpty()) {
			sourceReader.addSplits(splits);
		}

		// Start the reader.
		sourceReader.start();
		// Register the reader to the coordinator.
		registerReader();

	}

	@Override
	public void close() throws Exception {
		super.close();
	}

	@Override
	public InputStatus emitNext(DataOutput<OUT> output) throws Exception {
		// guarding an assumptions we currently make due to the fact that certain classes
		// assume a constant output
		assert lastInvokedOutput == output || lastInvokedOutput == null;

		// short circuit the common case (every invocation except the first)
		if (currentMainOutput != null) {
			return sourceReader.pollNext(currentMainOutput);
		}

		// this creates a batch or streaming output based on the runtime mode
		currentMainOutput = eventTimeLogic.createMainOutput(output);
		lastInvokedOutput = output;
		return sourceReader.pollNext(currentMainOutput);
	}

	@Override
	public CompletableFuture<?> getAvailableFuture() {
		return sourceReader.isAvailable();
	}

	private void registerReader() {

	}

	// --------------- methods for unit tests ------------

	@VisibleForTesting
	public SourceReader<OUT, SplitT> getSourceReader() {
		return sourceReader;
	}

	@VisibleForTesting
	ListState<SplitT> getReaderState() {
		return readerState;
	}
}
