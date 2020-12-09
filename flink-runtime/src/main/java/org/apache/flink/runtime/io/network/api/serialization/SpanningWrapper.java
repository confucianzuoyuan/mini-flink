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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.fs.RefCountedFile;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.StringUtils;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.Random;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.apache.flink.runtime.io.network.api.serialization.SpillingAdaptiveSpanningRecordDeserializer.LENGTH_BYTES;
import static org.apache.flink.util.IOUtils.closeQuietly;

final class SpanningWrapper {

	private static final int DEFAULT_THRESHOLD_FOR_SPILLING = 5 * 1024 * 1024; // 5 MiBytes
	private static final int DEFAULT_FILE_BUFFER_SIZE = 2 * 1024 * 1024;

	private final byte[] initialBuffer = new byte[1024];

	private final Random rnd = new Random();

	private final DataInputDeserializer serializationReadBuffer;

	final ByteBuffer lengthBuffer;

	private final int fileBufferSize;

	private FileChannel spillingChannel;

	private byte[] buffer;

	private int recordLength;

	private int accumulatedRecordBytes;

	private MemorySegment leftOverData;

	private int leftOverStart;

	private int leftOverLimit;

	private RefCountedFile spillFile;

	private DataInputViewStreamWrapper spillFileReader;

	private int thresholdForSpilling;

	SpanningWrapper() {
		this(DEFAULT_THRESHOLD_FOR_SPILLING, DEFAULT_FILE_BUFFER_SIZE);
	}

	SpanningWrapper(int threshold, int fileBufferSize) {
		this.lengthBuffer = ByteBuffer.allocate(LENGTH_BYTES);
		this.lengthBuffer.order(ByteOrder.BIG_ENDIAN);
		this.recordLength = -1;
		this.serializationReadBuffer = new DataInputDeserializer();
		this.buffer = initialBuffer;
		this.thresholdForSpilling = threshold;
		this.fileBufferSize = fileBufferSize;
	}

	/**
	 * Copies the data and transfers the "ownership" (i.e. clears the passed wrapper).
	 */
	void transferFrom(NonSpanningWrapper partial, int nextRecordLength) throws IOException {
		updateLength(nextRecordLength);
		accumulatedRecordBytes = isAboveSpillingThreshold() ? spill(partial) : partial.copyContentTo(buffer);
		partial.clear();
	}

	private boolean isAboveSpillingThreshold() {
		return recordLength > thresholdForSpilling;
	}

	void addNextChunkFromMemorySegment(MemorySegment segment, int offset, int numBytes) throws IOException {
		int limit = offset + numBytes;
		int numBytesRead = isReadingLength() ? readLength(segment, offset, numBytes) : 0;
		offset += numBytesRead;
		numBytes -= numBytesRead;
		if (numBytes == 0) {
			return;
		}

		int toCopy = min(recordLength - accumulatedRecordBytes, numBytes);
		if (toCopy > 0) {
			copyFromSegment(segment, offset, toCopy);
		}
		if (numBytes > toCopy) {
			leftOverData = segment;
			leftOverStart = offset + toCopy;
			leftOverLimit = limit;
		}
	}

	private void copyFromSegment(MemorySegment segment, int offset, int length) throws IOException {
		if (spillingChannel == null) {
			copyIntoBuffer(segment, offset, length);
		} else {
			copyIntoFile(segment, offset, length);
		}
	}

	private void copyIntoFile(MemorySegment segment, int offset, int length) throws IOException {
		accumulatedRecordBytes += length;
		if (hasFullRecord()) {
			spillingChannel.close();
			spillFileReader = new DataInputViewStreamWrapper(new BufferedInputStream(new FileInputStream(spillFile.getFile()), fileBufferSize));
		}
	}

	private void copyIntoBuffer(MemorySegment segment, int offset, int length) {
		segment.get(offset, buffer, accumulatedRecordBytes, length);
		accumulatedRecordBytes += length;
		if (hasFullRecord()) {
			serializationReadBuffer.setBuffer(buffer, 0, recordLength);
		}
	}

	private int readLength(MemorySegment segment, int segmentPosition, int segmentRemaining) throws IOException {
		int bytesToRead = min(lengthBuffer.remaining(), segmentRemaining);
		segment.get(segmentPosition, lengthBuffer, bytesToRead);
		if (!lengthBuffer.hasRemaining()) {
			updateLength(lengthBuffer.getInt(0));
		}
		return bytesToRead;
	}

	private void updateLength(int length) throws IOException {
		lengthBuffer.clear();
		recordLength = length;
		if (isAboveSpillingThreshold()) {
			spillingChannel = createSpillingChannel();
		} else {
			ensureBufferCapacity(length);
		}
	}

	/**
	 * Copies the leftover data and transfers the "ownership" (i.e. clears this wrapper).
	 */
	void transferLeftOverTo(NonSpanningWrapper nonSpanningWrapper) {
		nonSpanningWrapper.clear();
		if (leftOverData != null) {
			nonSpanningWrapper.initializeFromMemorySegment(leftOverData, leftOverStart, leftOverLimit);
		}
		clear();
	}

	boolean hasFullRecord() {
		return recordLength >= 0 && accumulatedRecordBytes >= recordLength;
	}

	int getNumGatheredBytes() {
		return accumulatedRecordBytes + (recordLength >= 0 ? LENGTH_BYTES : lengthBuffer.position());
	}

	public void clear() {
		buffer = initialBuffer;
		serializationReadBuffer.releaseArrays();

		recordLength = -1;
		lengthBuffer.clear();
		leftOverData = null;
		leftOverStart = 0;
		leftOverLimit = 0;
		accumulatedRecordBytes = 0;

		if (spillingChannel != null) {
			closeQuietly(spillingChannel);
		}
		if (spillFileReader != null) {
			closeQuietly(spillFileReader);
		}
		if (spillFile != null) {
			// It's important to avoid AtomicInteger access inside `release()` on the hot path
			closeQuietly(() -> spillFile.release());
		}

		spillingChannel = null;
		spillFileReader = null;
		spillFile = null;
	}

	public DataInputView getInputView() {
		return spillFileReader == null ? serializationReadBuffer : spillFileReader;
	}

	private void ensureBufferCapacity(int minLength) {
		if (buffer.length < minLength) {
			byte[] newBuffer = new byte[max(minLength, buffer.length * 2)];
			System.arraycopy(buffer, 0, newBuffer, 0, accumulatedRecordBytes);
			buffer = newBuffer;
		}
	}

	@SuppressWarnings("resource")
	private FileChannel createSpillingChannel() throws IOException {
		throw new IllegalStateException("Spilling file already exists.");
	}

	private static String randomString(Random random) {
		final byte[] bytes = new byte[20];
		random.nextBytes(bytes);
		return StringUtils.byteToHexString(bytes);
	}

	private int spill(NonSpanningWrapper partial) throws IOException {
		ByteBuffer buffer = partial.wrapIntoByteBuffer();
		int length = buffer.remaining();
		return length;
	}

	private boolean isReadingLength() {
		return lengthBuffer.position() > 0;
	}

	private static CloseableIterator<Buffer> toSingleBufferIterator(MemorySegment segment) {
		NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE, Buffer.DataType.DATA_BUFFER, segment.size());
		return CloseableIterator.ofElement(buffer, Buffer::recycleBuffer);
	}

}
