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
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;
import org.apache.flink.shaded.netty4.io.netty.buffer.ByteBuf;
import org.apache.flink.util.Preconditions;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

import static java.lang.Math.min;

interface ChannelStateSerializer {

	void readHeader(InputStream stream) throws IOException;

	byte[] extractAndMerge(byte[] bytes, List<Long> offsets) throws IOException;

	long getHeaderLength();
}

/**
 * Wrapper around various buffers to receive channel state data.
 */
@Internal
@NotThreadSafe
interface ChannelStateByteBuffer {

	boolean isWritable();

	/**
	 * Read up to <code>bytesToRead</code> bytes into this buffer from the given {@link InputStream}.
	 * @return     the total number of bytes read into this buffer.
	 */
	int writeBytes(InputStream input, int bytesToRead) throws IOException;

	static ChannelStateByteBuffer wrap(Buffer buffer) {
		return new ChannelStateByteBuffer() {

			private final ByteBuf byteBuf = buffer.asByteBuf();

			@Override
			public boolean isWritable() {
				return byteBuf.isWritable();
			}

			@Override
			public int writeBytes(InputStream input, int bytesToRead) throws IOException {
				return byteBuf.writeBytes(input, Math.min(bytesToRead, byteBuf.writableBytes()));
			}
		};
	}

	static ChannelStateByteBuffer wrap(BufferBuilder bufferBuilder) {
		final byte[] buf = new byte[1024];
		return new ChannelStateByteBuffer() {
			@Override
			public boolean isWritable() {
				return !bufferBuilder.isFull();
			}

			@Override
			public int writeBytes(InputStream input, int bytesToRead) throws IOException {
				int left = bytesToRead;
				for (int toRead = getToRead(left); toRead > 0; toRead = getToRead(left)) {
					int read = input.read(buf, 0, toRead);
					int copied = bufferBuilder.append(java.nio.ByteBuffer.wrap(buf, 0, read));
					Preconditions.checkState(copied == read);
					left -= read;
				}
				bufferBuilder.commit();
				return bytesToRead - left;
			}

			private int getToRead(int bytesToRead) {
				return min(bytesToRead, min(buf.length, bufferBuilder.getWritableBytes()));
			}
		};
	}

	static ChannelStateByteBuffer wrap(byte[] bytes) {
		return new ChannelStateByteBuffer() {
			private int written = 0;

			@Override
			public boolean isWritable() {
				return written < bytes.length;
			}

			@Override
			public int writeBytes(InputStream input, int bytesToRead) throws IOException {
				final int bytesRead = input.read(bytes, written, bytes.length - written);
				written += bytesRead;
				return bytesRead;
			}
		};
	}
}

