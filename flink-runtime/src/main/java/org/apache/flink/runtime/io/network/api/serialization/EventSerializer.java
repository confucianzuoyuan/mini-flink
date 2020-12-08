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

package org.apache.flink.runtime.io.network.api.serialization;

import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.event.AbstractEvent;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EndOfSuperstepEvent;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferConsumer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.partition.consumer.EndOfChannelStateEvent;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility class to serialize and deserialize task events.
 */
public class EventSerializer {

	// ------------------------------------------------------------------------
	//  Constants
	// ------------------------------------------------------------------------

	private static final int END_OF_PARTITION_EVENT = 0;

	private static final int CHECKPOINT_BARRIER_EVENT = 1;

	private static final int END_OF_SUPERSTEP_EVENT = 2;

	private static final int OTHER_EVENT = 3;

	private static final int END_OF_CHANNEL_STATE_EVENT = 5;

	// ------------------------------------------------------------------------
	//  Serialization Logic
	// ------------------------------------------------------------------------

	public static ByteBuffer toSerializedEvent(AbstractEvent event) throws IOException {
		final Class<?> eventClass = event.getClass();
		if (eventClass == EndOfPartitionEvent.class) {
			return ByteBuffer.wrap(new byte[] { 0, 0, 0, END_OF_PARTITION_EVENT });
		}
		else if (eventClass == EndOfSuperstepEvent.class) {
			return ByteBuffer.wrap(new byte[] { 0, 0, 0, END_OF_SUPERSTEP_EVENT });
		}
		else if (eventClass == EndOfChannelStateEvent.class) {
			return ByteBuffer.wrap(new byte[] { 0, 0, 0, END_OF_CHANNEL_STATE_EVENT });
		}
		else {
			try {
				final DataOutputSerializer serializer = new DataOutputSerializer(128);
				serializer.writeInt(OTHER_EVENT);
				serializer.writeUTF(event.getClass().getName());
				event.write(serializer);
				return serializer.wrapAsByteBuffer();
			}
			catch (IOException e) {
				throw new IOException("Error while serializing event.", e);
			}
		}
	}

	/**
	 * Identifies whether the given buffer encodes the given event. Custom events are not supported.
	 *
	 * <p><strong>Pre-condition</strong>: This buffer must encode some event!</p>
	 *
	 * @param buffer the buffer to peak into
	 * @param eventClass the expected class of the event type
	 * @return whether the event class of the <tt>buffer</tt> matches the given <tt>eventClass</tt>
	 */
	private static boolean isEvent(ByteBuffer buffer, Class<?> eventClass) throws IOException {
		if (buffer.remaining() < 4) {
			throw new IOException("Incomplete event");
		}

		final int bufferPos = buffer.position();
		final ByteOrder bufferOrder = buffer.order();
		buffer.order(ByteOrder.BIG_ENDIAN);

		try {
			int type = buffer.getInt();

			if (eventClass.equals(EndOfPartitionEvent.class)) {
				return type == END_OF_PARTITION_EVENT;
			} else if (eventClass.equals(CheckpointBarrier.class)) {
				return type == CHECKPOINT_BARRIER_EVENT;
			} else if (eventClass.equals(EndOfSuperstepEvent.class)) {
				return type == END_OF_SUPERSTEP_EVENT;
			} else if (eventClass.equals(EndOfChannelStateEvent.class)) {
				return type == END_OF_CHANNEL_STATE_EVENT;
			} else {
				throw new UnsupportedOperationException("Unsupported eventClass = " + eventClass);
			}
		}
		finally {
			buffer.order(bufferOrder);
			// restore the original position in the buffer (recall: we only peak into it!)
			buffer.position(bufferPos);
		}
	}

	public static AbstractEvent fromSerializedEvent(ByteBuffer buffer, ClassLoader classLoader) throws IOException {
		if (buffer.remaining() < 4) {
			throw new IOException("Incomplete event");
		}

		final ByteOrder bufferOrder = buffer.order();
		buffer.order(ByteOrder.BIG_ENDIAN);

		try {
			final int type = buffer.getInt();

			if (type == END_OF_PARTITION_EVENT) {
				return EndOfPartitionEvent.INSTANCE;
			}
			else if (type == END_OF_SUPERSTEP_EVENT) {
				return EndOfSuperstepEvent.INSTANCE;
			}
			else if (type == END_OF_CHANNEL_STATE_EVENT) {
				return EndOfChannelStateEvent.INSTANCE;
			}
			else if (type == OTHER_EVENT) {
				try {
					final DataInputDeserializer deserializer = new DataInputDeserializer(buffer);
					final String className = deserializer.readUTF();

					final Class<? extends AbstractEvent> clazz;
					try {
						clazz = classLoader.loadClass(className).asSubclass(AbstractEvent.class);
					}
					catch (ClassNotFoundException e) {
						throw new IOException("Could not load event class '" + className + "'.", e);
					}
					catch (ClassCastException e) {
						throw new IOException("The class '" + className + "' is not a valid subclass of '"
								+ AbstractEvent.class.getName() + "'.", e);
					}

					final AbstractEvent event = InstantiationUtil.instantiate(clazz, AbstractEvent.class);
					event.read(deserializer);

					return event;
				}
				catch (Exception e) {
					throw new IOException("Error while deserializing or instantiating event.", e);
				}
			}
			else {
				throw new IOException("Corrupt byte stream for event");
			}
		}
		finally {
			buffer.order(bufferOrder);
		}
	}

	// ------------------------------------------------------------------------
	// Buffer helpers
	// ------------------------------------------------------------------------

	public static Buffer toBuffer(AbstractEvent event) throws IOException {
		final ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

		MemorySegment data = MemorySegmentFactory.wrap(serializedEvent.array());

		final Buffer buffer = new NetworkBuffer(data, FreeingBufferRecycler.INSTANCE, Buffer.DataType.getDataType(event));
		buffer.setSize(serializedEvent.remaining());

		return buffer;
	}

	public static BufferConsumer toBufferConsumer(AbstractEvent event) throws IOException {
		final ByteBuffer serializedEvent = EventSerializer.toSerializedEvent(event);

		MemorySegment data = MemorySegmentFactory.wrap(serializedEvent.array());

		return new BufferConsumer(data, FreeingBufferRecycler.INSTANCE, Buffer.DataType.getDataType(event));
	}

	public static AbstractEvent fromBuffer(Buffer buffer, ClassLoader classLoader) throws IOException {
		return fromSerializedEvent(buffer.getNioBufferReadable(), classLoader);
	}

	public static boolean isEvent(Buffer buffer, Class<?> eventClass) throws IOException {
		return !buffer.isBuffer() && isEvent(buffer.getNioBufferReadable(), eventClass);
	}
}
