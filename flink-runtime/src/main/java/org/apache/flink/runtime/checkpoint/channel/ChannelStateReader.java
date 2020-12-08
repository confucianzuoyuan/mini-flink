package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferBuilder;

import java.io.IOException;

@Internal
public interface ChannelStateReader extends AutoCloseable {

	enum ReadResult { HAS_MORE_DATA, NO_MORE_DATA }

	boolean hasChannelStates();

	ReadResult readInputData(InputChannelInfo info, Buffer buffer) throws IOException;

	ReadResult readOutputData(ResultSubpartitionInfo info, BufferBuilder bufferBuilder) throws IOException;

	@Override
	void close() throws Exception;

}
