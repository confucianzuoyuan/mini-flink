package org.apache.flink.api.common.functions;

import org.apache.flink.annotation.Public;
import org.apache.flink.configuration.Configuration;

@Public
public interface RichFunction extends Function {

	void open(Configuration parameters) throws Exception;

	void close() throws Exception;

	RuntimeContext getRuntimeContext();

	void setRuntimeContext(RuntimeContext t);
}
