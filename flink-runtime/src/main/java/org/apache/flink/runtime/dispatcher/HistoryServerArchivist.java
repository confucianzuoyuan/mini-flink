package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;

import java.util.concurrent.Executor;

public interface HistoryServerArchivist {
	static HistoryServerArchivist createHistoryServerArchivist(Configuration configuration, Executor ioExecutor) {
		return VoidHistoryServerArchivist.INSTANCE;
	}
}
