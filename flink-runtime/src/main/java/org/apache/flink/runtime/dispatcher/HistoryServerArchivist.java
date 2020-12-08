package org.apache.flink.runtime.dispatcher;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.webmonitor.WebMonitorUtils;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

public interface HistoryServerArchivist {

	CompletableFuture<Acknowledge> archiveExecutionGraph(AccessExecutionGraph executionGraph);

	static HistoryServerArchivist createHistoryServerArchivist(Configuration configuration, Executor ioExecutor) {
		final String configuredArchivePath = configuration.getString(JobManagerOptions.ARCHIVE_DIR);

		if (configuredArchivePath != null) {
			final Path archivePath = WebMonitorUtils.validateAndNormalizeUri(new Path(configuredArchivePath).toUri());

			return new JsonResponseHistoryServerArchivist(archivePath, ioExecutor);
		} else {
			return VoidHistoryServerArchivist.INSTANCE;
		}
	}
}
