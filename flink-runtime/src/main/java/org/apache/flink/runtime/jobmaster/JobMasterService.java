package org.apache.flink.runtime.jobmaster;

import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.util.AutoCloseableAsync;

import java.util.concurrent.CompletableFuture;

public interface JobMasterService extends AutoCloseableAsync {

	CompletableFuture<Acknowledge> start(JobMasterId jobMasterId) throws Exception;

	CompletableFuture<Acknowledge> suspend(Exception cause);

	JobMasterGateway getGateway();

	String getAddress();
}
