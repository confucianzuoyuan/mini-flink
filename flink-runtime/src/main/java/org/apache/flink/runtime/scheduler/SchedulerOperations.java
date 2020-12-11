package org.apache.flink.runtime.scheduler;

import java.util.List;

public interface SchedulerOperations {
	void allocateSlotsAndDeploy(List<ExecutionVertexDeploymentOption> executionVertexDeploymentOptions);
}
