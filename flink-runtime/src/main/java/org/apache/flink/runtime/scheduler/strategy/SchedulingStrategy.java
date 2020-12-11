package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import java.util.Set;

public interface SchedulingStrategy {

	void startScheduling();

	void restartTasks(Set<ExecutionVertexID> verticesToRestart);

	void onExecutionStateChange(ExecutionVertexID executionVertexId, ExecutionState executionState);

	void onPartitionConsumable(IntermediateResultPartitionID resultPartitionId);
}
