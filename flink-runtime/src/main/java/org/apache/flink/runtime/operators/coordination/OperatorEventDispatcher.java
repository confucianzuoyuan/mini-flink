package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.jobgraph.OperatorID;

public interface OperatorEventDispatcher {
	void registerEventHandler(OperatorID operator, OperatorEventHandler handler);
}
