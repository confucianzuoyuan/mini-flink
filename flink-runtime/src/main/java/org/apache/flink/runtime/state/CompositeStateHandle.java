package org.apache.flink.runtime.state;

public interface CompositeStateHandle extends StateObject {
	void registerSharedStates(SharedStateRegistry stateRegistry);
}
