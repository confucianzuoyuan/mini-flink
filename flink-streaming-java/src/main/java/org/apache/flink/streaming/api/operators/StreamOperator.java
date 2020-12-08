package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Disposable;

import java.io.Serializable;

@PublicEvolving
public interface StreamOperator<OUT> extends KeyContext, Disposable, Serializable {

	// ------------------------------------------------------------------------
	//  life cycle
	// ------------------------------------------------------------------------

	void open() throws Exception;

	void close() throws Exception;

	@Override
	void dispose() throws Exception;

	// ------------------------------------------------------------------------
	//  state snapshots
	// ------------------------------------------------------------------------

	void prepareSnapshotPreBarrier(long checkpointId) throws Exception;

	/**
	 * Provides a context to initialize all state in the operator.
	 */
	void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception;

	// ------------------------------------------------------------------------
	//  miscellaneous
	// ------------------------------------------------------------------------

	void setKeyContextElement1(StreamRecord<?> record) throws Exception;

	void setKeyContextElement2(StreamRecord<?> record) throws Exception;

	OperatorID getOperatorID();
}
