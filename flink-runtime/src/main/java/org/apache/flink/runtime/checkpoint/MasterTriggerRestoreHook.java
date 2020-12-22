package org.apache.flink.runtime.checkpoint;

public interface MasterTriggerRestoreHook<T> {

	String getIdentifier();

	default void reset() throws Exception {
	}

	default void close() throws Exception {
	}

	interface Factory extends java.io.Serializable {
		<V> MasterTriggerRestoreHook<V> create();
	}
}
