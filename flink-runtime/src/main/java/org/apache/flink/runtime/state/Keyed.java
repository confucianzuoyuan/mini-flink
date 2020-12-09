package org.apache.flink.runtime.state;

public interface Keyed<K> {
	K getKey();
}
