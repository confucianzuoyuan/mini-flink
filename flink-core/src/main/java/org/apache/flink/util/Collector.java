package org.apache.flink.util;

import org.apache.flink.annotation.Public;

@Public
public interface Collector<T> {

	void collect(T record);

	void close();
}
