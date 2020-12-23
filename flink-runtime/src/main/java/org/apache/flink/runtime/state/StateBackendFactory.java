package org.apache.flink.runtime.state;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;

import java.io.IOException;

@PublicEvolving
public interface StateBackendFactory<T extends StateBackend> {
	T createFromConfig(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException, IOException;
}
