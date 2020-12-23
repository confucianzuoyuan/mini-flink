package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;

@Internal
public interface ConfigurableStateBackend {
	StateBackend configure(ReadableConfig config, ClassLoader classLoader) throws IllegalConfigurationException;
}
