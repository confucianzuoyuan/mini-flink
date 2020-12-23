package org.apache.flink.runtime.state;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.state.memory.MemoryStateBackendFactory;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;

public class StateBackendLoader {

	public static StateBackend fromApplicationOrConfigOrDefault(
			@Nullable StateBackend fromApplication,
			Configuration config,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {
		return new MemoryStateBackendFactory().createFromConfig(config, classLoader);
	}

	private StateBackendLoader() {}
}
