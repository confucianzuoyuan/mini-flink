package org.apache.flink.runtime.state;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackendFactory;
import org.apache.flink.util.DynamicCodeLoadingException;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class StateBackendLoader {

	// ------------------------------------------------------------------------
	//  Configuration shortcut names
	// ------------------------------------------------------------------------

	/** The shortcut configuration name for the MemoryState backend that checkpoints to the JobManager */
	public static final String MEMORY_STATE_BACKEND_NAME = "jobmanager";

	/** The shortcut configuration name for the FileSystem State backend */
	public static final String FS_STATE_BACKEND_NAME = "filesystem";

	/** The shortcut configuration name for the RocksDB State Backend */
	public static final String ROCKSDB_STATE_BACKEND_NAME = "rocksdb";

	// ------------------------------------------------------------------------
	//  Loading the state backend from a configuration 
	// ------------------------------------------------------------------------

	public static StateBackend loadStateBackendFromConfig(
			ReadableConfig config,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {
		return null;
	}

	public static StateBackend fromApplicationOrConfigOrDefault(
			@Nullable StateBackend fromApplication,
			Configuration config,
			ClassLoader classLoader,
			@Nullable Logger logger) throws IllegalConfigurationException, DynamicCodeLoadingException, IOException {
		return new MemoryStateBackendFactory().createFromConfig(config, classLoader);
	}

	// ------------------------------------------------------------------------

	/** This class is not meant to be instantiated */
	private StateBackendLoader() {}
}
