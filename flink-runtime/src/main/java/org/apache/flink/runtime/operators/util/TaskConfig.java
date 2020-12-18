package org.apache.flink.runtime.operators.util;

import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.io.Serializable;

/**
 * Configuration class which stores all relevant parameters required to set up the Pact tasks.
 */
public class TaskConfig implements Serializable {

	private static final long serialVersionUID = -2498884325640066272L;
	
	
	private static final String TASK_NAME = "taskname";
	
	private static final String STUB_OBJECT = "udf";
	
	private static final String STUB_PARAM_PREFIX = "udf.param.";
	
	protected final Configuration config;			// the actual configuration holding the values
	
	/**
	 * Creates a new Task Config that wraps the given configuration.
	 * 
	 * @param config The configuration holding the actual values.
	 */
	public TaskConfig(Configuration config) {
		this.config = config;
	}
	
	/**
	 * Gets the configuration that holds the actual values encoded.
	 * 
	 * @return The configuration that holds the actual values
	 */
	public Configuration getConfiguration() {
		return this.config;
	}
	
	// --------------------------------------------------------------------------------------------
	//                                       User Code
	// --------------------------------------------------------------------------------------------
	
	@SuppressWarnings("unchecked")
	public <T> UserCodeWrapper<T> getStubWrapper(ClassLoader cl) {
		try {
			return (UserCodeWrapper<T>) InstantiationUtil.readObjectFromConfig(this.config, STUB_OBJECT, cl);
		} catch (ClassNotFoundException | IOException e) {
			throw new CorruptConfigurationException("Could not read the user code wrapper: " + e.getMessage(), e);
		}
	}
	
	public void setStubParameters(Configuration parameters) {
		this.config.addAll(parameters, STUB_PARAM_PREFIX);
	}

	public Configuration getStubParameters() {
		return new DelegatingConfiguration(this.config, STUB_PARAM_PREFIX);
	}
	
}
