/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
	
	// ------------------------------------ User Code ---------------------------------------------
	
	private static final String STUB_OBJECT = "udf";
	
	private static final String STUB_PARAM_PREFIX = "udf.param.";
	
	// -------------------------------------- Driver ----------------------------------------------
	
	private static final String DRIVER_CLASS = "driver.class";
	
	private static final String DRIVER_STRATEGY = "driver.strategy";
	
	private static final String DRIVER_COMPARATOR_FACTORY_PREFIX = "driver.comp.";
	
	private static final String DRIVER_COMPARATOR_PARAMETERS_PREFIX = "driver.comp.params.";
	
	private static final String DRIVER_PAIR_COMPARATOR_FACTORY = "driver.paircomp";
	
	private static final String DRIVER_MUTABLE_OBJECT_MODE = "diver.mutableobjects";

	// -------------------------------------- Inputs ----------------------------------------------

	private static final String NUM_INPUTS = "in.num";
	
	private static final String NUM_BROADCAST_INPUTS = "in.bc.num";
	
	private static final String INPUT_GROUP_SIZE_PREFIX = "in.groupsize.";
	
	private static final String BROADCAST_INPUT_GROUP_SIZE_PREFIX = "in.bc.groupsize.";
	
	private static final String INPUT_TYPE_SERIALIZER_FACTORY_PREFIX = "in.serializer.";
	
	private static final String BROADCAST_INPUT_TYPE_SERIALIZER_FACTORY_PREFIX = "in.bc.serializer.";
	
	private static final String INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX = "in.serializer.param.";
	
	private static final String BROADCAST_INPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX = "in.bc.serializer.param.";
	
	private static final String INPUT_LOCAL_STRATEGY_PREFIX = "in.strategy.";
	
	private static final String INPUT_STRATEGY_COMPARATOR_FACTORY_PREFIX = "in.comparator.";
	
	private static final String INPUT_STRATEGY_COMPARATOR_PARAMETERS_PREFIX = "in.comparator.param.";
	
	private static final String INPUT_DAM_PREFIX = "in.dam.";
	
	private static final String INPUT_REPLAYABLE_PREFIX = "in.dam.replay.";
	
	private static final String INPUT_DAM_MEMORY_PREFIX = "in.dam.mem.";
	
	private static final String BROADCAST_INPUT_NAME_PREFIX = "in.broadcast.name.";
	
	
	// -------------------------------------- Outputs ---------------------------------------------
	
	private static final String OUTPUTS_NUM = "out.num";
	
	private static final String OUTPUT_TYPE_SERIALIZER_FACTORY = "out.serializer";
	
	private static final String OUTPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX = "out.serializer.param.";
	
	private static final String OUTPUT_SHIP_STRATEGY_PREFIX = "out.shipstrategy.";
	
	private static final String OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX = "out.comp.";
	
	private static final String OUTPUT_TYPE_COMPARATOR_PARAMETERS_PREFIX = "out.comp.param.";
	
	private static final String OUTPUT_DATA_DISTRIBUTION_CLASS = "out.distribution.class";
	
	private static final String OUTPUT_DATA_DISTRIBUTION_PREFIX = "out.distribution.";
	
	private static final String OUTPUT_PARTITIONER = "out.partitioner.";
	
	// ------------------------------------- Chaining ---------------------------------------------
	
	private static final String CHAINING_NUM_STUBS = "chaining.num";
	
	private static final String CHAINING_TASKCONFIG_PREFIX = "chaining.taskconfig.";
	
	private static final String CHAINING_TASK_PREFIX = "chaining.task.";
	
	private static final String CHAINING_TASKNAME_PREFIX = "chaining.taskname.";
	
	// ------------------------------------ Memory & Co -------------------------------------------
	
	private static final String MEMORY_DRIVER = "memory.driver";
	
	private static final String MEMORY_INPUT_PREFIX = "memory.input.";
	
	private static final String FILEHANDLES_DRIVER = "filehandles.driver";
	
	private static final String FILEHANDLES_INPUT_PREFIX = "filehandles.input.";
	
	private static final String SORT_SPILLING_THRESHOLD_DRIVER = "sort-spill-threshold.driver";
	
	private static final String SORT_SPILLING_THRESHOLD_INPUT_PREFIX = "sort-spill-threshold.input.";

	private static final String USE_LARGE_RECORD_HANDLER = "sort-spill.large-record-handler";

	private static final boolean USE_LARGE_RECORD_HANDLER_DEFAULT = false;

	// ----------------------------------- Iterations ---------------------------------------------
	
	private static final String NUMBER_OF_ITERATIONS = "iterative.num-iterations";
	
	private static final String NUMBER_OF_EOS_EVENTS_PREFIX = "iterative.num-eos-events.";
	
	private static final String NUMBER_OF_EOS_EVENTS_BROADCAST_PREFIX = "iterative.num-eos-events.bc.";
	
	private static final String ITERATION_HEAD_ID = "iterative.head.id";
	
	private static final String ITERATION_WORKSET_MARKER = "iterative.is-workset";
	
	private static final String ITERATION_HEAD_INDEX_OF_PARTIAL_SOLUTION = "iterative.head.ps-input-index";
	
	private static final String ITERATION_HEAD_INDEX_OF_SOLUTIONSET = "iterative.head.ss-input-index";
	
	private static final String ITERATION_HEAD_BACKCHANNEL_MEMORY = "iterative.head.backchannel-memory";
	
	private static final String ITERATION_HEAD_SOLUTION_SET_MEMORY = "iterative.head.solutionset-memory";
	
	private static final String ITERATION_HEAD_FINAL_OUT_CONFIG_PREFIX = "iterative.head.out.";
	
	private static final String ITERATION_HEAD_SYNC_OUT_INDEX = "iterative.head.sync-index.";
	
	private static final String ITERATION_CONVERGENCE_CRITERION = "iterative.terminationCriterion";
	
	private static final String ITERATION_CONVERGENCE_CRITERION_AGG_NAME = "iterative.terminationCriterion.agg.name";

	private static final String ITERATION_IMPLICIT_CONVERGENCE_CRITERION = "iterative.implicit.terminationCriterion";

	private static final String ITERATION_IMPLICIT_CONVERGENCE_CRITERION_AGG_NAME = "iterative.implicit.terminationCriterion.agg.name";
	
	private static final String ITERATION_NUM_AGGREGATORS = "iterative.num-aggs";
	
	private static final String ITERATION_AGGREGATOR_NAME_PREFIX = "iterative.agg.name.";
	
	private static final String ITERATION_AGGREGATOR_PREFIX = "iterative.agg.data.";
	
	private static final String ITERATION_SOLUTION_SET_SERIALIZER = "iterative.ss-serializer";
	
	private static final String ITERATION_SOLUTION_SET_SERIALIZER_PARAMETERS = "iterative.ss-serializer.params";
	
	private static final String ITERATION_SOLUTION_SET_COMPARATOR = "iterative.ss-comparator";
	
	private static final String ITERATION_SOLUTION_SET_COMPARATOR_PARAMETERS = "iterative.ss-comparator.params";
	
	private static final String ITERATION_SOLUTION_SET_UPDATE = "iterative.ss-update";
	
	private static final String ITERATION_SOLUTION_SET_UPDATE_SKIP_REPROBE = "iterative.ss-update-fast";

	private static final String ITERATION_SOLUTION_SET_UPDATE_WAIT = "iterative.ss-wait";

	private static final String ITERATION_WORKSET_UPDATE = "iterative.ws-update";
	
	private static final String SOLUTION_SET_OBJECTS = "itertive.ss.obj";

	// ---------------------------------- Miscellaneous -------------------------------------------
	
	private static final char SEPARATOR = '.';

	// --------------------------------------------------------------------------------------------
	//                         Members, Constructors, and Accessors
	// --------------------------------------------------------------------------------------------
	
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
	
	public void setTaskName(String name) {
		if (name != null) {
			this.config.setString(TASK_NAME, name);
		}
	}
	
	public String getTaskName() {
		return this.config.getString(TASK_NAME, null);
	}

	public void setStubWrapper(UserCodeWrapper<?> wrapper) {
		try {
			InstantiationUtil.writeObjectToConfig(wrapper, this.config, STUB_OBJECT);
		} catch (IOException e) {
			throw new CorruptConfigurationException("Could not write the user code wrapper " + wrapper.getClass() + " : " + e.toString(), e);
		}
	}

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
