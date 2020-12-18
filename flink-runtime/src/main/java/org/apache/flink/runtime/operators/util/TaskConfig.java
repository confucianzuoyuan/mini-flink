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

import org.apache.flink.api.common.aggregators.Aggregator;
import org.apache.flink.api.common.aggregators.AggregatorWithName;
import org.apache.flink.api.common.aggregators.ConvergenceCriterion;
import org.apache.flink.api.common.distributions.DataDistribution;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.operators.util.UserCodeWrapper;
import org.apache.flink.api.common.typeutils.TypeComparatorFactory;
import org.apache.flink.api.common.typeutils.TypePairComparatorFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerFactory;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DelegatingConfiguration;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.operators.Driver;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
	
	public void setStubParameter(String key, String value) {
		this.config.setString(STUB_PARAM_PREFIX + key, value);
	}

	public String getStubParameter(String key, String defaultValue) {
		return this.config.getString(STUB_PARAM_PREFIX + key, defaultValue);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                      Driver
	// --------------------------------------------------------------------------------------------
	
	public void setDriver(@SuppressWarnings("rawtypes") Class<? extends Driver> driver) {
		this.config.setString(DRIVER_CLASS, driver.getName());
	}
	
	public <S extends Function, OT> Class<? extends Driver<S, OT>> getDriver() {
		final String className = this.config.getString(DRIVER_CLASS, null);
		if (className == null) {
			throw new CorruptConfigurationException("The pact driver class is missing.");
		}
		
		try {
			@SuppressWarnings("unchecked")
			final Class<Driver<S, OT>> pdClazz = (Class<Driver<S, OT>>) (Class<?>) Driver.class;
			return Class.forName(className).asSubclass(pdClazz);
		} catch (ClassNotFoundException cnfex) {
			throw new CorruptConfigurationException("The given driver class cannot be found.");
		} catch (ClassCastException ccex) {
			throw new CorruptConfigurationException("The given driver class does not implement the pact driver interface.");
		}
	}
	
	public void setMutableObjectMode(boolean mode) {
		this.config.setBoolean(DRIVER_MUTABLE_OBJECT_MODE, mode);
	}
	
	public boolean getMutableObjectMode() {
		return this.config.getBoolean(DRIVER_MUTABLE_OBJECT_MODE, false);
	}

	// --------------------------------------------------------------------------------------------
	//                                        Inputs
	// --------------------------------------------------------------------------------------------
	
	public int getNumInputs() {
		return this.config.getInteger(NUM_INPUTS, 0);
	}
	
	public int getNumBroadcastInputs() {
		return this.config.getInteger(NUM_BROADCAST_INPUTS, 0);
	}
	
	public int getGroupSize(int groupIndex) {
		return this.config.getInteger(INPUT_GROUP_SIZE_PREFIX + groupIndex, -1);
	}
	
	public int getBroadcastGroupSize(int groupIndex) {
		return this.config.getInteger(BROADCAST_INPUT_GROUP_SIZE_PREFIX + groupIndex, -1);
	}
	

	public void setInputCached(int inputNum, boolean persistent) {
		this.config.setBoolean(INPUT_REPLAYABLE_PREFIX + inputNum, persistent);
	}
	
	public boolean isInputCached(int inputNum) {
		return this.config.getBoolean(INPUT_REPLAYABLE_PREFIX + inputNum, false);
	}
	
	public void setRelativeInputMaterializationMemory(int inputNum, double relativeMemory) {
		this.config.setDouble(INPUT_DAM_MEMORY_PREFIX + inputNum, relativeMemory);
	}
	


	
	// --------------------------------------------------------------------------------------------
	//                                        Outputs
	// --------------------------------------------------------------------------------------------
	

	public int getNumOutputs() {
		return this.config.getInteger(OUTPUTS_NUM, 0);
	}

	public void setOutputSerializer(TypeSerializerFactory<?> factory) {
		setTypeSerializerFactory(factory, OUTPUT_TYPE_SERIALIZER_FACTORY, OUTPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX);
	}
	
	public <T> TypeSerializerFactory<T> getOutputSerializer(ClassLoader cl) {
		return getTypeSerializerFactory(OUTPUT_TYPE_SERIALIZER_FACTORY, OUTPUT_TYPE_SERIALIZER_PARAMETERS_PREFIX, cl);
	}
	
	public void setOutputComparator(TypeComparatorFactory<?> factory, int outputNum) {
		setTypeComparatorFactory(factory, OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX + outputNum,
			OUTPUT_TYPE_COMPARATOR_PARAMETERS_PREFIX + outputNum + SEPARATOR);
	}
	
	public <T> TypeComparatorFactory<T> getOutputComparator(int outputNum, ClassLoader cl) {
		return getTypeComparatorFactory(OUTPUT_TYPE_COMPARATOR_FACTORY_PREFIX + outputNum,
			OUTPUT_TYPE_COMPARATOR_PARAMETERS_PREFIX + outputNum + SEPARATOR, cl);
	}
	
	public void setOutputDataDistribution(DataDistribution distribution, int outputNum) {
		this.config.setString(OUTPUT_DATA_DISTRIBUTION_CLASS, distribution.getClass().getName());
		
		try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
				DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos)) {
			
			distribution.write(out);
			config.setBytes(OUTPUT_DATA_DISTRIBUTION_PREFIX + outputNum, baos.toByteArray());
			
		}
		catch (IOException e) {
			throw new RuntimeException("Error serializing the DataDistribution: " + e.getMessage(), e);
		}
	}
	
	public void setOutputPartitioner(Partitioner<?> partitioner, int outputNum) {
		try {
			InstantiationUtil.writeObjectToConfig(partitioner, config, OUTPUT_PARTITIONER + outputNum);
		}
		catch (Throwable t) {
			throw new RuntimeException("Could not serialize custom partitioner.", t);
		}
	}
	
	public Partitioner<?> getOutputPartitioner(int outputNum, final ClassLoader cl) throws ClassNotFoundException {
		try {
			return (Partitioner<?>) InstantiationUtil.readObjectFromConfig(config, OUTPUT_PARTITIONER + outputNum, cl);
		}
		catch (ClassNotFoundException e) {
			throw e;
		}
		catch (Throwable t) {
			throw new RuntimeException("Could not deserialize custom partitioner.", t);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	//                       Parameters to configure the memory and I/O behavior
	// --------------------------------------------------------------------------------------------

	public void setRelativeMemoryDriver(double relativeMemorySize) {
		this.config.setDouble(MEMORY_DRIVER, relativeMemorySize);
	}

	public double getRelativeMemoryDriver() {
		return this.config.getDouble(MEMORY_DRIVER, 0);
	}
	
	public void setRelativeMemoryInput(int inputNum, double relativeMemorySize) {
		this.config.setDouble(MEMORY_INPUT_PREFIX + inputNum, relativeMemorySize);
	}

	public double getRelativeMemoryInput(int inputNum) {
		return this.config.getDouble(MEMORY_INPUT_PREFIX + inputNum, 0);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                    Parameters for Function Chaining
	// --------------------------------------------------------------------------------------------
	
	public int getNumberOfChainedStubs() {
		return this.config.getInteger(CHAINING_NUM_STUBS, 0);
	}
	
	public String getChainedTaskName(int chainPos) {
		return this.config.getString(CHAINING_TASKNAME_PREFIX + chainPos, null);
	}
	
	// --------------------------------------------------------------------------------------------
	//                                    Miscellaneous
	// --------------------------------------------------------------------------------------------
	
	private void setTypeSerializerFactory(TypeSerializerFactory<?> factory,
			String classNameKey, String parametersPrefix)
	{
		// sanity check the factory type
		InstantiationUtil.checkForInstantiation(factory.getClass());
		
		// store the type
		this.config.setString(classNameKey, factory.getClass().getName());
		// store the parameters
		final DelegatingConfiguration parameters = new DelegatingConfiguration(this.config, parametersPrefix);
		factory.writeParametersToConfig(parameters);
	}
	
	private <T> TypeSerializerFactory<T> getTypeSerializerFactory(String classNameKey, String parametersPrefix, ClassLoader cl) {
		// check the class name
		final String className = this.config.getString(classNameKey, null);
		if (className == null) {
			return null;
		}
		
		// instantiate the class
		@SuppressWarnings("unchecked")
		final Class<TypeSerializerFactory<T>> superClass = (Class<TypeSerializerFactory<T>>) (Class<?>) TypeSerializerFactory.class;
		final TypeSerializerFactory<T> factory;
		try {
			Class<? extends TypeSerializerFactory<T>> clazz = Class.forName(className, true, cl).asSubclass(superClass);
			factory = InstantiationUtil.instantiate(clazz, superClass);
		}
		catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The class '" + className + "', noted in the configuration as " +
					"serializer factory, could not be found. It is not part of the user code's class loader resources.");
		}
		catch (ClassCastException ccex) {
			throw new CorruptConfigurationException("The class noted in the configuration as the serializer factory " +
					"is no subclass of TypeSerializerFactory.");
		}
		
		// parameterize the comparator factory
		final Configuration parameters = new DelegatingConfiguration(this.config, parametersPrefix);
		try {
			factory.readParametersFromConfig(parameters, cl);
		} catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The type serializer factory could not load its parameters from the " +
					"configuration due to missing classes.", cnfex);
		}
		
		return factory;
	}
	
	private void setTypeComparatorFactory(TypeComparatorFactory<?> factory,
			String classNameKey, String parametersPrefix)
	{
		// sanity check the factory type
		InstantiationUtil.checkForInstantiation(factory.getClass());
		
		// store the type
		this.config.setString(classNameKey, factory.getClass().getName());
		// store the parameters
		final DelegatingConfiguration parameters = new DelegatingConfiguration(this.config, parametersPrefix);
		factory.writeParametersToConfig(parameters);
	}
	
	private <T> TypeComparatorFactory<T> getTypeComparatorFactory(String classNameKey, String parametersPrefix, ClassLoader cl) {
		// check the class name
		final String className = this.config.getString(classNameKey, null);
		if (className == null) {
			return null;
		}
		
		// instantiate the class
		@SuppressWarnings("unchecked")
		final Class<TypeComparatorFactory<T>> superClass = (Class<TypeComparatorFactory<T>>) (Class<?>) TypeComparatorFactory.class;
		final TypeComparatorFactory<T> factory;
		try {
			Class<? extends TypeComparatorFactory<T>> clazz = Class.forName(className, true, cl).asSubclass(superClass);
			factory = InstantiationUtil.instantiate(clazz, superClass);
		}
		catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The class '" + className + "', noted in the configuration as " +
					"comparator factory, could not be found. It is not part of the user code's class loader resources.");
		}
		catch (ClassCastException ccex) {
			throw new CorruptConfigurationException("The class noted in the configuration as the comparator factory " +
					"is no subclass of TypeComparatorFactory.");
		}
		
		// parameterize the comparator factory
		final Configuration parameters = new DelegatingConfiguration(this.config, parametersPrefix);
		try {
			factory.readParametersFromConfig(parameters, cl);
		} catch (ClassNotFoundException cnfex) {
			throw new RuntimeException("The type serializer factory could not load its parameters from the " +
					"configuration due to missing classes.", cnfex);
		}
		
		return factory;
	}
	
	public void setSolutionSetUnmanaged(boolean unmanaged) {
		config.setBoolean(SOLUTION_SET_OBJECTS, unmanaged);
	}
	
	public boolean isSolutionSetUnmanaged() {
		return config.getBoolean(SOLUTION_SET_OBJECTS, false);
	}

}
