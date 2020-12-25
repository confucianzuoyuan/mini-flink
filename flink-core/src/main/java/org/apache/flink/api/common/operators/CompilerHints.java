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


package org.apache.flink.api.common.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.operators.util.FieldSet;

import java.util.Set;

/**
 * A class encapsulating compiler hints describing the behavior of the user function.
 * If set, the optimizer will use them to estimate the sizes of the intermediate results.
 * Note that these values are optional hints, the optimizer will always generate a valid plan without
 * them as well. The hints may help, however, to improve the plan choice.
 */
@Internal
public class CompilerHints {

	private long outputSize = -1;
	
	private long outputCardinality = -1;
	
	private float avgOutputRecordSize = -1.0f; 
	
	private float filterFactor = -1.0f;

	private Set<FieldSet> uniqueFields;

	// --------------------------------------------------------------------------------------------
	//  Basic Record Statistics
	// --------------------------------------------------------------------------------------------
	
	public long getOutputSize() {
		return outputSize;
	}
	
	public void setOutputSize(long outputSize) {
		if (outputSize < 0) {
			throw new IllegalArgumentException("The output size cannot be smaller than zero.");
		}
		
		this.outputSize = outputSize;
	}
	
	public long getOutputCardinality() {
		return this.outputCardinality;
	}
	
	public void setOutputCardinality(long outputCardinality) {
		if (outputCardinality < 0) {
			throw new IllegalArgumentException("The output cardinality cannot be smaller than zero.");
		}
		
		this.outputCardinality = outputCardinality;
	}
	
	public float getAvgOutputRecordSize() {
		return this.avgOutputRecordSize;
	}
	
	public void setAvgOutputRecordSize(float avgOutputRecordSize) {
		if (avgOutputRecordSize <= 0) {
			throw new IllegalArgumentException("The size of produced records must be positive.");
		}
		
		this.avgOutputRecordSize = avgOutputRecordSize;
	}
	
	public float getFilterFactor() {
		return filterFactor;
	}

	
	public void setFilterFactor(float filterFactor) {
		if (filterFactor < 0) {
			throw new IllegalArgumentException("The filter factor cannot be smaller than zero.");
		}
		
		this.filterFactor = filterFactor;
	}

	
	// --------------------------------------------------------------------------------------------
	//  Uniqueness
	// --------------------------------------------------------------------------------------------
	
	/**
	 * Gets the FieldSets that are unique
	 * 
	 * @return List of FieldSet that are unique
	 */
	public Set<FieldSet> getUniqueFields() {
		return this.uniqueFields;
	}
	
	public void clearUniqueFields() {
		this.uniqueFields = null;
	}
	
}
