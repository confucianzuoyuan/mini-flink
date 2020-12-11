/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.io.network.partition.consumer.IndexedInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;
import org.apache.flink.streaming.api.graph.StreamConfig;

import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;


@Internal
public class InputProcessorUtil {
	@SuppressWarnings("unchecked")
	public static CheckpointedInputGate createCheckpointedInputGate(
			AbstractInvokable toNotifyOnCheckpoint,
			StreamConfig config,
			IndexedInputGate[] inputGates,
			String taskName) {
		CheckpointedInputGate[] checkpointedInputGates = createCheckpointedMultipleInputGate(
			toNotifyOnCheckpoint,
			config,
			taskName,
			Arrays.asList(inputGates));
		return Iterables.getOnlyElement(Arrays.asList(checkpointedInputGates));
	}

	/**
	 * @return a pair of {@link CheckpointedInputGate} created for two corresponding
	 * {@link InputGate}s supplied as parameters.
	 */
	public static CheckpointedInputGate[] createCheckpointedMultipleInputGate(
			AbstractInvokable toNotifyOnCheckpoint,
			StreamConfig config,
			String taskName,
			List<IndexedInputGate>... inputGates) {

		IndexedInputGate[] sortedInputGates = Arrays.stream(inputGates)
			.flatMap(Collection::stream)
			.sorted(Comparator.comparing(IndexedInputGate::getGateIndex))
			.toArray(IndexedInputGate[]::new);

		InputGate[] unionedInputGates = Arrays.stream(inputGates)
			.map(InputGateUtil::createInputGate)
			.toArray(InputGate[]::new);

		return Arrays.stream(unionedInputGates)
			.map(unionedInputGate -> new CheckpointedInputGate(unionedInputGate))
			.toArray(CheckpointedInputGate[]::new);
	}

}
