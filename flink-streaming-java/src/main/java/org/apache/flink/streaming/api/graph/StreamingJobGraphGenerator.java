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

package org.apache.flink.streaming.api.graph;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.*;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalPipelinedRegion;
import org.apache.flink.runtime.jobgraph.topology.DefaultLogicalTopology;
import org.apache.flink.runtime.jobmanager.scheduler.CoLocationGroup;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

/**
 * The StreamingJobGraphGenerator converts a {@link StreamGraph} into a {@link JobGraph}.
 */
@Internal
public class StreamingJobGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamingJobGraphGenerator.class);

	public static JobGraph createJobGraph(StreamGraph streamGraph, @Nullable JobID jobID) {
		return new StreamingJobGraphGenerator(streamGraph, jobID).createJobGraph();
	}

	// ------------------------------------------------------------------------

	private final StreamGraph streamGraph;

	private final Map<Integer, JobVertex> jobVertices;
	private final JobGraph jobGraph;
	private final Collection<Integer> builtVertices;

	private final List<StreamEdge> physicalEdgesInOrder;

	private final Map<Integer, Map<Integer, StreamConfig>> chainedConfigs;

	private final Map<Integer, StreamConfig> vertexConfigs;
	private final Map<Integer, String> chainedNames;

	private final Map<Integer, ResourceSpec> chainedMinResources;
	private final Map<Integer, ResourceSpec> chainedPreferredResources;

	private final Map<Integer, InputOutputFormatContainer> chainedInputOutputFormats;

	private final StreamGraphHasher defaultStreamGraphHasher;
	private final List<StreamGraphHasher> legacyStreamGraphHashers;

	private StreamingJobGraphGenerator(StreamGraph streamGraph, @Nullable JobID jobID) {
		this.streamGraph = streamGraph;
		this.defaultStreamGraphHasher = new StreamGraphHasherV2();
		this.legacyStreamGraphHashers = Arrays.asList(new StreamGraphUserHashHasher());

		this.jobVertices = new HashMap<>();
		this.builtVertices = new HashSet<>();
		this.chainedConfigs = new HashMap<>();
		this.vertexConfigs = new HashMap<>();
		this.chainedNames = new HashMap<>();
		this.chainedMinResources = new HashMap<>();
		this.chainedPreferredResources = new HashMap<>();
		this.chainedInputOutputFormats = new HashMap<>();
		this.physicalEdgesInOrder = new ArrayList<>();

		jobGraph = new JobGraph(jobID, streamGraph.getJobName());
	}

	// 获取JobGraph
	private JobGraph createJobGraph() {

		// make sure that all vertices start immediately
		jobGraph.setScheduleMode(streamGraph.getScheduleMode());

		// Generate deterministic hashes for the nodes in order to identify them across
		// submission iff they didn't change.
		Map<Integer, byte[]> hashes = defaultStreamGraphHasher.traverseStreamGraphAndGenerateHashes(streamGraph);

		// Generate legacy version hashes for backwards compatibility
		List<Map<Integer, byte[]>> legacyHashes = new ArrayList<>(legacyStreamGraphHashers.size());
		for (StreamGraphHasher hasher : legacyStreamGraphHashers) {
			legacyHashes.add(hasher.traverseStreamGraphAndGenerateHashes(streamGraph));
		}

		// 将并行度一样且one-to-one的算子优化成任务链
		setChaining(hashes, legacyHashes);

		setPhysicalEdges();

		setSlotSharingAndCoLocation();

		// set the ExecutionConfig last when it has been finalized
		try {
			jobGraph.setExecutionConfig(streamGraph.getExecutionConfig());
		}
		catch (IOException e) {
			throw new IllegalConfigurationException("Could not serialize the ExecutionConfig." +
					"This indicates that non-serializable types (like custom serializers) were registered");
		}

		return jobGraph;
	}

	private void setPhysicalEdges() {
		Map<Integer, List<StreamEdge>> physicalInEdgesInOrder = new HashMap<Integer, List<StreamEdge>>();

		for (StreamEdge edge : physicalEdgesInOrder) {
			int target = edge.getTargetId();

			List<StreamEdge> inEdges = physicalInEdgesInOrder.computeIfAbsent(target, k -> new ArrayList<>());

			inEdges.add(edge);
		}

		for (Map.Entry<Integer, List<StreamEdge>> inEdges : physicalInEdgesInOrder.entrySet()) {
			int vertex = inEdges.getKey();
			List<StreamEdge> edgeList = inEdges.getValue();

			vertexConfigs.get(vertex).setInPhysicalEdges(edgeList);
		}
	}

	/**
	 * Sets up task chains from the source {@link StreamNode} instances.
	 *
	 * <p>This will recursively create all {@link JobVertex} instances.
	 */
	private void setChaining(Map<Integer, byte[]> hashes, List<Map<Integer, byte[]>> legacyHashes) {
		for (Integer sourceNodeId : streamGraph.getSourceIDs()) {
			createChain(
					sourceNodeId,
					0,
					new OperatorChainInfo(sourceNodeId, hashes, legacyHashes, streamGraph));
		}
	}

	private List<StreamEdge> createChain(Integer currentNodeId, int chainIndex, OperatorChainInfo chainInfo) {
		Integer startNodeId = chainInfo.getStartNodeId();
		if (!builtVertices.contains(startNodeId)) {

			List<StreamEdge> transitiveOutEdges = new ArrayList<StreamEdge>();

			List<StreamEdge> chainableOutputs = new ArrayList<StreamEdge>();
			List<StreamEdge> nonChainableOutputs = new ArrayList<StreamEdge>();

			StreamNode currentNode = streamGraph.getStreamNode(currentNodeId);

			for (StreamEdge outEdge : currentNode.getOutEdges()) {
				// isChainable方法判断上下游的算子是不是one-to-one且并行度相同
				if (isChainable(outEdge, streamGraph)) {
					chainableOutputs.add(outEdge);
				} else {
					nonChainableOutputs.add(outEdge);
				}
			}

			for (StreamEdge chainable : chainableOutputs) {
				transitiveOutEdges.addAll(
						createChain(chainable.getTargetId(), chainIndex + 1, chainInfo));
			}

			for (StreamEdge nonChainable : nonChainableOutputs) {
				transitiveOutEdges.add(nonChainable);
				createChain(nonChainable.getTargetId(), 0, chainInfo.newChain(nonChainable.getTargetId()));
			}

			chainedNames.put(currentNodeId, createChainedName(currentNodeId, chainableOutputs));
			chainedMinResources.put(currentNodeId, createChainedMinResources(currentNodeId, chainableOutputs));
			chainedPreferredResources.put(currentNodeId, createChainedPreferredResources(currentNodeId, chainableOutputs));

			OperatorID currentOperatorId = chainInfo.addNodeToChain(currentNodeId, chainedNames.get(currentNodeId));

			if (currentNode.getInputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addInputFormat(currentOperatorId, currentNode.getInputFormat());
			}

			if (currentNode.getOutputFormat() != null) {
				getOrCreateFormatContainer(startNodeId).addOutputFormat(currentOperatorId, currentNode.getOutputFormat());
			}

			StreamConfig config = currentNodeId.equals(startNodeId)
					? createJobVertex(startNodeId, chainInfo)
					: new StreamConfig(new Configuration());

			setVertexConfig(currentNodeId, config, chainableOutputs, nonChainableOutputs);

			if (currentNodeId.equals(startNodeId)) {

				config.setChainStart();
				config.setChainIndex(0);
				config.setOperatorName(streamGraph.getStreamNode(currentNodeId).getOperatorName());
				config.setOutEdgesInOrder(transitiveOutEdges);
				config.setOutEdges(streamGraph.getStreamNode(currentNodeId).getOutEdges());

				for (StreamEdge edge : transitiveOutEdges) {
					connect(startNodeId, edge);
				}

				config.setTransitiveChainedTaskConfigs(chainedConfigs.get(startNodeId));

			} else {
				chainedConfigs.computeIfAbsent(startNodeId, k -> new HashMap<Integer, StreamConfig>());

				config.setChainIndex(chainIndex);
				StreamNode node = streamGraph.getStreamNode(currentNodeId);
				config.setOperatorName(node.getOperatorName());
				chainedConfigs.get(startNodeId).put(currentNodeId, config);
			}

			config.setOperatorID(currentOperatorId);

			if (chainableOutputs.isEmpty()) {
				config.setChainEnd();
			}
			return transitiveOutEdges;

		} else {
			return new ArrayList<>();
		}
	}

	private InputOutputFormatContainer getOrCreateFormatContainer(Integer startNodeId) {
		return chainedInputOutputFormats
			.computeIfAbsent(startNodeId, k -> new InputOutputFormatContainer(Thread.currentThread().getContextClassLoader()));
	}

	private String createChainedName(Integer vertexID, List<StreamEdge> chainedOutputs) {
		String operatorName = streamGraph.getStreamNode(vertexID).getOperatorName();
		if (chainedOutputs.size() > 1) {
			List<String> outputChainedNames = new ArrayList<>();
			for (StreamEdge chainable : chainedOutputs) {
				outputChainedNames.add(chainedNames.get(chainable.getTargetId()));
			}
			return operatorName + " -> (" + StringUtils.join(outputChainedNames, ", ") + ")";
		} else if (chainedOutputs.size() == 1) {
			return operatorName + " -> " + chainedNames.get(chainedOutputs.get(0).getTargetId());
		} else {
			return operatorName;
		}
	}

	private ResourceSpec createChainedMinResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec minResources = streamGraph.getStreamNode(vertexID).getMinResources();
		for (StreamEdge chainable : chainedOutputs) {
			minResources = minResources.merge(chainedMinResources.get(chainable.getTargetId()));
		}
		return minResources;
	}

	private ResourceSpec createChainedPreferredResources(Integer vertexID, List<StreamEdge> chainedOutputs) {
		ResourceSpec preferredResources = streamGraph.getStreamNode(vertexID).getPreferredResources();
		for (StreamEdge chainable : chainedOutputs) {
			preferredResources = preferredResources.merge(chainedPreferredResources.get(chainable.getTargetId()));
		}
		return preferredResources;
	}

	private StreamConfig createJobVertex(
			Integer streamNodeId,
			OperatorChainInfo chainInfo) {

		JobVertex jobVertex;
		StreamNode streamNode = streamGraph.getStreamNode(streamNodeId);

		byte[] hash = chainInfo.getHash(streamNodeId);

		if (hash == null) {
			throw new IllegalStateException("Cannot find node hash. " +
					"Did you generate them before calling this method?");
		}

		JobVertexID jobVertexId = new JobVertexID(hash);

		List<Tuple2<byte[], byte[]>> chainedOperators = chainInfo.getChainedOperatorHashes(streamNodeId);
		List<OperatorIDPair> operatorIDPairs = new ArrayList<>();
		if (chainedOperators != null) {
			for (Tuple2<byte[], byte[]> chainedOperator : chainedOperators) {
				OperatorID userDefinedOperatorID = chainedOperator.f1 == null ? null : new OperatorID(chainedOperator.f1);
				operatorIDPairs.add(OperatorIDPair.of(new OperatorID(chainedOperator.f0), userDefinedOperatorID));
			}
		}


		jobVertex = new JobVertex(
					chainedNames.get(streamNodeId),
					jobVertexId,
					operatorIDPairs);

		for (OperatorCoordinator.Provider coordinatorProvider : chainInfo.getCoordinatorProviders()) {
			try {
				jobVertex.addOperatorCoordinator(new SerializedValue<>(coordinatorProvider));
			} catch (IOException e) {
				throw new FlinkRuntimeException(String.format(
						"Coordinator Provider for node %s is not serializable.", chainedNames.get(streamNodeId)));
			}
		}

		jobVertex.setResources(chainedMinResources.get(streamNodeId), chainedPreferredResources.get(streamNodeId));

		jobVertex.setInvokableClass(streamNode.getJobVertexClass());

		int parallelism = streamNode.getParallelism();

		if (parallelism > 0) {
			jobVertex.setParallelism(parallelism);
		} else {
			parallelism = jobVertex.getParallelism();
		}

		jobVertex.setMaxParallelism(streamNode.getMaxParallelism());

		if (LOG.isDebugEnabled()) {
			LOG.debug("Parallelism set: {} for {}", parallelism, streamNodeId);
		}

		// TODO: inherit InputDependencyConstraint from the head operator
		jobVertex.setInputDependencyConstraint(streamGraph.getExecutionConfig().getDefaultInputDependencyConstraint());

		jobVertices.put(streamNodeId, jobVertex);
		builtVertices.add(streamNodeId);
		jobGraph.addVertex(jobVertex);

		return new StreamConfig(jobVertex.getConfiguration());
	}

	@SuppressWarnings("unchecked")
	private void setVertexConfig(Integer vertexID, StreamConfig config,
			List<StreamEdge> chainableOutputs, List<StreamEdge> nonChainableOutputs) {

		StreamNode vertex = streamGraph.getStreamNode(vertexID);

		config.setVertexID(vertexID);
		config.setBufferTimeout(vertex.getBufferTimeout());

		config.setTypeSerializersIn(vertex.getTypeSerializersIn());
		config.setTypeSerializerOut(vertex.getTypeSerializerOut());

		// iterate edges, find sideOutput edges create and save serializers for each outputTag type
		for (StreamEdge edge : chainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
					edge.getOutputTag(),
					edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}
		for (StreamEdge edge : nonChainableOutputs) {
			if (edge.getOutputTag() != null) {
				config.setTypeSerializerSideOut(
						edge.getOutputTag(),
						edge.getOutputTag().getTypeInfo().createSerializer(streamGraph.getExecutionConfig())
				);
			}
		}

		config.setStreamOperatorFactory(vertex.getOperatorFactory());
		config.setOutputSelectors(vertex.getOutputSelectors());

		config.setNumberOfOutputs(nonChainableOutputs.size());
		config.setNonChainedOutputs(nonChainableOutputs);
		config.setChainedOutputs(chainableOutputs);

		config.setTimeCharacteristic(streamGraph.getTimeCharacteristic());

		config.setStateBackend(streamGraph.getStateBackend());

		for (int i = 0; i < vertex.getStatePartitioners().length; i++) {
			config.setStatePartitioner(i, vertex.getStatePartitioners()[i]);
		}
		config.setStateKeySerializer(vertex.getStateKeySerializer());

		Class<? extends AbstractInvokable> vertexClass = vertex.getJobVertexClass();

		vertexConfigs.put(vertexID, config);
	}

	private void connect(Integer headOfChain, StreamEdge edge) {

		physicalEdgesInOrder.add(edge);

		Integer downStreamVertexID = edge.getTargetId();

		JobVertex headVertex = jobVertices.get(headOfChain);
		JobVertex downStreamVertex = jobVertices.get(downStreamVertexID);

		StreamConfig downStreamConfig = new StreamConfig(downStreamVertex.getConfiguration());

		downStreamConfig.setNumberOfInputs(downStreamConfig.getNumberOfInputs() + 1);

		StreamPartitioner<?> partitioner = edge.getPartitioner();

		ResultPartitionType resultPartitionType;
		switch (edge.getShuffleMode()) {
			case PIPELINED:
				resultPartitionType = ResultPartitionType.PIPELINED_BOUNDED;
				break;
			case BATCH:
				resultPartitionType = ResultPartitionType.BLOCKING;
				break;
			case UNDEFINED:
				resultPartitionType = determineResultPartitionType(partitioner);
				break;
			default:
				throw new UnsupportedOperationException("Data exchange mode " +
					edge.getShuffleMode() + " is not supported yet.");
		}

		JobEdge jobEdge;
		if (isPointwisePartitioner(partitioner)) {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
				headVertex,
				DistributionPattern.POINTWISE,
				resultPartitionType);
		} else {
			jobEdge = downStreamVertex.connectNewDataSetAsInput(
					headVertex,
					DistributionPattern.ALL_TO_ALL,
					resultPartitionType);
		}
		// set strategy name so that web interface can show it.
		jobEdge.setShipStrategyName(partitioner.toString());

		if (LOG.isDebugEnabled()) {
			LOG.debug("CONNECTED: {} - {} -> {}", partitioner.getClass().getSimpleName(),
					headOfChain, downStreamVertexID);
		}
	}

	private static boolean isPointwisePartitioner(StreamPartitioner<?> partitioner) {
		return partitioner instanceof ForwardPartitioner;
	}

	private ResultPartitionType determineResultPartitionType(StreamPartitioner<?> partitioner) {
		switch (streamGraph.getGlobalDataExchangeMode()) {
			case ALL_EDGES_BLOCKING:
				return ResultPartitionType.BLOCKING;
			case FORWARD_EDGES_PIPELINED:
				if (partitioner instanceof ForwardPartitioner) {
					return ResultPartitionType.PIPELINED_BOUNDED;
				} else {
					return ResultPartitionType.BLOCKING;
				}
			case POINTWISE_EDGES_PIPELINED:
				if (isPointwisePartitioner(partitioner)) {
					return ResultPartitionType.PIPELINED_BOUNDED;
				} else {
					return ResultPartitionType.BLOCKING;
				}
			case ALL_EDGES_PIPELINED:
				return ResultPartitionType.PIPELINED_BOUNDED;
			default:
				throw new RuntimeException("Unrecognized global data exchange mode " + streamGraph.getGlobalDataExchangeMode());
		}
	}

	// 并行度一样，one-to-one，可以做任务链优化
	public static boolean isChainable(StreamEdge edge, StreamGraph streamGraph) {
		StreamNode upStreamVertex = streamGraph.getSourceVertex(edge);
		StreamNode downStreamVertex = streamGraph.getTargetVertex(edge);

		return downStreamVertex.getInEdges().size() == 1
				&& upStreamVertex.isSameSlotSharingGroup(downStreamVertex)
				&& areOperatorsChainable(upStreamVertex, downStreamVertex, streamGraph)
			    // one-to-one
				&& (edge.getPartitioner() instanceof ForwardPartitioner)
				&& edge.getShuffleMode() != ShuffleMode.BATCH
			    // 并行度必须一样
				&& upStreamVertex.getParallelism() == downStreamVertex.getParallelism()
				&& streamGraph.isChainingEnabled();
	}

	@VisibleForTesting
	static boolean areOperatorsChainable(
			StreamNode upStreamVertex,
			StreamNode downStreamVertex,
			StreamGraph streamGraph) {
		StreamOperatorFactory<?> upStreamOperator = upStreamVertex.getOperatorFactory();
		StreamOperatorFactory<?> downStreamOperator = downStreamVertex.getOperatorFactory();
		if (downStreamOperator == null || upStreamOperator == null) {
			return false;
		}

		if (upStreamOperator.getChainingStrategy() == ChainingStrategy.NEVER ||
			downStreamOperator.getChainingStrategy() != ChainingStrategy.ALWAYS) {
			return false;
		}

		return true;
	}

	/**
	 * Backtraces the head of an operator chain.
	 */
	private static StreamOperatorFactory<?> getHeadOperator(StreamNode upStreamVertex, StreamGraph streamGraph) {
		if (upStreamVertex.getInEdges().size() == 1 && isChainable(upStreamVertex.getInEdges().get(0), streamGraph)) {
			return getHeadOperator(streamGraph.getSourceVertex(upStreamVertex.getInEdges().get(0)), streamGraph);
		}
		return upStreamVertex.getOperatorFactory();
	}

	private void setSlotSharingAndCoLocation() {
		setSlotSharing();
		setCoLocation();
	}

	private void setSlotSharing() {
		final Map<String, SlotSharingGroup> specifiedSlotSharingGroups = new HashMap<>();
		final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = buildVertexRegionSlotSharingGroups();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			final JobVertex vertex = entry.getValue();
			final String slotSharingGroupKey = streamGraph.getStreamNode(entry.getKey()).getSlotSharingGroup();

			final SlotSharingGroup effectiveSlotSharingGroup;
			if (slotSharingGroupKey == null) {
				effectiveSlotSharingGroup = null;
			} else if (slotSharingGroupKey.equals(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)) {
				// fallback to the region slot sharing group by default
				effectiveSlotSharingGroup = vertexRegionSlotSharingGroups.get(vertex.getID());
			} else {
				effectiveSlotSharingGroup = specifiedSlotSharingGroups.computeIfAbsent(
					slotSharingGroupKey, k -> new SlotSharingGroup());
			}

			vertex.setSlotSharingGroup(effectiveSlotSharingGroup);
		}
	}

	/**
	 * Maps a vertex to its region slot sharing group.
	 * If {@link StreamGraph#isAllVerticesInSameSlotSharingGroupByDefault()}
	 * returns true, all regions will be in the same slot sharing group.
	 */
	private Map<JobVertexID, SlotSharingGroup> buildVertexRegionSlotSharingGroups() {
		final Map<JobVertexID, SlotSharingGroup> vertexRegionSlotSharingGroups = new HashMap<>();
		final SlotSharingGroup defaultSlotSharingGroup = new SlotSharingGroup();

		final boolean allRegionsInSameSlotSharingGroup = streamGraph.isAllVerticesInSameSlotSharingGroupByDefault();

		final Set<DefaultLogicalPipelinedRegion> regions = new DefaultLogicalTopology(jobGraph).getLogicalPipelinedRegions();
		for (DefaultLogicalPipelinedRegion region : regions) {
			final SlotSharingGroup regionSlotSharingGroup;
			if (allRegionsInSameSlotSharingGroup) {
				regionSlotSharingGroup = defaultSlotSharingGroup;
			} else {
				regionSlotSharingGroup = new SlotSharingGroup();
			}

			for (JobVertexID jobVertexID : region.getVertexIDs()) {
				vertexRegionSlotSharingGroups.put(jobVertexID, regionSlotSharingGroup);
			}
		}

		return vertexRegionSlotSharingGroups;
	}

	private void setCoLocation() {
		final Map<String, Tuple2<SlotSharingGroup, CoLocationGroup>> coLocationGroups = new HashMap<>();

		for (Entry<Integer, JobVertex> entry : jobVertices.entrySet()) {

			final StreamNode node = streamGraph.getStreamNode(entry.getKey());
			final JobVertex vertex = entry.getValue();
			final SlotSharingGroup sharingGroup = vertex.getSlotSharingGroup();

			// configure co-location constraint
			final String coLocationGroupKey = node.getCoLocationGroup();
			if (coLocationGroupKey != null) {
				if (sharingGroup == null) {
					throw new IllegalStateException("Cannot use a co-location constraint without a slot sharing group");
				}

				Tuple2<SlotSharingGroup, CoLocationGroup> constraint = coLocationGroups.computeIfAbsent(
						coLocationGroupKey, k -> new Tuple2<>(sharingGroup, new CoLocationGroup()));

				if (constraint.f0 != sharingGroup) {
					throw new IllegalStateException("Cannot co-locate operators from different slot sharing groups");
				}

				vertex.updateCoLocationGroup(constraint.f1);
				constraint.f1.addVertex(vertex);
			}
		}
	}

	/**
	 * A private class to help maintain the information of an operator chain during the recursive call in
	 * {@link #createChain(Integer, int, OperatorChainInfo)}.
	 */
	private static class OperatorChainInfo {
		private final Integer startNodeId;
		private final Map<Integer, byte[]> hashes;
		private final List<Map<Integer, byte[]>> legacyHashes;
		private final Map<Integer, List<Tuple2<byte[], byte[]>>> chainedOperatorHashes;
		private final List<OperatorCoordinator.Provider> coordinatorProviders;
		private final StreamGraph streamGraph;

		private OperatorChainInfo(
				int startNodeId,
				Map<Integer, byte[]> hashes,
				List<Map<Integer, byte[]>> legacyHashes,
				StreamGraph streamGraph) {
			this.startNodeId = startNodeId;
			this.hashes = hashes;
			this.legacyHashes = legacyHashes;
			this.chainedOperatorHashes = new HashMap<>();
			this.coordinatorProviders = new ArrayList<>();
			this.streamGraph = streamGraph;
		}

		byte[] getHash(Integer streamNodeId) {
			return hashes.get(streamNodeId);
		}

		private Integer getStartNodeId() {
			return startNodeId;
		}

		private List<Tuple2<byte[], byte[]>> getChainedOperatorHashes(int startNodeId) {
			return chainedOperatorHashes.get(startNodeId);
		}

		private List<OperatorCoordinator.Provider> getCoordinatorProviders() {
			return coordinatorProviders;
		}

		private OperatorID addNodeToChain(int currentNodeId, String operatorName) {
			List<Tuple2<byte[], byte[]>> operatorHashes =
					chainedOperatorHashes.computeIfAbsent(startNodeId, k -> new ArrayList<>());

			byte[] primaryHashBytes = hashes.get(currentNodeId);

			for (Map<Integer, byte[]> legacyHash : legacyHashes) {
				operatorHashes.add(new Tuple2<>(primaryHashBytes, legacyHash.get(currentNodeId)));
			}

			streamGraph
					.getStreamNode(currentNodeId)
					.getCoordinatorProvider(operatorName, new OperatorID(getHash(currentNodeId)))
					.map(coordinatorProviders::add);
			return new OperatorID(primaryHashBytes);
		}

		private OperatorChainInfo newChain(Integer startNodeId) {
			return new OperatorChainInfo(startNodeId, hashes, legacyHashes, streamGraph);
		}
	}
}
