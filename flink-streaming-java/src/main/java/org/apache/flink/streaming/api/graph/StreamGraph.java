package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Pipeline;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.transformations.ShuffleMode;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Class representing the streaming topology. It contains all the information
 * necessary to build the jobgraph for the execution.
 *
 */
@Internal
public class StreamGraph implements Pipeline {

	// 图数据结构：定点（节点）和边构成的。

	private String jobName;

	private final ExecutionConfig executionConfig;

	private ScheduleMode scheduleMode;

	private boolean chaining;

	private TimeCharacteristic timeCharacteristic;

	private GlobalDataExchangeMode globalDataExchangeMode;

	/** Flag to indicate whether to put all vertices into the same slot sharing group by default. */
	private boolean allVerticesInSameSlotSharingGroupByDefault = true;

	private Map<Integer, StreamNode> streamNodes;
	private Set<Integer> sources;
	private Set<Integer> sinks;
	private Map<Integer, Tuple2<Integer, List<String>>> virtualSelectNodes;
	private Map<Integer, Tuple2<Integer, OutputTag>> virtualSideOutputNodes;
	private Map<Integer, Tuple3<Integer, StreamPartitioner<?>, ShuffleMode>> virtualPartitionNodes;

	protected Map<Integer, String> vertexIDtoBrokerID;
	protected Map<Integer, Long> vertexIDtoLoopTimeout;
	private StateBackend stateBackend;
	private Set<Tuple2<StreamNode, StreamNode>> iterationSourceSinkPairs;

	public StreamGraph(ExecutionConfig executionConfig) {
		this.executionConfig = checkNotNull(executionConfig);

		// create an empty new stream graph.
		clear();
	}

	/**
	 * Remove all registered nodes etc.
	 */
	public void clear() {
		streamNodes = new HashMap<>();
		virtualSelectNodes = new HashMap<>();
		virtualSideOutputNodes = new HashMap<>();
		virtualPartitionNodes = new HashMap<>();
		vertexIDtoBrokerID = new HashMap<>();
		vertexIDtoLoopTimeout  = new HashMap<>();
		iterationSourceSinkPairs = new HashSet<>();
		sources = new HashSet<>();
		sinks = new HashSet<>();
	}

	public ExecutionConfig getExecutionConfig() {
		return executionConfig;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public void setChaining(boolean chaining) {
		this.chaining = chaining;
	}

	public void setStateBackend(StateBackend backend) {
		this.stateBackend = backend;
	}

	public StateBackend getStateBackend() {
		return this.stateBackend;
	}

	public ScheduleMode getScheduleMode() {
		return scheduleMode;
	}

	public void setScheduleMode(ScheduleMode scheduleMode) {
		this.scheduleMode = scheduleMode;
	}

	public TimeCharacteristic getTimeCharacteristic() {
		return timeCharacteristic;
	}

	public void setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		this.timeCharacteristic = timeCharacteristic;
	}

	public GlobalDataExchangeMode getGlobalDataExchangeMode() {
		return globalDataExchangeMode;
	}

	public void setGlobalDataExchangeMode(GlobalDataExchangeMode globalDataExchangeMode) {
		this.globalDataExchangeMode = globalDataExchangeMode;
	}

	/**
	 * Gets whether to put all vertices into the same slot sharing group by default.
	 *
	 * @return whether to put all vertices into the same slot sharing group by default.
	 */
	public boolean isAllVerticesInSameSlotSharingGroupByDefault() {
		return allVerticesInSameSlotSharingGroupByDefault;
	}

	// Checkpointing

	public boolean isChainingEnabled() {
		return chaining;
	}

	public boolean isIterative() {
		return !vertexIDtoLoopTimeout.isEmpty();
	}

	public <IN, OUT> void addLegacySource(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {
		addOperator(vertexID, slotSharingGroup, coLocationGroup, operatorFactory, inTypeInfo, outTypeInfo, operatorName);
		// graph
		// vertex: 点
		// edge: 边
		System.out.println("vertexID: " + vertexID);
		sources.add(vertexID);
	}

	public <IN, OUT> void addSink(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {
		addOperator(vertexID, slotSharingGroup, coLocationGroup, operatorFactory, inTypeInfo, outTypeInfo, operatorName);
		sinks.add(vertexID);
	}

	public <IN, OUT> void addOperator(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName) {
		Class<? extends AbstractInvokable> invokableClass =
				operatorFactory.isStreamSource() ? SourceStreamTask.class : OneInputStreamTask.class;
		addOperator(vertexID, slotSharingGroup, coLocationGroup, operatorFactory, inTypeInfo,
				outTypeInfo, operatorName, invokableClass);
	}

	private <IN, OUT> void addOperator(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			StreamOperatorFactory<OUT> operatorFactory,
			TypeInformation<IN> inTypeInfo,
			TypeInformation<OUT> outTypeInfo,
			String operatorName,
			Class<? extends AbstractInvokable> invokableClass) {

		addNode(vertexID, slotSharingGroup, coLocationGroup, invokableClass, operatorFactory, operatorName);
		setSerializers(vertexID, createSerializer(inTypeInfo), null, createSerializer(outTypeInfo));

		if (operatorFactory.isOutputTypeConfigurable() && outTypeInfo != null) {
			// sets the output type which must be know at StreamGraph creation time
			operatorFactory.setOutputType(outTypeInfo, executionConfig);
		}
	}

	protected StreamNode addNode(
			Integer vertexID,
			@Nullable String slotSharingGroup,
			@Nullable String coLocationGroup,
			Class<? extends AbstractInvokable> vertexClass,
			StreamOperatorFactory<?> operatorFactory,
			String operatorName) {

		if (streamNodes.containsKey(vertexID)) {
			throw new RuntimeException("Duplicate vertexID " + vertexID);
		}

		StreamNode vertex = new StreamNode(
				vertexID,
				slotSharingGroup,
				coLocationGroup,
				operatorFactory,
				operatorName,
				new ArrayList<OutputSelector<?>>(),
				vertexClass);

		streamNodes.put(vertexID, vertex);

		return vertex;
	}



	/**
	 * Adds a new virtual node that is used to connect a downstream vertex to an input with a
	 * certain partitioning.
	 *
	 * <p>When adding an edge from the virtual node to a downstream node the connection will be made
	 * to the original node, but with the partitioning given here.
	 *
	 * @param originalId ID of the node that should be connected to.
	 * @param virtualId ID of the virtual node.
	 * @param partitioner The partitioner
	 */
	public void addVirtualPartitionNode(
			Integer originalId,
			Integer virtualId,
			StreamPartitioner<?> partitioner,
			ShuffleMode shuffleMode) {

		if (virtualPartitionNodes.containsKey(virtualId)) {
			throw new IllegalStateException("Already has virtual partition node with id " + virtualId);
		}

		virtualPartitionNodes.put(virtualId, new Tuple3<>(originalId, partitioner, shuffleMode));
	}

	// upStreamVertexID --Edge--> downStreamVertexID
	public void addEdge(Integer upStreamVertexID, Integer downStreamVertexID, int typeNumber) {
		addEdgeInternal(upStreamVertexID,
				downStreamVertexID,
				typeNumber,
				null,
				new ArrayList<String>(),
				null,
				null);

	}

	private void addEdgeInternal(Integer upStreamVertexID,
			Integer downStreamVertexID,
			int typeNumber,
			StreamPartitioner<?> partitioner,
			List<String> outputNames,
			OutputTag outputTag,
			ShuffleMode shuffleMode) {

		if (virtualPartitionNodes.containsKey(upStreamVertexID)) {
			int virtualId = upStreamVertexID;
			upStreamVertexID = virtualPartitionNodes.get(virtualId).f0;
			if (partitioner == null) {
				partitioner = virtualPartitionNodes.get(virtualId).f1;
			}
			shuffleMode = virtualPartitionNodes.get(virtualId).f2;
			addEdgeInternal(upStreamVertexID, downStreamVertexID, typeNumber, partitioner, outputNames, outputTag, shuffleMode);
		} else {
			StreamNode upstreamNode = getStreamNode(upStreamVertexID);
			StreamNode downstreamNode = getStreamNode(downStreamVertexID);

			// If no partitioner was specified and the parallelism of upstream and downstream
			// operator matches use forward partitioning, use rebalance otherwise.
			if (partitioner == null && upstreamNode.getParallelism() == downstreamNode.getParallelism()) {
				partitioner = new ForwardPartitioner<Object>();
			} else if (partitioner == null) {
				partitioner = new RebalancePartitioner<Object>();
			}

			if (shuffleMode == null) {
				shuffleMode = ShuffleMode.UNDEFINED;
			}

			StreamEdge edge = new StreamEdge(upstreamNode, downstreamNode, typeNumber, outputNames, partitioner, outputTag, shuffleMode);

			getStreamNode(edge.getSourceId()).addOutEdge(edge);
			getStreamNode(edge.getTargetId()).addInEdge(edge);
		}
	}

	public void setParallelism(Integer vertexID, int parallelism) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setParallelism(parallelism);
		}
	}

	public void setMaxParallelism(int vertexID, int maxParallelism) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setMaxParallelism(maxParallelism);
		}
	}

	public void setResources(int vertexID, ResourceSpec minResources, ResourceSpec preferredResources) {
		if (getStreamNode(vertexID) != null) {
			getStreamNode(vertexID).setResources(minResources, preferredResources);
		}
	}

	public void setOneInputStateKey(Integer vertexID, KeySelector<?, ?> keySelector, TypeSerializer<?> keySerializer) {
		StreamNode node = getStreamNode(vertexID);
		node.setStatePartitioners(keySelector);
		node.setStateKeySerializer(keySerializer);
	}

	public void setSerializers(Integer vertexID, TypeSerializer<?> in1, TypeSerializer<?> in2, TypeSerializer<?> out) {
		StreamNode vertex = getStreamNode(vertexID);
		vertex.setSerializersIn(in1, in2);
		vertex.setSerializerOut(out);
	}

	public <OUT> void setOutType(Integer vertexID, TypeInformation<OUT> outType) {
		getStreamNode(vertexID).setSerializerOut(outType.createSerializer(executionConfig));
	}

	public StreamNode getStreamNode(Integer vertexID) {
		return streamNodes.get(vertexID);
	}

	protected Collection<? extends Integer> getVertexIDs() {
		return streamNodes.keySet();
	}

	@VisibleForTesting
	public List<StreamEdge> getStreamEdges(int sourceId, int targetId) {
		List<StreamEdge> result = new ArrayList<>();
		for (StreamEdge edge : getStreamNode(sourceId).getOutEdges()) {
			if (edge.getTargetId() == targetId) {
				result.add(edge);
			}
		}
		return result;
	}

	@VisibleForTesting
	@Deprecated
	public List<StreamEdge> getStreamEdgesOrThrow(int sourceId, int targetId) {
		List<StreamEdge> result = getStreamEdges(sourceId, targetId);
		if (result.isEmpty()) {
			throw new RuntimeException("No such edge in stream graph: " + sourceId + " -> " + targetId);
		}
		return result;
	}

	public Collection<Integer> getSourceIDs() {
		return sources;
	}

	public Collection<Integer> getSinkIDs() {
		return sinks;
	}

	public Collection<StreamNode> getStreamNodes() {
		return streamNodes.values();
	}

	public String getBrokerID(Integer vertexID) {
		return vertexIDtoBrokerID.get(vertexID);
	}

	public long getLoopTimeout(Integer vertexID) {
		return vertexIDtoLoopTimeout.get(vertexID);
	}

	public Set<Tuple2<StreamNode, StreamNode>> getIterationSourceSinkPairs() {
		return iterationSourceSinkPairs;
	}

	public StreamNode getSourceVertex(StreamEdge edge) {
		return streamNodes.get(edge.getSourceId());
	}

	public StreamNode getTargetVertex(StreamEdge edge) {
		return streamNodes.get(edge.getTargetId());
	}

	/**
	 * Gets the assembled {@link JobGraph} with a random {@link JobID}.
	 */
	public JobGraph getJobGraph() {
		return getJobGraph(null);
	}

	/**
	 * Gets the assembled {@link JobGraph} with a specified {@link JobID}.
	 */
	public JobGraph getJobGraph(@Nullable JobID jobID) {
		return StreamingJobGraphGenerator.createJobGraph(this, jobID);
	}

	public String getStreamingPlanAsJSON() {
		try {
			return new JSONGenerator(this).getJSON();
		}
		catch (Exception e) {
			throw new RuntimeException("JSON plan creation failed", e);
		}
	}

	private <T> TypeSerializer<T> createSerializer(TypeInformation<T> typeInfo) {
		return typeInfo != null && !(typeInfo instanceof MissingTypeInfo) ?
			typeInfo.createSerializer(executionConfig) : null;
	}
}
