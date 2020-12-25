package org.apache.flink.streaming.api.graph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class StreamGraphGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(StreamGraphGenerator.class);

	public static final int DEFAULT_LOWER_BOUND_MAX_PARALLELISM = KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM;

	public static final ScheduleMode DEFAULT_SCHEDULE_MODE = ScheduleMode.EAGER;

	public static final TimeCharacteristic DEFAULT_TIME_CHARACTERISTIC = TimeCharacteristic.ProcessingTime;

	public static final String DEFAULT_JOB_NAME = "Flink Streaming Job";

	/** The default buffer timeout (max delay of records in the network stack). */
	public static final long DEFAULT_NETWORK_BUFFER_TIMEOUT = 100L;

	public static final String DEFAULT_SLOT_SHARING_GROUP = "default";

	private final List<Transformation<?>> transformations;

	private final ExecutionConfig executionConfig;

	private StateBackend stateBackend;

	private boolean chaining = true;

	private ScheduleMode scheduleMode = DEFAULT_SCHEDULE_MODE;

	private TimeCharacteristic timeCharacteristic = DEFAULT_TIME_CHARACTERISTIC;

	private long defaultBufferTimeout = DEFAULT_NETWORK_BUFFER_TIMEOUT;

	private String jobName = DEFAULT_JOB_NAME;

	private GlobalDataExchangeMode globalDataExchangeMode = GlobalDataExchangeMode.ALL_EDGES_PIPELINED;

	// This is used to assign a unique ID to iteration source/sink
	protected static Integer iterationIdCounter = 0;

	private StreamGraph streamGraph;

	// Keep track of which Transforms we have already transformed, this is necessary because
	// we have loops, i.e. feedback edges.
	private Map<Transformation<?>, Collection<Integer>> alreadyTransformed;

	public StreamGraphGenerator(List<Transformation<?>> transformations, ExecutionConfig executionConfig) {
		this.transformations = checkNotNull(transformations);
		this.executionConfig = checkNotNull(executionConfig);
	}

	public StreamGraphGenerator setStateBackend(StateBackend stateBackend) {
		this.stateBackend = stateBackend;
		return this;
	}

	public StreamGraphGenerator setChaining(boolean chaining) {
		this.chaining = chaining;
		return this;
	}

	public StreamGraphGenerator setScheduleMode(ScheduleMode scheduleMode) {
		this.scheduleMode = scheduleMode;
		return this;
	}

	public StreamGraphGenerator setTimeCharacteristic(TimeCharacteristic timeCharacteristic) {
		this.timeCharacteristic = timeCharacteristic;
		return this;
	}

	public StreamGraphGenerator setDefaultBufferTimeout(long defaultBufferTimeout) {
		this.defaultBufferTimeout = defaultBufferTimeout;
		return this;
	}

	public StreamGraphGenerator setJobName(String jobName) {
		this.jobName = jobName;
		return this;
	}

	public StreamGraph generate() {
		streamGraph = new StreamGraph(executionConfig);
		streamGraph.setStateBackend(stateBackend);
		streamGraph.setChaining(chaining);
		streamGraph.setScheduleMode(scheduleMode);
		streamGraph.setTimeCharacteristic(timeCharacteristic);
		streamGraph.setJobName(jobName);
		streamGraph.setGlobalDataExchangeMode(globalDataExchangeMode);

		alreadyTransformed = new HashMap<>();

		for (Transformation<?> transformation: transformations) {
			transform(transformation);
		}

		final StreamGraph builtStreamGraph = streamGraph;

		alreadyTransformed.clear();
		alreadyTransformed = null;
		streamGraph = null;

		return builtStreamGraph;
	}

	/**
	 * Transforms one {@code Transformation}.
	 *
	 * <p>This checks whether we already transformed it and exits early in that case. If not it
	 * delegates to one of the transformation specific methods.
	 */
	private Collection<Integer> transform(Transformation<?> transform) {

		if (alreadyTransformed.containsKey(transform)) {
			return alreadyTransformed.get(transform);
		}

		Collection<Integer> transformedIds;
		if (transform instanceof OneInputTransformation<?, ?>) {
			transformedIds = transformOneInputTransform((OneInputTransformation<?, ?>) transform);
		} else if (transform instanceof LegacySourceTransformation<?>) {
			transformedIds = transformLegacySource((LegacySourceTransformation<?>) transform);
		} else if (transform instanceof SinkTransformation<?>) {
			transformedIds = transformSink((SinkTransformation<?>) transform);
		} else if (transform instanceof PartitionTransformation<?>) {
			transformedIds = transformPartition((PartitionTransformation<?>) transform);
		} else {
			throw new IllegalStateException("Unknown transformation: " + transform);
		}

		// need this check because the iterate transformation adds itself before
		// transforming the feedback edges
		if (!alreadyTransformed.containsKey(transform)) {
			alreadyTransformed.put(transform, transformedIds);
		}

		return transformedIds;
	}

	/**
	 * Transforms a {@code PartitionTransformation}.
	 *
	 * <p>For this we create a virtual node in the {@code StreamGraph} that holds the partition
	 * property. @see StreamGraphGenerator
	 */
	private <T> Collection<Integer> transformPartition(PartitionTransformation<T> partition) {
		Transformation<T> input = partition.getInput();
		List<Integer> resultIds = new ArrayList<>();

		Collection<Integer> transformedIds = transform(input);
		for (Integer transformedId: transformedIds) {
			int virtualId = Transformation.getNewNodeId();
			streamGraph.addVirtualPartitionNode(
					transformedId, virtualId, partition.getPartitioner(), partition.getShuffleMode());
			resultIds.add(virtualId);
		}

		return resultIds;
	}

	/**
	 * Transforms a {@code LegacySourceTransformation}.
	 */
	private <T> Collection<Integer> transformLegacySource(LegacySourceTransformation<T> source) {
		String slotSharingGroup = "default";

		streamGraph.addLegacySource(source.getId(),
				slotSharingGroup,
				source.getCoLocationGroupKey(),
				source.getOperatorFactory(),
				null,
				source.getOutputType(),
				"Source: " + source.getName());
		int parallelism = source.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
			source.getParallelism() : executionConfig.getParallelism();
		streamGraph.setParallelism(source.getId(), parallelism);
		streamGraph.setMaxParallelism(source.getId(), source.getMaxParallelism());
		return Collections.singleton(source.getId());
	}

	/**
	 * Transforms a {@code SinkTransformation}.
	 */
	private <T> Collection<Integer> transformSink(SinkTransformation<T> sink) {

		Collection<Integer> inputIds = transform(sink.getInput());

		// 将任务槽放在一个组里面，任务槽可以分组。
		String slotSharingGroup = "default";

		streamGraph.addSink(sink.getId(),
				slotSharingGroup,
				sink.getCoLocationGroupKey(),
				sink.getOperatorFactory(),
				sink.getInput().getOutputType(),
				null,
				"Sink: " + sink.getName());

		int parallelism = sink.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
			sink.getParallelism() : executionConfig.getParallelism();
		streamGraph.setParallelism(sink.getId(), parallelism);
		streamGraph.setMaxParallelism(sink.getId(), sink.getMaxParallelism());

		for (Integer inputId: inputIds) {
			streamGraph.addEdge(inputId,
					sink.getId(),
					0
			);
		}

		return Collections.emptyList();
	}

	/**
	 * Transforms a {@code OneInputTransformation}.
	 *
	 * <p>This recursively transforms the inputs, creates a new {@code StreamNode} in the graph and
	 * wired the inputs to this new node.
	 */
	private <IN, OUT> Collection<Integer> transformOneInputTransform(OneInputTransformation<IN, OUT> transform) {

		Collection<Integer> inputIds = transform(transform.getInput());

		// the recursive call might have already transformed this
		if (alreadyTransformed.containsKey(transform)) {
			return alreadyTransformed.get(transform);
		}

		String slotSharingGroup = "default";

		streamGraph.addOperator(transform.getId(),
				slotSharingGroup,
				transform.getCoLocationGroupKey(),
				transform.getOperatorFactory(),
				transform.getInputType(),
				transform.getOutputType(),
				transform.getName());

		if (transform.getStateKeySelector() != null) {
			TypeSerializer<?> keySerializer = transform.getStateKeyType().createSerializer(executionConfig);
			streamGraph.setOneInputStateKey(transform.getId(), transform.getStateKeySelector(), keySerializer);
		}

		int parallelism = transform.getParallelism() != ExecutionConfig.PARALLELISM_DEFAULT ?
			transform.getParallelism() : executionConfig.getParallelism();
		streamGraph.setParallelism(transform.getId(), parallelism);
		streamGraph.setMaxParallelism(transform.getId(), transform.getMaxParallelism());

		for (Integer inputId: inputIds) {
			streamGraph.addEdge(inputId, transform.getId(), 0);
		}

		return Collections.singleton(transform.getId());
	}
}
