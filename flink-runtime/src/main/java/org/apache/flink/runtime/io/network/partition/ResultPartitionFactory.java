package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.network.NettyShuffleEnvironment;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferPoolFactory;
import org.apache.flink.runtime.io.network.buffer.BufferPoolOwner;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.FunctionWithException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Factory for {@link ResultPartition} to use in {@link NettyShuffleEnvironment}.
 */
public class ResultPartitionFactory {

	private static final Logger LOG = LoggerFactory.getLogger(ResultPartitionFactory.class);

	private final ResultPartitionManager partitionManager;

	private final FileChannelManager channelManager;

	private final BufferPoolFactory bufferPoolFactory;

	private final int networkBuffersPerChannel;

	private final int floatingNetworkBuffersPerGate;

	private final int networkBufferSize;

	private final boolean forcePartitionReleaseOnConsumption;

	private final boolean blockingShuffleCompressionEnabled;

	private final String compressionCodec;

	private final int maxBuffersPerChannel;

	public ResultPartitionFactory(
		ResultPartitionManager partitionManager,
		FileChannelManager channelManager,
		BufferPoolFactory bufferPoolFactory,
		int networkBuffersPerChannel,
		int floatingNetworkBuffersPerGate,
		int networkBufferSize,
		boolean forcePartitionReleaseOnConsumption,
		boolean blockingShuffleCompressionEnabled,
		String compressionCodec,
		int maxBuffersPerChannel) {

		this.partitionManager = partitionManager;
		this.channelManager = channelManager;
		this.networkBuffersPerChannel = networkBuffersPerChannel;
		this.floatingNetworkBuffersPerGate = floatingNetworkBuffersPerGate;
		this.bufferPoolFactory = bufferPoolFactory;
		this.networkBufferSize = networkBufferSize;
		this.forcePartitionReleaseOnConsumption = forcePartitionReleaseOnConsumption;
		this.blockingShuffleCompressionEnabled = blockingShuffleCompressionEnabled;
		this.compressionCodec = compressionCodec;
		this.maxBuffersPerChannel = maxBuffersPerChannel;
	}

	public ResultPartition create(
			String taskNameWithSubtaskAndId,
			int partitionIndex,
			ResultPartitionDeploymentDescriptor desc) {
		return create(
			taskNameWithSubtaskAndId,
			partitionIndex,
			desc.getShuffleDescriptor().getResultPartitionID(),
			desc.getPartitionType(),
			desc.getNumberOfSubpartitions(),
			desc.getMaxParallelism(),
			createBufferPoolFactory(desc.getNumberOfSubpartitions(), desc.getPartitionType()));
	}

	@VisibleForTesting
	public ResultPartition create(
			String taskNameWithSubtaskAndId,
			int partitionIndex,
			ResultPartitionID id,
			ResultPartitionType type,
			int numberOfSubpartitions,
			int maxParallelism,
			FunctionWithException<BufferPoolOwner, BufferPool, IOException> bufferPoolFactory) {
		ResultSubpartition[] subpartitions = new ResultSubpartition[numberOfSubpartitions];
		ResultPartition partition = forcePartitionReleaseOnConsumption || !type.isBlocking()
			? new ReleaseOnConsumptionResultPartition(
				taskNameWithSubtaskAndId,
				partitionIndex,
				id,
				type,
				subpartitions,
				maxParallelism,
				partitionManager,
				bufferPoolFactory)
			: new ResultPartition(
				taskNameWithSubtaskAndId,
				partitionIndex,
				id,
				type,
				subpartitions,
				maxParallelism,
				partitionManager,
				bufferPoolFactory);

		createSubpartitions(partition, type, subpartitions);

		LOG.debug("{}: Initialized {}", taskNameWithSubtaskAndId, this);

		return partition;
	}

	private void createSubpartitions(
			ResultPartition partition,
			ResultPartitionType type,
			ResultSubpartition[] subpartitions) {
		// Create the subpartitions.
		if (type.isBlocking()) {
			initializeBoundedBlockingPartitions(
				subpartitions,
				partition,
				networkBufferSize,
				channelManager);
		} else {
			for (int i = 0; i < subpartitions.length; i++) {
				subpartitions[i] = new PipelinedSubpartition(i, partition);
			}
		}
	}

	private static void initializeBoundedBlockingPartitions(
			ResultSubpartition[] subpartitions,
			ResultPartition parent,
			int networkBufferSize,
			FileChannelManager channelManager) {
		int i = 0;
	}

	private static void releasePartitionsQuietly(ResultSubpartition[] partitions, int until) {
		for (int i = 0; i < until; i++) {
			final ResultSubpartition subpartition = partitions[i];
			ExceptionUtils.suppressExceptions(subpartition::release);
		}
	}

	/**
	 * The minimum pool size should be <code>numberOfSubpartitions + 1</code> for two considerations:
	 *
	 * <p>1. StreamTask can only process input if there is at-least one available buffer on output side, so it might cause
	 * stuck problem if the minimum pool size is exactly equal to the number of subpartitions, because every subpartition
	 * might maintain a partial unfilled buffer.
	 *
	 * <p>2. Increases one more buffer for every output LocalBufferPool to void performance regression if processing input is
	 * based on at-least one buffer available on output side.
	 */
	@VisibleForTesting
	FunctionWithException<BufferPoolOwner, BufferPool, IOException> createBufferPoolFactory(
			int numberOfSubpartitions,
			ResultPartitionType type) {
		return bufferPoolOwner -> {
			int maxNumberOfMemorySegments = type.isBounded() ?
				numberOfSubpartitions * networkBuffersPerChannel + floatingNetworkBuffersPerGate : Integer.MAX_VALUE;
			// If the partition type is back pressure-free, we register with the buffer pool for
			// callbacks to release memory.
			return bufferPoolFactory.createBufferPool(
				numberOfSubpartitions + 1,
				maxNumberOfMemorySegments,
				type.hasBackPressure() ? null : bufferPoolOwner,
				numberOfSubpartitions,
				maxBuffersPerChannel);
		};
	}
}
