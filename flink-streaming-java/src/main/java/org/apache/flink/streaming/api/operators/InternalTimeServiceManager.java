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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.PriorityQueueSetFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * An entity keeping all the time-related services available to all operators extending the
 * {@link AbstractStreamOperator}. Right now, this is only a
 * {@link InternalTimerServiceImpl timer services}.
 *
 * <b>NOTE:</b> These services are only available to keyed operators.
 *
 * @param <K> The type of keys used for the timers and the registry.
 */
@Internal
public class InternalTimeServiceManager<K> {
	protected static final Logger LOG = LoggerFactory.getLogger(InternalTimeServiceManager.class);

	@VisibleForTesting
	static final String TIMER_STATE_PREFIX = "_timer_state";
	@VisibleForTesting
	static final String PROCESSING_TIMER_PREFIX = TIMER_STATE_PREFIX + "/processing_";
	@VisibleForTesting
	static final String EVENT_TIMER_PREFIX = TIMER_STATE_PREFIX + "/event_";

	private final KeyGroupRange localKeyGroupRange;
	private final KeyContext keyContext;

	private final PriorityQueueSetFactory priorityQueueSetFactory;
	private final ProcessingTimeService processingTimeService;

	private final Map<String, InternalTimerServiceImpl<K, ?>> timerServices;

	private final boolean useLegacySynchronousSnapshots;

	InternalTimeServiceManager(
		KeyGroupRange localKeyGroupRange,
		KeyContext keyContext,
		PriorityQueueSetFactory priorityQueueSetFactory,
		ProcessingTimeService processingTimeService, boolean useLegacySynchronousSnapshots) {

		this.localKeyGroupRange = Preconditions.checkNotNull(localKeyGroupRange);
		this.priorityQueueSetFactory = Preconditions.checkNotNull(priorityQueueSetFactory);
		this.keyContext = Preconditions.checkNotNull(keyContext);
		this.processingTimeService = Preconditions.checkNotNull(processingTimeService);
		this.useLegacySynchronousSnapshots = useLegacySynchronousSnapshots;

		this.timerServices = new HashMap<>();
	}

	public void advanceWatermark(Watermark watermark) throws Exception {
		for (InternalTimerServiceImpl<?, ?> service : timerServices.values()) {
			service.advanceWatermark(watermark.getTimestamp());
		}
	}

	@VisibleForTesting
	public int numProcessingTimeTimers() {
		int count = 0;
		for (InternalTimerServiceImpl<?, ?> timerService : timerServices.values()) {
			count += timerService.numProcessingTimeTimers();
		}
		return count;
	}

	@VisibleForTesting
	public int numEventTimeTimers() {
		int count = 0;
		for (InternalTimerServiceImpl<?, ?> timerService : timerServices.values()) {
			count += timerService.numEventTimeTimers();
		}
		return count;
	}
}
