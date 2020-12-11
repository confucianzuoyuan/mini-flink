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

package org.apache.flink.runtime.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Extension for the {@link Executor} interface which is enriched by method for scheduling tasks
 * in the future.
 */
public interface ScheduledExecutor extends Executor {

	ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit);

	<V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit);

	ScheduledFuture<?> scheduleAtFixedRate(
		Runnable command,
		long initialDelay,
		long period,
		TimeUnit unit);

	ScheduledFuture<?> scheduleWithFixedDelay(
		Runnable command,
		long initialDelay,
		long delay,
		TimeUnit unit);
}
