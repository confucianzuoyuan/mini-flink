/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

import org.apache.flink.runtime.concurrent.Executors;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executor;

public class SharedStateRegistry implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(SharedStateRegistry.class);

	/** A singleton object for the default implementation of a {@link SharedStateRegistryFactory} */
	public static final SharedStateRegistryFactory DEFAULT_FACTORY = SharedStateRegistry::new;

	/** All registered state objects by an artificial key */
	private final Map<SharedStateRegistryKey, SharedStateRegistry.SharedStateEntry> registeredStates;

	/** This flag indicates whether or not the registry is open or if close() was called */
	private boolean open;

	/** Executor for async state deletion */
	private final Executor asyncDisposalExecutor;

	/** Default uses direct executor to delete unreferenced state */
	public SharedStateRegistry() {
		this(Executors.directExecutor());
	}

	public SharedStateRegistry(Executor asyncDisposalExecutor) {
		this.registeredStates = new HashMap<>();
		this.asyncDisposalExecutor = Preconditions.checkNotNull(asyncDisposalExecutor);
		this.open = true;
	}

	@Override
	public String toString() {
		synchronized (registeredStates) {
			return "SharedStateRegistry{" +
				"registeredStates=" + registeredStates +
				'}';
		}
	}

	@Override
	public void close() {
		synchronized (registeredStates) {
			open = false;
		}
	}

	/**
	 * An entry in the registry, tracking the handle and the corresponding reference count.
	 */
	private static class SharedStateEntry {

		/** The shared state handle */
		private final StreamStateHandle stateHandle;

		/** The current reference count of the state handle */
		private int referenceCount;

		SharedStateEntry(StreamStateHandle value) {
			this.stateHandle = value;
			this.referenceCount = 1;
		}

		StreamStateHandle getStateHandle() {
			return stateHandle;
		}

		int getReferenceCount() {
			return referenceCount;
		}

		void increaseReferenceCount() {
			++referenceCount;
		}

		void decreaseReferenceCount() {
			--referenceCount;
		}

		@Override
		public String toString() {
			return "SharedStateEntry{" +
				"stateHandle=" + stateHandle +
				", referenceCount=" + referenceCount +
				'}';
		}
	}

	/**
	 * The result of an attempt to (un)/reference state
	 */
	public static class Result {

		/** The (un)registered state handle from the request */
		private final StreamStateHandle reference;

		/** The reference count to the state handle after the request to (un)register */
		private final int referenceCount;

		private Result(SharedStateEntry sharedStateEntry) {
			this.reference = sharedStateEntry.getStateHandle();
			this.referenceCount = sharedStateEntry.getReferenceCount();
		}

		public Result(StreamStateHandle reference, int referenceCount) {
			Preconditions.checkArgument(referenceCount >= 0);

			this.reference = reference;
			this.referenceCount = referenceCount;
		}

		public StreamStateHandle getReference() {
			return reference;
		}

		public int getReferenceCount() {
			return referenceCount;
		}

		@Override
		public String toString() {
			return "Result{" +
				"reference=" + reference +
				", referenceCount=" + referenceCount +
				'}';
		}
	}

}
