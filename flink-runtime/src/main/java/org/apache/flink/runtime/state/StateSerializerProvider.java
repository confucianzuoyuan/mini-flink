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

package org.apache.flink.runtime.state;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.UnloadableDummyTypeSerializer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

@Internal
public abstract class StateSerializerProvider<T> {

	/**
	 * The registered serializer for the state.
	 *
	 * <p>In the case that this provider was created from a restored serializer snapshot via
	 * {@link #fromPreviousSerializerSnapshot(TypeSerializerSnapshot)}, but a new serializer was never registered
	 * for the state (i.e., this is the case if a restored state was never accessed), this would be {@code null}.
	 */
	@Nullable
	TypeSerializer<T> registeredSerializer;

	/**
	 * The state's previous serializer's snapshot.
	 *
	 * <p>In the case that this provider was created from a registered state serializer instance via
	 * {@link #fromNewRegisteredSerializer(TypeSerializer)}, but a serializer snapshot was never supplied to this
	 * provider (i.e. because the registered serializer was for a new state, not a restored one), this
	 * would be {@code null}.
	 */
	@Nullable
	TypeSerializerSnapshot<T> previousSerializerSnapshot;

	/**
	 * The restore serializer, lazily created only when the restore serializer is accessed.
	 *
	 * <p>NOTE: It is important to only create this lazily, so that off-heap
	 * state do not fail eagerly when restoring state that has a
	 * {@link UnloadableDummyTypeSerializer} as the previous serializer. This should
	 * be relevant only for restores from Flink versions prior to 1.7.x.
	 */
	@Nullable
	private TypeSerializer<T> cachedRestoredSerializer;

	private boolean isRegisteredWithIncompatibleSerializer = false;

	public static <T> StateSerializerProvider<T> fromPreviousSerializerSnapshot(TypeSerializerSnapshot<T> stateSerializerSnapshot) {
		return new LazilyRegisteredStateSerializerProvider<>(stateSerializerSnapshot);
	}

	public static <T> StateSerializerProvider<T> fromNewRegisteredSerializer(TypeSerializer<T> registeredStateSerializer) {
		return new EagerlyRegisteredStateSerializerProvider<>(registeredStateSerializer);
	}

	private StateSerializerProvider(@Nonnull TypeSerializer<T> stateSerializer) {
		this.registeredSerializer = stateSerializer;
		this.previousSerializerSnapshot = null;
	}

	private StateSerializerProvider(@Nonnull TypeSerializerSnapshot<T> previousSerializerSnapshot) {
		this.previousSerializerSnapshot = previousSerializerSnapshot;
		this.registeredSerializer = null;
	}

	@Nonnull
	public final TypeSerializer<T> currentSchemaSerializer() {
		if (registeredSerializer != null) {
			checkState(
				!isRegisteredWithIncompatibleSerializer,
				"Unable to provide a serializer with the current schema, because the restored state was " +
					"registered with a new serializer that has incompatible schema.");

			return registeredSerializer;
		}

		// if we are not yet registered with a new serializer,
		// we can just use the restore serializer to read / write the state.
		return previousSchemaSerializer();
	}

	@Nonnull
	public final TypeSerializer<T> previousSchemaSerializer() {
		if (cachedRestoredSerializer != null) {
			return cachedRestoredSerializer;
		}

		if (previousSerializerSnapshot == null) {
			throw new UnsupportedOperationException(
				"This provider does not contain the state's previous serializer's snapshot. Cannot provider a serializer for previous schema.");
		}

		this.cachedRestoredSerializer = previousSerializerSnapshot.restoreSerializer();
		return cachedRestoredSerializer;
	}



	@Nonnull
	public abstract TypeSerializerSchemaCompatibility<T> registerNewSerializerForRestoredState(TypeSerializer<T> newSerializer);

	protected final void invalidateCurrentSchemaSerializerAccess() {
		this.isRegisteredWithIncompatibleSerializer = true;
	}

	/**
	 * Implementation of the {@link StateSerializerProvider} for the case where a snapshot of the
	 * previous serializer is obtained before a new state serializer is registered (hence, the naming "lazily" registered).
	 */
	private static class LazilyRegisteredStateSerializerProvider<T> extends StateSerializerProvider<T> {

		LazilyRegisteredStateSerializerProvider(TypeSerializerSnapshot<T> previousSerializerSnapshot) {
			super(Preconditions.checkNotNull(previousSerializerSnapshot));
		}

		@Nonnull
		@Override
		@SuppressWarnings("ConstantConditions")
		public TypeSerializerSchemaCompatibility<T> registerNewSerializerForRestoredState(TypeSerializer<T> newSerializer) {
			checkNotNull(newSerializer);
			if (registeredSerializer != null) {
				throw new UnsupportedOperationException("A serializer has already been registered for the state; re-registration is not allowed.");
			}

			TypeSerializerSchemaCompatibility<T> result = previousSerializerSnapshot.resolveSchemaCompatibility(newSerializer);
			if (result.isIncompatible()) {
				invalidateCurrentSchemaSerializerAccess();
			}
			if (result.isCompatibleWithReconfiguredSerializer()) {
				this.registeredSerializer = result.getReconfiguredSerializer();
			} else {
				this.registeredSerializer = newSerializer;
			}
			return result;
		}
	}

	/**
	 * Implementation of the {@link StateSerializerProvider} for the case where a new state
	 * serializer instance is registered first, before any snapshots of the previous state serializer
	 * is obtained (hence, the naming "eagerly" registered).
	 */
	private static class EagerlyRegisteredStateSerializerProvider<T> extends StateSerializerProvider<T> {

		EagerlyRegisteredStateSerializerProvider(TypeSerializer<T> registeredStateSerializer) {
			super(Preconditions.checkNotNull(registeredStateSerializer));
		}

		@Nonnull
		@Override
		public TypeSerializerSchemaCompatibility<T> registerNewSerializerForRestoredState(TypeSerializer<T> newSerializer) {
			throw new UnsupportedOperationException("A serializer has already been registered for the state; re-registration is not allowed.");
		}

	}
}
