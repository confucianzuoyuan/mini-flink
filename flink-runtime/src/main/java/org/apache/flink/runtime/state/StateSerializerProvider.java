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
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import static org.apache.flink.util.Preconditions.checkState;

@Internal
public abstract class StateSerializerProvider<T> {

	@Nullable
	TypeSerializer<T> registeredSerializer;

	@Nullable
	private TypeSerializer<T> cachedRestoredSerializer;

	private boolean isRegisteredWithIncompatibleSerializer = false;

	public static <T> StateSerializerProvider<T> fromNewRegisteredSerializer(TypeSerializer<T> registeredStateSerializer) {
		return new EagerlyRegisteredStateSerializerProvider<>(registeredStateSerializer);
	}

	private StateSerializerProvider(@Nonnull TypeSerializer<T> stateSerializer) {
		this.registeredSerializer = stateSerializer;
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
		return cachedRestoredSerializer;
	}



	protected final void invalidateCurrentSchemaSerializerAccess() {
		this.isRegisteredWithIncompatibleSerializer = true;
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
	}
}
