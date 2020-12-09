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

package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.io.VersionedIOReadableWritable;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.Preconditions;

import java.io.IOException;

@PublicEvolving
@Deprecated
public abstract class TypeSerializerConfigSnapshot<T> extends VersionedIOReadableWritable implements TypeSerializerSnapshot<T> {

	/** Version / Magic number for the format that bridges between the old and new interface. */
	static final int ADAPTER_VERSION = 0x7a53c4f0;

	/** The user code class loader; only relevant if this configuration instance was deserialized from binary form. */
	private ClassLoader userCodeClassLoader;

	/** The originating serializer of this configuration snapshot. */
	private TypeSerializer<T> serializer;

	/**
	 * Set the originating serializer of this configuration snapshot.
	 */
	@Internal
	public final void setPriorSerializer(TypeSerializer<T> serializer) {
		this.serializer = Preconditions.checkNotNull(serializer);
	}

	@Internal
	public final void setUserCodeClassLoader(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = Preconditions.checkNotNull(userCodeClassLoader);
	}

	@Internal
	public final ClassLoader getUserCodeClassLoader() {
		return userCodeClassLoader;
	}

	// ----------------------------------------------------------------------------
	//  Implementation of the TypeSerializerSnapshot interface
	// ----------------------------------------------------------------------------

	@Override
	public final int getCurrentVersion() {
		return ADAPTER_VERSION;
	}

	@Override
	public final void writeSnapshot(DataOutputView out) throws IOException {
	}

	@Override
	public final void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException {
	}

	@Override
	public final TypeSerializer<T> restoreSerializer() {
		return this.serializer;
	}

	@Override
	public TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(
			TypeSerializer<T> newSerializer) {
		return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
	}

	/**
	 * This interface assists with the migration path to the new serialization abstraction.
	 *
	 * <p>This interface can be used for cases where the `ensureCompatibility` method cannot be removed.
	 * Implementing this interface by your {@link TypeSerializer} would allow it to "redirect" the
	 * compatibility check to the corresponding {code TypeSerializerSnapshot} class.
	 *
	 * <p>Please note that if it is possible to directly override
	 * {@link TypeSerializerConfigSnapshot#resolveSchemaCompatibility} and preform the redirection logic there,
	 * then that is the preferred way. This interface is useful for cases where there is not enough information,
	 * and the new serializer should assist with the redirection.
	 */
	@Internal
	public interface SelfResolvingTypeSerializer<E> {

		/**
		 * Resolve Schema Compatibility.
		 *
		 * <p>Given an instance of a {@code TypeSerializerConfigSnapshot} this method should redirect the compatibility
		 * check to the new {@code TypeSerializerSnapshot} class along with the relevant information as present in the
		 * given {@code deprecatedConfigSnapshot}.
		 *
		 * @param deprecatedConfigSnapshot the not yet migrated config snapshot class.
		 * @return the compatibility result of the {@code deprecatedConfigSnapshot} with {@code this} serializer.
		 */
		TypeSerializerSchemaCompatibility<E> resolveSchemaCompatibilityViaRedirectingToNewSnapshotClass(
			TypeSerializerConfigSnapshot<E> deprecatedConfigSnapshot);
	}
}
