package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

@PublicEvolving
public interface TypeSerializerSnapshot<T> {

	int getCurrentVersion();

	void writeSnapshot(DataOutputView out) throws IOException;

	void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException;

	TypeSerializer<T> restoreSerializer();

	TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer);

	static void writeVersionedSnapshot(DataOutputView out, TypeSerializerSnapshot<?> snapshot) throws IOException {
	}

	static <T> TypeSerializerSnapshot<T> readVersionedSnapshot(DataInputView in, ClassLoader cl) throws IOException {
		final TypeSerializerSnapshot<T> snapshot =
				TypeSerializerSnapshotSerializationUtil.readAndInstantiateSnapshotClass(in, cl);
		return snapshot;
	}
}
