package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

import static org.apache.flink.api.common.typeutils.TypeSerializerConfigSnapshot.ADAPTER_VERSION;

@PublicEvolving
public interface TypeSerializerSnapshot<T> {

	int getCurrentVersion();

	void writeSnapshot(DataOutputView out) throws IOException;

	void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader) throws IOException;

	TypeSerializer<T> restoreSerializer();

	/**
	 * Checks a new serializer's compatibility to read data written by the prior serializer.
	 *
	 * <p>When a checkpoint/savepoint is restored, this method checks whether the serialization
	 * format of the data in the checkpoint/savepoint is compatible for the format of the serializer used by the
	 * program that restores the checkpoint/savepoint. The outcome can be that the serialization format is
	 * compatible, that the program's serializer needs to reconfigure itself (meaning to incorporate some
	 * information from the TypeSerializerSnapshot to be compatible), that the format is outright incompatible,
	 * or that a migration needed. In the latter case, the TypeSerializerSnapshot produces a serializer to
	 * deserialize the data, and the restoring program's serializer re-serializes the data, thus converting
	 * the format during the restore operation.
	 *
	 * @param newSerializer the new serializer to check.
	 *
	 * @return the serializer compatibility result.
	 */
	TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(TypeSerializer<T> newSerializer);

	static void writeVersionedSnapshot(DataOutputView out, TypeSerializerSnapshot<?> snapshot) throws IOException {
		out.writeUTF(snapshot.getClass().getName());
		out.writeInt(snapshot.getCurrentVersion());
		snapshot.writeSnapshot(out);
	}

	static <T> TypeSerializerSnapshot<T> readVersionedSnapshot(DataInputView in, ClassLoader cl) throws IOException {
		final TypeSerializerSnapshot<T> snapshot =
				TypeSerializerSnapshotSerializationUtil.readAndInstantiateSnapshotClass(in, cl);
		return snapshot;
	}
}
