
package org.apache.flink.api.common.typeutils;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;
import java.io.Serializable;

@PublicEvolving
public abstract class TypeSerializer<T> implements Serializable {
	
	private static final long serialVersionUID = 1L;

	// --------------------------------------------------------------------------------------------
	// General information about the type and the serializer
	// --------------------------------------------------------------------------------------------

	/**
	 * Gets whether the type is an immutable type.
	 * 
	 * @return True, if the type is immutable.
	 */
	public abstract boolean isImmutableType();
	
	public abstract TypeSerializer<T> duplicate();

	public abstract T createInstance();

	public abstract T copy(T from);
	
	public abstract T copy(T from, T reuse);
	
	public abstract int getLength();
	
	// --------------------------------------------------------------------------------------------

	/**
	 * Serializes the given record to the given target output view.
	 * 
	 * @param record The record to serialize.
	 * @param target The output view to write the serialized data to.
	 * 
	 * @throws IOException Thrown, if the serialization encountered an I/O related error. Typically raised by the
	 *                     output view, which may have an underlying I/O channel to which it delegates.
	 */
	public abstract void serialize(T record, DataOutputView target) throws IOException;

	/**
	 * De-serializes a record from the given source input view.
	 * 
	 * @param source The input view from which to read the data.
	 * @return The deserialized element.
	 * 
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
	 *                     input view, which may have an underlying I/O channel from which it reads.
	 */
	public abstract T deserialize(DataInputView source) throws IOException;
	
	/**
	 * De-serializes a record from the given source input view into the given reuse record instance if mutable.
	 * 
	 * @param reuse The record instance into which to de-serialize the data.
	 * @param source The input view from which to read the data.
	 * @return The deserialized element.
	 * 
	 * @throws IOException Thrown, if the de-serialization encountered an I/O related error. Typically raised by the
	 *                     input view, which may have an underlying I/O channel from which it reads.
	 */
	public abstract T deserialize(T reuse, DataInputView source) throws IOException;
	
	/**
	 * Copies exactly one record from the source input view to the target output view. Whether this operation
	 * works on binary data or partially de-serializes the record to determine its length (such as for records
	 * of variable length) is up to the implementer. Binary copies are typically faster. A copy of a record containing
	 * two integer numbers (8 bytes total) is most efficiently implemented as
	 * {@code target.write(source, 8);}.
	 *  
	 * @param source The input view from which to read the record.
	 * @param target The target output view to which to write the record.
	 * 
	 * @throws IOException Thrown if any of the two views raises an exception.
	 */
	public abstract void copy(DataInputView source, DataOutputView target) throws IOException;

	public abstract boolean equals(Object obj);

	public abstract int hashCode();

}
