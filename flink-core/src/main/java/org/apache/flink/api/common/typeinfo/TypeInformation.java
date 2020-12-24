package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;

@Public
public abstract class TypeInformation<T> implements Serializable {

	private static final long serialVersionUID = -7742311969684489493L;

	@PublicEvolving
	public abstract boolean isBasicType();

	@PublicEvolving
	public abstract boolean isTupleType();

	@PublicEvolving
	public abstract int getArity();

	@PublicEvolving
	public abstract int getTotalFields();

	@PublicEvolving
	public abstract Class<T> getTypeClass();

	@PublicEvolving
	public Map<String, TypeInformation<?>> getGenericParameters() {
		// return an empty map as the default implementation
		return Collections.emptyMap();
	}

	@PublicEvolving
	public abstract boolean isKeyType();

	@PublicEvolving
	public boolean isSortKeyType() {
		return isKeyType();
	}

	@PublicEvolving
	public abstract TypeSerializer<T> createSerializer(ExecutionConfig config);

	@Override
	public abstract String toString();

	@Override
	public abstract boolean equals(Object obj);

	@Override
	public abstract int hashCode();

	public abstract boolean canEqual(Object obj);

	public static <T> TypeInformation<T> of(Class<T> typeClass) {
		try {
			return TypeExtractor.createTypeInfo(typeClass);
		}
		catch (InvalidTypesException e) {
			throw new FlinkRuntimeException(
					"Cannot extract TypeInformation from Class alone, because generic parameters are missing. " +
					"Please use TypeInformation.of(TypeHint) instead, or another equivalent method in the API that " +
					"accepts a TypeHint instead of a Class. " +
					"For example for a Tuple2<Long, String> pass a 'new TypeHint<Tuple2<Long, String>>(){}'.");
		}
	}

	public static <T> TypeInformation<T> of(TypeHint<T> typeHint) {
		return typeHint.getTypeInfo();
	}
}
