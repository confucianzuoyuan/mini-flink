package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeutils.TypeComparator;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.*;

import java.lang.reflect.Constructor;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkNotNull;

@Public
public class BasicTypeInfo<T> extends TypeInformation<T> implements AtomicType<T> {

	private static final long serialVersionUID = -430955220409131770L;

	public static final BasicTypeInfo<String> STRING_TYPE_INFO = new BasicTypeInfo<>(String.class, new Class<?>[]{}, StringSerializer.INSTANCE, StringComparator.class);
	public static final BasicTypeInfo<Boolean> BOOLEAN_TYPE_INFO = new BasicTypeInfo<>(Boolean.class, new Class<?>[]{}, BooleanSerializer.INSTANCE, BooleanComparator.class);
	public static final BasicTypeInfo<Integer> INT_TYPE_INFO = new IntegerTypeInfo<>(Integer.class, new Class<?>[]{Long.class, Float.class, Double.class, Character.class}, IntSerializer.INSTANCE, IntComparator.class);
	public static final BasicTypeInfo<Long> LONG_TYPE_INFO = new IntegerTypeInfo<>(Long.class, new Class<?>[]{Float.class, Double.class, Character.class}, LongSerializer.INSTANCE, LongComparator.class);
	public static final BasicTypeInfo<Character> CHAR_TYPE_INFO = new BasicTypeInfo<>(Character.class, new Class<?>[]{}, CharSerializer.INSTANCE, CharComparator.class);
	public static final BasicTypeInfo<Void> VOID_TYPE_INFO = new BasicTypeInfo<>(Void.class, new Class<?>[]{}, VoidSerializer.INSTANCE, null);
	public static final BasicTypeInfo<Instant> INSTANT_TYPE_INFO = new BasicTypeInfo<>(Instant.class, new Class<?>[]{}, InstantSerializer.INSTANCE, InstantComparator.class);


	// --------------------------------------------------------------------------------------------

	private final Class<T> clazz;

	private final TypeSerializer<T> serializer;

	private final Class<?>[] possibleCastTargetTypes;

	private final Class<? extends TypeComparator<T>> comparatorClass;

	protected BasicTypeInfo(Class<T> clazz, Class<?>[] possibleCastTargetTypes, TypeSerializer<T> serializer, Class<? extends TypeComparator<T>> comparatorClass) {
		this.clazz = checkNotNull(clazz);
		this.possibleCastTargetTypes = checkNotNull(possibleCastTargetTypes);
		this.serializer = checkNotNull(serializer);
		// comparator can be null as in VOID_TYPE_INFO
		this.comparatorClass = comparatorClass;
	}

	@Override
	@PublicEvolving
	public boolean isBasicType() {
		return true;
	}

	@Override
	@PublicEvolving
	public boolean isTupleType() {
		return false;
	}

	@Override
	@PublicEvolving
	public int getArity() {
		return 1;
	}

	@Override
	@PublicEvolving
	public int getTotalFields() {
		return 1;
	}

	@Override
	@PublicEvolving
	public Class<T> getTypeClass() {
		return this.clazz;
	}

	@Override
	@PublicEvolving
	public boolean isKeyType() {
		return true;
	}

	@Override
	@PublicEvolving
	public TypeSerializer<T> createSerializer(ExecutionConfig executionConfig) {
		return this.serializer;
	}

	@Override
	@PublicEvolving
	public TypeComparator<T> createComparator(boolean sortOrderAscending, ExecutionConfig executionConfig) {
		if (comparatorClass != null) {
			return instantiateComparator(comparatorClass, sortOrderAscending);
		} else {
			throw new InvalidTypesException("The type " + clazz.getSimpleName() + " cannot be used as a key.");
		}
	}

	// --------------------------------------------------------------------------------------------

	@Override
	public int hashCode() {
		return (31 * Objects.hash(clazz, serializer, comparatorClass)) + Arrays.hashCode(possibleCastTargetTypes);
	}

	@Override
	public boolean canEqual(Object obj) {
		return obj instanceof BasicTypeInfo;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BasicTypeInfo) {
			@SuppressWarnings("unchecked")
			BasicTypeInfo<T> other = (BasicTypeInfo<T>) obj;

			return other.canEqual(this) &&
				this.clazz == other.clazz &&
				serializer.equals(other.serializer) &&
				this.comparatorClass == other.comparatorClass;
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return clazz.getSimpleName();
	}

	// --------------------------------------------------------------------------------------------

	@PublicEvolving
	public static <X> BasicTypeInfo<X> getInfoFor(Class<X> type) {
		if (type == null) {
			throw new NullPointerException();
		}

		@SuppressWarnings("unchecked")
		BasicTypeInfo<X> info = (BasicTypeInfo<X>) TYPES.get(type);
		return info;
	}

	private static <X> TypeComparator<X> instantiateComparator(Class<? extends TypeComparator<X>> comparatorClass, boolean ascendingOrder) {
		try {
			Constructor<? extends TypeComparator<X>> constructor = comparatorClass.getConstructor(boolean.class);
			return constructor.newInstance(ascendingOrder);
		}
		catch (Exception e) {
			throw new RuntimeException("Could not initialize basic comparator " + comparatorClass.getName(), e);
		}
	}

	private static final Map<Class<?>, BasicTypeInfo<?>> TYPES = new HashMap<Class<?>, BasicTypeInfo<?>>();

	static {
		TYPES.put(String.class, STRING_TYPE_INFO);
		TYPES.put(Boolean.class, BOOLEAN_TYPE_INFO);
		TYPES.put(boolean.class, BOOLEAN_TYPE_INFO);
		TYPES.put(Integer.class, INT_TYPE_INFO);
		TYPES.put(int.class, INT_TYPE_INFO);
		TYPES.put(Long.class, LONG_TYPE_INFO);
		TYPES.put(long.class, LONG_TYPE_INFO);
		TYPES.put(Character.class, CHAR_TYPE_INFO);
		TYPES.put(char.class, CHAR_TYPE_INFO);
		TYPES.put(Void.class, VOID_TYPE_INFO);
		TYPES.put(void.class, VOID_TYPE_INFO);
		TYPES.put(Instant.class, INSTANT_TYPE_INFO);
	}
}
