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

package org.apache.flink.api.common.typeinfo;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.api.java.typeutils.ValueTypeInfo;
import org.apache.flink.types.Value;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * This class gives access to the type information of the most common types for which Flink
 * has built-in serializers and comparators.
 *
 * <p>In many cases, Flink tries to analyze generic signatures of functions to determine return
 * types automatically. This class is intended for cases where type information has to be
 * supplied manually or cases where automatic type inference results in an inefficient type.
 *
 * <p>Please note that the Scala API and Table API have dedicated Types classes.
 * (See <code>org.apache.flink.api.scala.Types</code> and <code>org.apache.flink.table.api.Types</code>)
 *
 * <p>A more convenient alternative might be a {@link TypeHint}.
 *
 * @see TypeInformation#of(Class) specify type information based on a class that will be analyzed
 * @see TypeInformation#of(TypeHint) specify type information based on a {@link TypeHint}
 */
@PublicEvolving
public class Types {

	/**
	 * Returns type information for {@link java.lang.Void}. Does not support a null value.
	 */
	public static final TypeInformation<Void> VOID = BasicTypeInfo.VOID_TYPE_INFO;

	/**
	 * Returns type information for {@link java.lang.String}. Supports a null value.
	 */
	public static final TypeInformation<String> STRING = BasicTypeInfo.STRING_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>byte</code> and {@link java.lang.Byte}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Byte> BYTE = BasicTypeInfo.BYTE_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>boolean</code> and {@link java.lang.Boolean}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Boolean> BOOLEAN = BasicTypeInfo.BOOLEAN_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>short</code> and {@link java.lang.Short}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Short> SHORT = BasicTypeInfo.SHORT_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>int</code> and {@link java.lang.Integer}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Integer> INT = BasicTypeInfo.INT_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>long</code> and {@link java.lang.Long}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Long> LONG = BasicTypeInfo.LONG_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>float</code> and {@link java.lang.Float}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Float> FLOAT = BasicTypeInfo.FLOAT_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>double</code> and {@link java.lang.Double}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Double> DOUBLE = BasicTypeInfo.DOUBLE_TYPE_INFO;

	/**
	 * Returns type information for both a primitive <code>char</code> and {@link java.lang.Character}.
	 * Does not support a null value.
	 */
	public static final TypeInformation<Character> CHAR = BasicTypeInfo.CHAR_TYPE_INFO;

	/**
	 * Returns type information for {@link java.math.BigDecimal}. Supports a null value.
	 */
	public static final TypeInformation<BigDecimal> BIG_DEC = BasicTypeInfo.BIG_DEC_TYPE_INFO;

	/**
	 * Returns type information for {@link java.math.BigInteger}. Supports a null value.
	 */
	public static final TypeInformation<BigInteger> BIG_INT = BasicTypeInfo.BIG_INT_TYPE_INFO;

	//CHECKSTYLE.OFF: MethodName

	/**
	 * Returns type information for a POJO (Plain Old Java Object).
	 *
	 * <p>A POJO class is public and standalone (no non-static inner class). It has a public no-argument
	 * constructor. All non-static, non-transient fields in the class (and all superclasses) are either public
	 * (and non-final) or have a public getter and a setter method that follows the Java beans naming
	 * conventions for getters and setters.
	 *
	 * <p>A POJO is a fixed-length and null-aware composite type. Every field can be null independent
	 * of the field's type.
	 *
	 * <p>The generic types for all fields of the POJO can be defined in a hierarchy of subclasses.
	 *
	 * <p>If Flink's type analyzer is unable to extract a valid POJO type information with
	 * type information for all fields, an {@link org.apache.flink.api.common.functions.InvalidTypesException}
	 * is thrown. Alternatively, you can use {@link Types#POJO(Class, Map)} to specify all fields manually.
	 *
	 * @param pojoClass POJO class to be analyzed by Flink
	 */
	public static <T> TypeInformation<T> POJO(Class<T> pojoClass) {
		final TypeInformation<T> ti = TypeExtractor.createTypeInfo(pojoClass);
		if (ti instanceof PojoTypeInfo) {
			return ti;
		}
		throw new InvalidTypesException("POJO type expected but was: " + ti);
	}

	/**
	 * Returns type information for a POJO (Plain Old Java Object) and allows to specify all fields manually.
	 *
	 * <p>A POJO class is public and standalone (no non-static inner class). It has a public no-argument
	 * constructor. All non-static, non-transient fields in the class (and all superclasses) are either public
	 * (and non-final) or have a public getter and a setter method that follows the Java beans naming
	 * conventions for getters and setters.
	 *
	 * <p>A POJO is a fixed-length, null-aware composite type with non-deterministic field order. Every field
	 * can be null independent of the field's type.
	 *
	 * <p>The generic types for all fields of the POJO can be defined in a hierarchy of subclasses.
	 *
	 * <p>If Flink's type analyzer is unable to extract a POJO field, an
	 * {@link org.apache.flink.api.common.functions.InvalidTypesException} is thrown.
	 *
	 * <p><strong>Note:</strong> In most cases the type information of fields can be determined automatically,
	 * we recommend to use {@link Types#POJO(Class)}.
	 *
	 * @param pojoClass POJO class
	 * @param fields map of fields that map a name to type information. The map key is the name of
	 *               the field and the value is its type.
	 */
	public static <T> TypeInformation<T> POJO(Class<T> pojoClass, Map<String, TypeInformation<?>> fields) {
		final List<PojoField> pojoFields = new ArrayList<>(fields.size());
		for (Map.Entry<String, TypeInformation<?>> field : fields.entrySet()) {
			final Field f = TypeExtractor.getDeclaredField(pojoClass, field.getKey());
			if (f == null) {
				throw new InvalidTypesException("Field '" + field.getKey() + "'could not be accessed.");
			}
			pojoFields.add(new PojoField(f, field.getValue()));
		}

		return new PojoTypeInfo<>(pojoClass, pojoFields);
	}

	public static TypeInformation<?> PRIMITIVE_ARRAY(TypeInformation<?> elementType) {
		throw new IllegalArgumentException("Invalid element type for a primitive array.");
	}

	public static <V extends Value> TypeInformation<V> VALUE(Class<V> valueType) {
		return new ValueTypeInfo<>(valueType);
	}

}
