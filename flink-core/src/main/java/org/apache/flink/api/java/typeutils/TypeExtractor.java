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

package org.apache.flink.api.java.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple0;
import org.apache.flink.api.java.typeutils.TypeExtractionUtils.*;
import org.apache.flink.types.Value;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.*;

import static org.apache.flink.api.java.typeutils.TypeExtractionUtils.*;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A utility for reflection analysis on classes, to determine the return type of implementations of transformation
 * functions.
 *
 * <p>NOTES FOR USERS OF THIS CLASS:
 * Automatic type extraction is a hacky business that depends on a lot of variables such as generics,
 * compiler, interfaces, etc. The type extraction fails regularly with either {@link MissingTypeInfo} or
 * hard exceptions. Whenever you use methods of this class, make sure to provide a way to pass custom
 * type information as a fallback.
 */
@Public
public class TypeExtractor {

	/*
	 * NOTE: Most methods of the TypeExtractor work with a so-called "typeHierarchy".
	 * The type hierarchy describes all types (Classes, ParameterizedTypes, TypeVariables etc. ) and intermediate
	 * types from a given type of a function or type (e.g. MyMapper, Tuple2) until a current type
	 * (depends on the method, e.g. MyPojoFieldType).
	 *
	 * Thus, it fully qualifies types until tuple/POJO field level.
	 *
	 * A typical typeHierarchy could look like:
	 *
	 * UDF: MyMapFunction.class
	 * top-level UDF: MyMapFunctionBase.class
	 * RichMapFunction: RichMapFunction.class
	 * MapFunction: MapFunction.class
	 * Function's OUT: Tuple1<MyPojo>
	 * user-defined POJO: MyPojo.class
	 * user-defined top-level POJO: MyPojoBase.class
	 * POJO field: Tuple1<String>
	 * Field type: String.class
	 *
	 */

	/** The name of the class representing Hadoop's writable */
	private static final String HADOOP_WRITABLE_CLASS = "org.apache.hadoop.io.Writable";

	private static final String HADOOP_WRITABLE_TYPEINFO_CLASS = "org.apache.flink.api.java.typeutils.WritableTypeInfo";

	private static final String AVRO_SPECIFIC_RECORD_BASE_CLASS = "org.apache.avro.specific.SpecificRecordBase";

	private static final Logger LOG = LoggerFactory.getLogger(TypeExtractor.class);

	public static final int[] NO_INDEX = new int[] {};

	protected TypeExtractor() {
		// only create instances for special use cases
	}

	// --------------------------------------------------------------------------------------------
	//  TypeInfoFactory registry
	// --------------------------------------------------------------------------------------------

	private static Map<Type, Class<? extends TypeInfoFactory>> registeredTypeInfoFactories = new HashMap<>();

	/**
	 * Registers a type information factory globally for a certain type. Every following type extraction
	 * operation will use the provided factory for this type. The factory will have highest precedence
	 * for this type. In a hierarchy of types the registered factory has higher precedence than annotations
	 * at the same level but lower precedence than factories defined down the hierarchy.
	 *
	 * @param t type for which a new factory is registered
	 * @param factory type information factory that will produce {@link TypeInformation}
	 */
	private static void registerFactory(Type t, Class<? extends TypeInfoFactory> factory) {
		Preconditions.checkNotNull(t, "Type parameter must not be null.");
		Preconditions.checkNotNull(factory, "Factory parameter must not be null.");

		if (!TypeInfoFactory.class.isAssignableFrom(factory)) {
			throw new IllegalArgumentException("Class is not a TypeInfoFactory.");
		}
		if (registeredTypeInfoFactories.containsKey(t)) {
			throw new InvalidTypesException("A TypeInfoFactory for type '" + t + "' is already registered.");
		}
		registeredTypeInfoFactories.put(t, factory);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getMapReturnTypes(MapFunction<IN, OUT> mapInterface, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			(Function) mapInterface,
			MapFunction.class,
			0,
			1,
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}


	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getFlatMapReturnTypes(FlatMapFunction<IN, OUT> flatMapInterface, TypeInformation<IN> inType,
			String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			(Function) flatMapInterface,
			FlatMapFunction.class,
			0,
			1,
			new int[]{1, 0},
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(KeySelector<IN, OUT> selectorInterface, TypeInformation<IN> inType) {
		return getKeySelectorTypes(selectorInterface, inType, null, false);
	}

	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getKeySelectorTypes(KeySelector<IN, OUT> selectorInterface,
			TypeInformation<IN> inType, String functionName, boolean allowMissing)
	{
		return getUnaryOperatorReturnType(
			(Function) selectorInterface,
			KeySelector.class,
			0,
			1,
			NO_INDEX,
			inType,
			functionName,
			allowMissing);
	}

	@PublicEvolving
	public static <T> TypeInformation<T> getPartitionerTypes(Partitioner<T> partitioner) {
		return getPartitionerTypes(partitioner, null, false);
	}

	@PublicEvolving
	public static <T> TypeInformation<T> getPartitionerTypes(
		Partitioner<T> partitioner,
		String functionName,
		boolean allowMissing) {

		return getUnaryOperatorReturnType(
			partitioner,
			Partitioner.class,
			-1,
			0,
			new int[]{0},
			null,
			functionName,
			allowMissing);
	}


	// --------------------------------------------------------------------------------------------
	//  Generic extraction methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the unary operator's return type.
	 *
	 * <p>This method can extract a type in 4 different ways:
	 *
	 * <p>1. By using the generics of the base class like MyFunction<X, Y, Z, IN, OUT>.
	 *    This is what outputTypeArgumentIndex (in this example "4") is good for.
	 *
	 * <p>2. By using input type inference SubMyFunction<T, String, String, String, T>.
	 *    This is what inputTypeArgumentIndex (in this example "0") and inType is good for.
	 *
	 * <p>3. By using the static method that a compiler generates for Java lambdas.
	 *    This is what lambdaOutputTypeArgumentIndices is good for. Given that MyFunction has
	 *    the following single abstract method:
	 *
	 * <pre>
	 * <code>
	 * void apply(IN value, Collector<OUT> value)
	 * </code>
	 * </pre>
	 *
	 * <p> Lambda type indices allow the extraction of a type from lambdas. To extract the
	 *     output type <b>OUT</b> from the function one should pass {@code new int[] {1, 0}}.
	 *     "1" for selecting the parameter and 0 for the first generic in this type.
	 *     Use {@code TypeExtractor.NO_INDEX} for selecting the return type of the lambda for
	 *     extraction or if the class cannot be a lambda because it is not a single abstract
	 *     method interface.
	 *
	 * <p>4. By using interfaces such as {@link TypeInfoFactory} or {@link ResultTypeQueryable}.
	 *
	 * <p>See also comments in the header of this class.
	 *
	 * @param function Function to extract the return type from
	 * @param baseClass Base class of the function
	 * @param inputTypeArgumentIndex Index of input generic type in the base class specification (ignored if inType is null)
	 * @param outputTypeArgumentIndex Index of output generic type in the base class specification
	 * @param lambdaOutputTypeArgumentIndices Table of indices of the type argument specifying the input type. See example.
	 * @param inType Type of the input elements (In case of an iterable, it is the element type) or null
	 * @param functionName Function name
	 * @param allowMissing Can the type information be missing (this generates a MissingTypeInfo for postponing an exception)
	 * @param <IN> Input type
	 * @param <OUT> Output type
	 * @return TypeInformation of the return type of the function
	 */
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <IN, OUT> TypeInformation<OUT> getUnaryOperatorReturnType(
		Function function,
		Class<?> baseClass,
		int inputTypeArgumentIndex,
		int outputTypeArgumentIndex,
		int[] lambdaOutputTypeArgumentIndices,
		TypeInformation<IN> inType,
		String functionName,
		boolean allowMissing) {

		Preconditions.checkArgument(inType == null || inputTypeArgumentIndex >= 0, "Input type argument index was not provided");
		Preconditions.checkArgument(outputTypeArgumentIndex >= 0, "Output type argument index was not provided");
		Preconditions.checkArgument(
			lambdaOutputTypeArgumentIndices != null,
			"Indices for output type arguments within lambda not provided");

		// explicit result type has highest precedence
		if (function instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) function).getProducedType();
		}

		// perform extraction
		try {
			final LambdaExecutable exec;
			try {
				exec = checkAndExtractLambda(function);
			} catch (TypeExtractionException e) {
				throw new InvalidTypesException("Internal error occurred.", e);
			}
			if (exec != null) {

				// parameters must be accessed from behind, since JVM can add additional parameters e.g. when using local variables inside lambda function
				// paramLen is the total number of parameters of the provided lambda, it includes parameters added through closure
				final int paramLen = exec.getParameterTypes().length;

				final Method sam = TypeExtractionUtils.getSingleAbstractMethod(baseClass);

				// number of parameters the SAM of implemented interface has; the parameter indexing applies to this range
				final int baseParametersLen = sam.getParameterTypes().length;

				final Type output;
				if (lambdaOutputTypeArgumentIndices.length > 0) {
					output = TypeExtractionUtils.extractTypeFromLambda(
						baseClass,
						exec,
						lambdaOutputTypeArgumentIndices,
						paramLen,
						baseParametersLen);
				} else {
					output = exec.getReturnType();
					TypeExtractionUtils.validateLambdaType(baseClass, output);
				}

				return new TypeExtractor().privateCreateTypeInfo(output, inType, null);
			} else {
				if (inType != null) {
					validateInputType(baseClass, function.getClass(), inputTypeArgumentIndex, inType);
				}
				return new TypeExtractor().privateCreateTypeInfo(baseClass, function.getClass(), outputTypeArgumentIndex, inType, null);
			}
		}
		catch (InvalidTypesException e) {
			if (allowMissing) {
				return (TypeInformation<OUT>) new MissingTypeInfo(functionName != null ? functionName : function.toString(), e);
			} else {
				throw e;
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Create type information
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("unchecked")
	public static <T> TypeInformation<T> createTypeInfo(Class<T> type) {
		return (TypeInformation<T>) createTypeInfo((Type) type);
	}

	public static TypeInformation<?> createTypeInfo(Type t) {
		TypeInformation<?> ti = new TypeExtractor().privateCreateTypeInfo(t);
		if (ti == null) {
			throw new InvalidTypesException("Could not extract type information.");
		}
		return ti;
	}

	/**
	 * Creates a {@link TypeInformation} from the given parameters.
	 *
	 * If the given {@code instance} implements {@link ResultTypeQueryable}, its information
	 * is used to determine the type information. Otherwise, the type information is derived
	 * based on the given class information.
	 *
	 * @param instance			instance to determine type information for
	 * @param baseClass			base class of {@code instance}
	 * @param clazz				class of {@code instance}
	 * @param returnParamPos	index of the return type in the type arguments of {@code clazz}
	 * @param <OUT>				output type
	 * @return type information
	 */
	@SuppressWarnings("unchecked")
	@PublicEvolving
	public static <OUT> TypeInformation<OUT> createTypeInfo(Object instance, Class<?> baseClass, Class<?> clazz, int returnParamPos) {
		if (instance instanceof ResultTypeQueryable) {
			return ((ResultTypeQueryable<OUT>) instance).getProducedType();
		} else {
			return createTypeInfo(baseClass, clazz, returnParamPos, null, null);
		}
	}

	@PublicEvolving
	public static <IN1, IN2, OUT> TypeInformation<OUT> createTypeInfo(Class<?> baseClass, Class<?> clazz, int returnParamPos,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		TypeInformation<OUT> ti =  new TypeExtractor().privateCreateTypeInfo(baseClass, clazz, returnParamPos, in1Type, in2Type);
		if (ti == null) {
			throw new InvalidTypesException("Could not extract type information.");
		}
		return ti;
	}

	// ----------------------------------- private methods ----------------------------------------

	private TypeInformation<?> privateCreateTypeInfo(Type t) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();
		typeHierarchy.add(t);
		return createTypeInfoWithTypeHierarchy(typeHierarchy, t, null, null);
	}

	// for (Rich)Functions
	@SuppressWarnings("unchecked")
	private <IN1, IN2, OUT> TypeInformation<OUT> privateCreateTypeInfo(Class<?> baseClass, Class<?> clazz, int returnParamPos,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();
		Type returnType = getParameterType(baseClass, typeHierarchy, clazz, returnParamPos);

		TypeInformation<OUT> typeInfo;

		// return type is a variable -> try to get the type info from the input directly
		if (returnType instanceof TypeVariable<?>) {
			typeInfo = (TypeInformation<OUT>) createTypeInfoFromInputs((TypeVariable<?>) returnType, typeHierarchy, in1Type, in2Type);

			if (typeInfo != null) {
				return typeInfo;
			}
		}

		// get info from hierarchy
		return (TypeInformation<OUT>) createTypeInfoWithTypeHierarchy(typeHierarchy, returnType, in1Type, in2Type);
	}

	// for LambdaFunctions
	@SuppressWarnings("unchecked")
	private <IN1, IN2, OUT> TypeInformation<OUT> privateCreateTypeInfo(Type returnType, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();

		// get info from hierarchy
		return (TypeInformation<OUT>) createTypeInfoWithTypeHierarchy(typeHierarchy, returnType, in1Type, in2Type);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <IN1, IN2, OUT> TypeInformation<OUT> createTypeInfoWithTypeHierarchy(ArrayList<Type> typeHierarchy, Type t,
			TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {

		// check if type information can be created using a type factory
		final TypeInformation<OUT> typeFromFactory = createTypeInfoFromFactory(t, typeHierarchy, in1Type, in2Type);
		if (typeFromFactory != null) {
			return typeFromFactory;
		}
		// check if type is a subclass of tuple
		else if (isClassType(t) && Tuple.class.isAssignableFrom(typeToClass(t))) {
			Type curT = t;

			// do not allow usage of Tuple as type
			if (typeToClass(t).equals(Tuple.class)) {
				throw new InvalidTypesException(
						"Usage of class Tuple as a type is not allowed. Use a concrete subclass (e.g. Tuple1, Tuple2, etc.) instead.");
			}

			// go up the hierarchy until we reach immediate child of Tuple (with or without generics)
			// collect the types while moving up for a later top-down
			while (!(isClassType(curT) && typeToClass(curT).getSuperclass().equals(Tuple.class))) {
				typeHierarchy.add(curT);
				curT = typeToClass(curT).getGenericSuperclass();
			}

			if(curT == Tuple0.class) {
				return new TupleTypeInfo(Tuple0.class);
			}

			// check if immediate child of Tuple has generics
			if (curT instanceof Class<?>) {
				throw new InvalidTypesException("Tuple needs to be parameterized by using generics.");
			}

			typeHierarchy.add(curT);

			// create the type information for the subtypes
			final TypeInformation<?>[] subTypesInfo = createSubTypesInfo(t, (ParameterizedType) curT, typeHierarchy, in1Type, in2Type, false);
			// type needs to be treated a pojo due to additional fields
			// return tuple info
			return new TupleTypeInfo(typeToClass(t), subTypesInfo);

		}
		// type depends on another type
		// e.g. class MyMapper<E> extends MapFunction<String, E>
		else if (t instanceof TypeVariable) {
			Type typeVar = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) t);

			if (!(typeVar instanceof TypeVariable)) {
				return createTypeInfoWithTypeHierarchy(typeHierarchy, typeVar, in1Type, in2Type);
			}
			// try to derive the type info of the TypeVariable from the immediate base child input as a last attempt
			else {
				TypeInformation<OUT> typeInfo = (TypeInformation<OUT>) createTypeInfoFromInputs((TypeVariable<?>) t, typeHierarchy, in1Type, in2Type);
				if (typeInfo != null) {
					return typeInfo;
				} else {
					throw new InvalidTypesException("Type of TypeVariable '" + ((TypeVariable<?>) t).getName() + "' in '"
						+ ((TypeVariable<?>) t).getGenericDeclaration() + "' could not be determined. This is most likely a type erasure problem. "
						+ "The type extraction currently supports types with generic variables only in cases where "
						+ "all variables in the return type can be deduced from the input type(s). "
						+ "Otherwise the type has to be specified explicitly using type information.");
				}
			}
		}
		// objects with generics are treated as Class first
		else if (t instanceof ParameterizedType) {
			return (TypeInformation<OUT>) privateGetForClass(typeToClass(t), typeHierarchy, (ParameterizedType) t, in1Type, in2Type);
		}
		// no tuple, no TypeVariable, no generic type
		else if (t instanceof Class) {
			return privateGetForClass((Class<OUT>) t, typeHierarchy);
		}

		throw new InvalidTypesException("Type Information could not be created.");
	}

	private <IN1, IN2> TypeInformation<?> createTypeInfoFromInputs(TypeVariable<?> returnTypeVar, ArrayList<Type> returnTypeHierarchy,
			TypeInformation<IN1> in1TypeInfo, TypeInformation<IN2> in2TypeInfo) {

		Type matReturnTypeVar = materializeTypeVariable(returnTypeHierarchy, returnTypeVar);

		// variable could be resolved
		if (!(matReturnTypeVar instanceof TypeVariable)) {
			return createTypeInfoWithTypeHierarchy(returnTypeHierarchy, matReturnTypeVar, in1TypeInfo, in2TypeInfo);
		}
		else {
			returnTypeVar = (TypeVariable<?>) matReturnTypeVar;
		}

		// no input information exists
		if (in1TypeInfo == null && in2TypeInfo == null) {
			return null;
		}

		// create a new type hierarchy for the input
		ArrayList<Type> inputTypeHierarchy = new ArrayList<Type>();
		// copy the function part of the type hierarchy
		for (Type t : returnTypeHierarchy) {
			if (isClassType(t) && Function.class.isAssignableFrom(typeToClass(t)) && typeToClass(t) != Function.class) {
				inputTypeHierarchy.add(t);
			}
			else {
				break;
			}
		}

		if (inputTypeHierarchy.size() == 0) {
			return null;
		}

		ParameterizedType baseClass = (ParameterizedType) inputTypeHierarchy.get(inputTypeHierarchy.size() - 1);

		TypeInformation<?> info = null;
		if (in1TypeInfo != null) {
			// find the deepest type variable that describes the type of input 1
			Type in1Type = baseClass.getActualTypeArguments()[0];

			info = createTypeInfoFromInput(returnTypeVar, new ArrayList<Type>(inputTypeHierarchy), in1Type, in1TypeInfo);
		}

		if (info == null && in2TypeInfo != null) {
			// find the deepest type variable that describes the type of input 2
			Type in2Type = baseClass.getActualTypeArguments()[1];

			info = createTypeInfoFromInput(returnTypeVar, new ArrayList<Type>(inputTypeHierarchy), in2Type, in2TypeInfo);
		}

		if (info != null) {
			return info;
		}

		return null;
	}

	/**
	 * Finds the type information to a type variable.
	 *
	 * It solve the following:
	 *
	 * Return the type information for "returnTypeVar" given that "inType" has type information "inTypeInfo".
	 * Thus "inType" must contain "returnTypeVar" in a "inputTypeHierarchy", otherwise null is returned.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	private <IN1> TypeInformation<?> createTypeInfoFromInput(TypeVariable<?> returnTypeVar, ArrayList<Type> inputTypeHierarchy, Type inType, TypeInformation<IN1> inTypeInfo) {
		TypeInformation<?> info = null;

		// use a factory to find corresponding type information to type variable
		final ArrayList<Type> factoryHierarchy = new ArrayList<>(inputTypeHierarchy);
		final TypeInfoFactory<?> factory = getClosestFactory(factoryHierarchy, inType);
		if (factory != null) {
			// the type that defines the factory is last in factory hierarchy
			final Type factoryDefiningType = factoryHierarchy.get(factoryHierarchy.size() - 1);
			// defining type has generics, the factory need to be asked for a mapping of subtypes to type information
			if (factoryDefiningType instanceof ParameterizedType) {
				final Type[] typeParams = typeToClass(factoryDefiningType).getTypeParameters();
				final Type[] actualParams = ((ParameterizedType) factoryDefiningType).getActualTypeArguments();
				// go thru all elements and search for type variables
				for (int i = 0; i < actualParams.length; i++) {
					final Map<String, TypeInformation<?>> componentInfo = inTypeInfo.getGenericParameters();
					final String typeParamName = typeParams[i].toString();
					if (!componentInfo.containsKey(typeParamName) || componentInfo.get(typeParamName) == null) {
						throw new InvalidTypesException("TypeInformation '" + inTypeInfo.getClass().getSimpleName() +
							"' does not supply a mapping of TypeVariable '" + typeParamName + "' to corresponding TypeInformation. " +
							"Input type inference can only produce a result with this information. " +
							"Please implement method 'TypeInformation.getGenericParameters()' for this.");
					}
					info = createTypeInfoFromInput(returnTypeVar, factoryHierarchy, actualParams[i], componentInfo.get(typeParamName));
					if (info != null) {
						break;
					}
				}
			}
		}
		// the input is a type variable
		else if (sameTypeVars(inType, returnTypeVar)) {
			return inTypeInfo;
		}
		else if (inType instanceof TypeVariable) {
			Type resolvedInType = materializeTypeVariable(inputTypeHierarchy, (TypeVariable<?>) inType);
			if (resolvedInType != inType) {
				info = createTypeInfoFromInput(returnTypeVar, inputTypeHierarchy, resolvedInType, inTypeInfo);
			}
		}
		// the input is a tuple
		else if (inTypeInfo instanceof TupleTypeInfo && isClassType(inType) && Tuple.class.isAssignableFrom(typeToClass(inType))) {
			ParameterizedType tupleBaseClass;

			// get tuple from possible tuple subclass
			while (!(isClassType(inType) && typeToClass(inType).getSuperclass().equals(Tuple.class))) {
				inputTypeHierarchy.add(inType);
				inType = typeToClass(inType).getGenericSuperclass();
			}
			inputTypeHierarchy.add(inType);

			// we can assume to be parameterized since we
			// already did input validation
			tupleBaseClass = (ParameterizedType) inType;

			Type[] tupleElements = tupleBaseClass.getActualTypeArguments();
			// go thru all tuple elements and search for type variables
			for (int i = 0; i < tupleElements.length; i++) {
				info = createTypeInfoFromInput(returnTypeVar, inputTypeHierarchy, tupleElements[i], ((TupleTypeInfo<?>) inTypeInfo).getTypeAt(i));
				if(info != null) {
					break;
				}
			}
		}
		// the input is a pojo
		else if (inTypeInfo instanceof PojoTypeInfo && isClassType(inType)) {
			// build the entire type hierarchy for the pojo
			getTypeHierarchy(inputTypeHierarchy, inType, Object.class);
			// determine a field containing the type variable
			List<Field> fields = getAllDeclaredFields(typeToClass(inType), false);
			for (Field field : fields) {
				Type fieldType = field.getGenericType();
				if (fieldType instanceof TypeVariable && sameTypeVars(returnTypeVar, materializeTypeVariable(inputTypeHierarchy, (TypeVariable<?>) fieldType))) {
					return getTypeOfPojoField(inTypeInfo, field);
				}
				else if (fieldType instanceof ParameterizedType || fieldType instanceof GenericArrayType) {
					ArrayList<Type> typeHierarchyWithFieldType = new ArrayList<>(inputTypeHierarchy);
					typeHierarchyWithFieldType.add(fieldType);
					TypeInformation<?> foundInfo = createTypeInfoFromInput(returnTypeVar, typeHierarchyWithFieldType, fieldType, getTypeOfPojoField(inTypeInfo, field));
					if (foundInfo != null) {
						return foundInfo;
					}
				}
			}
		}
		return info;
	}

	/**
	 * Creates the TypeInformation for all elements of a type that expects a certain number of
	 * subtypes (e.g. TupleXX).
	 *
	 * @param originalType most concrete subclass
	 * @param definingType type that defines the number of subtypes (e.g. Tuple2 -> 2 subtypes)
	 * @param typeHierarchy necessary for type inference
	 * @param in1Type necessary for type inference
	 * @param in2Type necessary for type inference
	 * @param lenient decides whether exceptions should be thrown if a subtype can not be determined
	 * @return array containing TypeInformation of sub types or null if definingType contains
	 *     more subtypes (fields) that defined
	 */
	private <IN1, IN2> TypeInformation<?>[] createSubTypesInfo(Type originalType, ParameterizedType definingType,
			ArrayList<Type> typeHierarchy, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type, boolean lenient) {
		Type[] subtypes = new Type[definingType.getActualTypeArguments().length];

		// materialize possible type variables
		for (int i = 0; i < subtypes.length; i++) {
			final Type actualTypeArg = definingType.getActualTypeArguments()[i];
			// materialize immediate TypeVariables
			if (actualTypeArg instanceof TypeVariable<?>) {
				subtypes[i] = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) actualTypeArg);
			}
			// class or parameterized type
			else {
				subtypes[i] = actualTypeArg;
			}
		}

		TypeInformation<?>[] subTypesInfo = new TypeInformation<?>[subtypes.length];
		for (int i = 0; i < subtypes.length; i++) {
			final ArrayList<Type> subTypeHierarchy = new ArrayList<>(typeHierarchy);
			subTypeHierarchy.add(subtypes[i]);
			// sub type could not be determined with materializing
			// try to derive the type info of the TypeVariable from the immediate base child input as a last attempt
			if (subtypes[i] instanceof TypeVariable<?>) {
				subTypesInfo[i] = createTypeInfoFromInputs((TypeVariable<?>) subtypes[i], subTypeHierarchy, in1Type, in2Type);

				// variable could not be determined
				if (subTypesInfo[i] == null && !lenient) {
					throw new InvalidTypesException("Type of TypeVariable '" + ((TypeVariable<?>) subtypes[i]).getName() + "' in '"
						+ ((TypeVariable<?>) subtypes[i]).getGenericDeclaration()
						+ "' could not be determined. This is most likely a type erasure problem. "
						+ "The type extraction currently supports types with generic variables only in cases where "
						+ "all variables in the return type can be deduced from the input type(s). "
						+ "Otherwise the type has to be specified explicitly using type information.");
				}
			} else {
				// create the type information of the subtype or null/exception
				try {
					subTypesInfo[i] = createTypeInfoWithTypeHierarchy(subTypeHierarchy, subtypes[i], in1Type, in2Type);
				} catch (InvalidTypesException e) {
					if (lenient) {
						subTypesInfo[i] = null;
					} else {
						throw e;
					}
				}
			}
		}

		// check that number of fields matches the number of subtypes
		if (!lenient) {
			Class<?> originalTypeAsClass = null;
			if (isClassType(originalType)) {
				originalTypeAsClass = typeToClass(originalType);
			}
			checkNotNull(originalTypeAsClass, "originalType has an unexpected type");
			// check if the class we assumed to conform to the defining type so far is actually a pojo because the
			// original type contains additional fields.
			// check for additional fields.
			int fieldCount = countFieldsInClass(originalTypeAsClass);
			if(fieldCount > subTypesInfo.length) {
				return null;
			}
		}

		return subTypesInfo;
	}

	/**
	 * Creates type information using a factory if for this type or super types. Returns null otherwise.
	 */
	@SuppressWarnings("unchecked")
	private <IN1, IN2, OUT> TypeInformation<OUT> createTypeInfoFromFactory(
			Type t, ArrayList<Type> typeHierarchy, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {

		final ArrayList<Type> factoryHierarchy = new ArrayList<>(typeHierarchy);
		final TypeInfoFactory<? super OUT> factory = getClosestFactory(factoryHierarchy, t);
		if (factory == null) {
			return null;
		}
		final Type factoryDefiningType = factoryHierarchy.get(factoryHierarchy.size() - 1);

		// infer possible type parameters from input
		final Map<String, TypeInformation<?>> genericParams;
		if (factoryDefiningType instanceof ParameterizedType) {
			genericParams = new HashMap<>();
			final ParameterizedType paramDefiningType = (ParameterizedType) factoryDefiningType;
			final Type[] args = typeToClass(paramDefiningType).getTypeParameters();

			final TypeInformation<?>[] subtypeInfo = createSubTypesInfo(t, paramDefiningType, factoryHierarchy, in1Type, in2Type, true);
			assert subtypeInfo != null;
			for (int i = 0; i < subtypeInfo.length; i++) {
				genericParams.put(args[i].toString(), subtypeInfo[i]);
			}
		} else {
			genericParams = Collections.emptyMap();
		}

		final TypeInformation<OUT> createdTypeInfo = (TypeInformation<OUT>) factory.createTypeInfo(t, genericParams);
		if (createdTypeInfo == null) {
			throw new InvalidTypesException("TypeInfoFactory returned invalid TypeInformation 'null'");
		}
		return createdTypeInfo;
	}

	// --------------------------------------------------------------------------------------------
	//  Extract type parameters
	// --------------------------------------------------------------------------------------------

	@PublicEvolving
	public static Type getParameterType(Class<?> baseClass, Class<?> clazz, int pos) {
		return getParameterType(baseClass, null, clazz, pos);
	}

	private static Type getParameterType(Class<?> baseClass, ArrayList<Type> typeHierarchy, Class<?> clazz, int pos) {
		if (typeHierarchy != null) {
			typeHierarchy.add(clazz);
		}
		Type[] interfaceTypes = clazz.getGenericInterfaces();

		// search in interfaces for base class
		for (Type t : interfaceTypes) {
			Type parameter = getParameterTypeFromGenericType(baseClass, typeHierarchy, t, pos);
			if (parameter != null) {
				return parameter;
			}
		}

		// search in superclass for base class
		Type t = clazz.getGenericSuperclass();
		Type parameter = getParameterTypeFromGenericType(baseClass, typeHierarchy, t, pos);
		if (parameter != null) {
			return parameter;
		}

		throw new InvalidTypesException("The types of the interface " + baseClass.getName() + " could not be inferred. " +
						"Support for synthetic interfaces, lambdas, and generic or raw types is limited at this point");
	}

	private static Type getParameterTypeFromGenericType(Class<?> baseClass, ArrayList<Type> typeHierarchy, Type t, int pos) {
		// base class
		if (t instanceof ParameterizedType && baseClass.equals(((ParameterizedType) t).getRawType())) {
			if (typeHierarchy != null) {
				typeHierarchy.add(t);
			}
			ParameterizedType baseClassChild = (ParameterizedType) t;
			return baseClassChild.getActualTypeArguments()[pos];
		}
		// interface that extended base class as class or parameterized type
		else if (t instanceof ParameterizedType && baseClass.isAssignableFrom((Class<?>) ((ParameterizedType) t).getRawType())) {
			if (typeHierarchy != null) {
				typeHierarchy.add(t);
			}
			return getParameterType(baseClass, typeHierarchy, (Class<?>) ((ParameterizedType) t).getRawType(), pos);
		}
		else if (t instanceof Class<?> && baseClass.isAssignableFrom((Class<?>) t)) {
			if (typeHierarchy != null) {
				typeHierarchy.add(t);
			}
			return getParameterType(baseClass, typeHierarchy, (Class<?>) t, pos);
		}
		return null;
	}

	// --------------------------------------------------------------------------------------------
	//  Validate input
	// --------------------------------------------------------------------------------------------

	private static void validateInputType(Type t, TypeInformation<?> inType) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();
		try {
			validateInfo(typeHierarchy, t, inType);
		}
		catch(InvalidTypesException e) {
			throw new InvalidTypesException("Input mismatch: " + e.getMessage(), e);
		}
	}

	private static void validateInputType(Class<?> baseClass, Class<?> clazz, int inputParamPos, TypeInformation<?> inTypeInfo) {
		ArrayList<Type> typeHierarchy = new ArrayList<Type>();

		// try to get generic parameter
		Type inType;
		try {
			inType = getParameterType(baseClass, typeHierarchy, clazz, inputParamPos);
		}
		catch (InvalidTypesException e) {
			return; // skip input validation e.g. for raw types
		}

		try {
			validateInfo(typeHierarchy, inType, inTypeInfo);
		}
		catch(InvalidTypesException e) {
			throw new InvalidTypesException("Input mismatch: " + e.getMessage(), e);
		}
	}

	@SuppressWarnings("unchecked")
	private static void validateInfo(ArrayList<Type> typeHierarchy, Type type, TypeInformation<?> typeInfo) {
		if (type == null) {
			throw new InvalidTypesException("Unknown Error. Type is null.");
		}

		if (typeInfo == null) {
			throw new InvalidTypesException("Unknown Error. TypeInformation is null.");
		}

		if (!(type instanceof TypeVariable<?>)) {
			// check for Java Basic Types
			if (typeInfo instanceof BasicTypeInfo) {

				TypeInformation<?> actual;
				// check if basic type at all
				if (!(type instanceof Class<?>) || (actual = BasicTypeInfo.getInfoFor((Class<?>) type)) == null) {
					throw new InvalidTypesException("Basic type expected.");
				}
				// check if correct basic type
				if (!typeInfo.equals(actual)) {
					throw new InvalidTypesException("Basic type '" + typeInfo + "' expected but was '" + actual + "'.");
				}

			}
			// check for Java Tuples
			else if (typeInfo instanceof TupleTypeInfo) {
				// check if tuple at all
				if (!(isClassType(type) && Tuple.class.isAssignableFrom(typeToClass(type)))) {
					throw new InvalidTypesException("Tuple type expected.");
				}

				// do not allow usage of Tuple as type
				if (isClassType(type) && typeToClass(type).equals(Tuple.class)) {
					throw new InvalidTypesException("Concrete subclass of Tuple expected.");
				}

				// go up the hierarchy until we reach immediate child of Tuple (with or without generics)
				while (!(isClassType(type) && typeToClass(type).getSuperclass().equals(Tuple.class))) {
					typeHierarchy.add(type);
					type = typeToClass(type).getGenericSuperclass();
				}

				if(type == Tuple0.class) {
					return;
				}

				// check if immediate child of Tuple has generics
				if (type instanceof Class<?>) {
					throw new InvalidTypesException("Parameterized Tuple type expected.");
				}

				TupleTypeInfo<?> tti = (TupleTypeInfo<?>) typeInfo;

				Type[] subTypes = ((ParameterizedType) type).getActualTypeArguments();

				if (subTypes.length != tti.getArity()) {
					throw new InvalidTypesException("Tuple arity '" + tti.getArity() + "' expected but was '"
							+ subTypes.length + "'.");
				}

				for (int i = 0; i < subTypes.length; i++) {
					validateInfo(new ArrayList<Type>(typeHierarchy), subTypes[i], tti.getTypeAt(i));
				}
			}
			// check for value
			else if (typeInfo instanceof ValueTypeInfo<?>) {
				// check if value at all
				if (!(type instanceof Class<?> && Value.class.isAssignableFrom((Class<?>) type))) {
					throw new InvalidTypesException("Value type expected.");
				}

				TypeInformation<?> actual;
				// check value type contents
				if (!((ValueTypeInfo<?>) typeInfo).equals(actual = ValueTypeInfo.getValueTypeInfo((Class<? extends Value>) type))) {
					throw new InvalidTypesException("Value type '" + typeInfo + "' expected but was '" + actual + "'.");
				}
			}
			// check for POJO
			else if (typeInfo instanceof PojoTypeInfo) {
				Class<?> clazz = null;
				if (!(isClassType(type) && ((PojoTypeInfo<?>) typeInfo).getTypeClass() == (clazz = typeToClass(type)))) {
					throw new InvalidTypesException("POJO type '"
							+ ((PojoTypeInfo<?>) typeInfo).getTypeClass().getCanonicalName() + "' expected but was '"
							+ clazz.getCanonicalName() + "'.");
				}
			}
			// check for Writable
			else {
				validateIfWritable(typeInfo, type);
			}
		} else {
			type = materializeTypeVariable(typeHierarchy, (TypeVariable<?>) type);
			if (!(type instanceof TypeVariable)) {
				validateInfo(typeHierarchy, type, typeInfo);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Utility methods
	// --------------------------------------------------------------------------------------------

	/**
	 * Returns the type information factory for a type using the factory registry or annotations.
	 */
	@Internal
	public static <OUT> TypeInfoFactory<OUT> getTypeInfoFactory(Type t) {
		final Class<?> factoryClass;
		if (registeredTypeInfoFactories.containsKey(t)) {
			factoryClass = registeredTypeInfoFactories.get(t);
		}
		else {
			if (!isClassType(t) || !typeToClass(t).isAnnotationPresent(TypeInfo.class)) {
				return null;
			}
			final TypeInfo typeInfoAnnotation = typeToClass(t).getAnnotation(TypeInfo.class);
			factoryClass = typeInfoAnnotation.value();
			// check for valid factory class
			if (!TypeInfoFactory.class.isAssignableFrom(factoryClass)) {
				throw new InvalidTypesException("TypeInfo annotation does not specify a valid TypeInfoFactory.");
			}
		}

		// instantiate
		return (TypeInfoFactory<OUT>) InstantiationUtil.instantiate(factoryClass);
	}

	/**
	 * Traverses the type hierarchy up until a type information factory can be found.
	 *
	 * @param typeHierarchy hierarchy to be filled while traversing up
	 * @param t type for which a factory needs to be found
	 * @return closest type information factory or null if there is no factory in the type hierarchy
	 */
	private static <OUT> TypeInfoFactory<? super OUT> getClosestFactory(ArrayList<Type> typeHierarchy, Type t) {
		TypeInfoFactory factory = null;
		while (factory == null && isClassType(t) && !(typeToClass(t).equals(Object.class))) {
			typeHierarchy.add(t);
			factory = getTypeInfoFactory(t);
			t = typeToClass(t).getGenericSuperclass();

			if (t == null) {
				break;
			}
		}
		return factory;
	}

	private int countFieldsInClass(Class<?> clazz) {
		int fieldCount = 0;
		for(Field field : clazz.getFields()) { // get all fields
			if(	!Modifier.isStatic(field.getModifiers()) &&
				!Modifier.isTransient(field.getModifiers())
				) {
				fieldCount++;
			}
		}
		return fieldCount;
	}

	/**
	 * Tries to find a concrete value (Class, ParameterizedType etc. ) for a TypeVariable by traversing the type hierarchy downwards.
	 * If a value could not be found it will return the most bottom type variable in the hierarchy.
	 */
	private static Type materializeTypeVariable(ArrayList<Type> typeHierarchy, TypeVariable<?> typeVar) {
		TypeVariable<?> inTypeTypeVar = typeVar;
		// iterate thru hierarchy from top to bottom until type variable gets a class assigned
		for (int i = typeHierarchy.size() - 1; i >= 0; i--) {
			Type curT = typeHierarchy.get(i);

			// parameterized type
			if (curT instanceof ParameterizedType) {
				Class<?> rawType = ((Class<?>) ((ParameterizedType) curT).getRawType());

				for (int paramIndex = 0; paramIndex < rawType.getTypeParameters().length; paramIndex++) {

					TypeVariable<?> curVarOfCurT = rawType.getTypeParameters()[paramIndex];

					// check if variable names match
					if (sameTypeVars(curVarOfCurT, inTypeTypeVar)) {
						Type curVarType = ((ParameterizedType) curT).getActualTypeArguments()[paramIndex];

						// another type variable level
						if (curVarType instanceof TypeVariable<?>) {
							inTypeTypeVar = (TypeVariable<?>) curVarType;
						}
						// class
						else {
							return curVarType;
						}
					}
				}
			}
		}
		// can not be materialized, most likely due to type erasure
		// return the type variable of the deepest level
		return inTypeTypeVar;
	}

	/**
	 * Creates type information from a given Class such as Integer, String[] or POJOs.
	 *
	 * This method does not support ParameterizedTypes such as Tuples or complex type hierarchies.
	 * In most cases {@link TypeExtractor#createTypeInfo(Type)} is the recommended method for type extraction
	 * (a Class is a child of Type).
	 *
	 * @param clazz a Class to create TypeInformation for
	 * @return TypeInformation that describes the passed Class
	 */
	public static <X> TypeInformation<X> getForClass(Class<X> clazz) {
		final ArrayList<Type> typeHierarchy = new ArrayList<>();
		typeHierarchy.add(clazz);
		return new TypeExtractor().privateGetForClass(clazz, typeHierarchy);
	}

	private <X> TypeInformation<X> privateGetForClass(Class<X> clazz, ArrayList<Type> typeHierarchy) {
		return privateGetForClass(clazz, typeHierarchy, null, null, null);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <OUT,IN1,IN2> TypeInformation<OUT> privateGetForClass(Class<OUT> clazz, ArrayList<Type> typeHierarchy,
			ParameterizedType parameterizedType, TypeInformation<IN1> in1Type, TypeInformation<IN2> in2Type) {
		checkNotNull(clazz);

		// check for basic types
		TypeInformation<OUT> basicTypeInfo = BasicTypeInfo.getInfoFor(clazz);
		return basicTypeInfo;
	}

	@PublicEvolving
	public static List<Field> getAllDeclaredFields(Class<?> clazz, boolean ignoreDuplicates) {
		List<Field> result = new ArrayList<Field>();
		while (clazz != null) {
			Field[] fields = clazz.getDeclaredFields();
			for (Field field : fields) {
				if(Modifier.isTransient(field.getModifiers()) || Modifier.isStatic(field.getModifiers())) {
					continue; // we have no use for transient or static fields
				}
				if(hasFieldWithSameName(field.getName(), result)) {
					if (ignoreDuplicates) {
						continue;
					} else {
						throw new InvalidTypesException("The field "+field+" is already contained in the hierarchy of the "+clazz+"."
							+ "Please use unique field names through your classes hierarchy");
					}
				}
				result.add(field);
			}
			clazz = clazz.getSuperclass();
		}
		return result;
	}

	@PublicEvolving
	public static Field getDeclaredField(Class<?> clazz, String name) {
		for (Field field : getAllDeclaredFields(clazz, true)) {
			if (field.getName().equals(name)) {
				return field;
			}
		}
		return null;
	}

	private static boolean hasFieldWithSameName(String name, List<Field> fields) {
		for(Field field : fields) {
			if(name.equals(field.getName())) {
				return true;
			}
		}
		return false;
	}

	private static TypeInformation<?> getTypeOfPojoField(TypeInformation<?> pojoInfo, Field field) {
		for (int j = 0; j < pojoInfo.getArity(); j++) {
			PojoField pf = ((PojoTypeInfo<?>) pojoInfo).getPojoFieldAt(j);
			if (pf.getField().getName().equals(field.getName())) {
				return pf.getTypeInformation();
			}
		}
		return null;
	}

	public static <X> TypeInformation<X> getForObject(X value) {
		return new TypeExtractor().privateGetForObject(value);
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private <X> TypeInformation<X> privateGetForObject(X value) {
		checkNotNull(value);

		// check if type information can be produced using a factory
		final ArrayList<Type> typeHierarchy = new ArrayList<>();
		typeHierarchy.add(value.getClass());
		final TypeInformation<X> typeFromFactory = createTypeInfoFromFactory(value.getClass(), typeHierarchy, null, null);
		if (typeFromFactory != null) {
			return typeFromFactory;
		}

		// check if we can extract the types from tuples, otherwise work with the class
		if (value instanceof Tuple) {
			Tuple t = (Tuple) value;
			int numFields = t.getArity();

			TypeInformation<?>[] infos = new TypeInformation[numFields];
			for (int i = 0; i < numFields; i++) {
				Object field = t.getField(i);

				if (field == null) {
					throw new InvalidTypesException("Automatic type extraction is not possible on candidates with null values. "
							+ "Please specify the types directly.");
				}

				infos[i] = privateGetForObject(field);
			}
			return new TupleTypeInfo(value.getClass(), infos);
		}
		else {
			return privateGetForClass((Class<X>) value.getClass(), new ArrayList<Type>());
		}
	}

	// ------------------------------------------------------------------------
	//  Utilities to handle Hadoop's 'Writable' type via reflection
	// ------------------------------------------------------------------------

	// visible for testing
	static boolean isHadoopWritable(Class<?> typeClass) {
		// check if this is directly the writable interface
		if (typeClass.getName().equals(HADOOP_WRITABLE_CLASS)) {
			return false;
		}

		final HashSet<Class<?>> alreadySeen = new HashSet<>();
		alreadySeen.add(typeClass);
		return hasHadoopWritableInterface(typeClass, alreadySeen);
	}

	private static boolean hasHadoopWritableInterface(Class<?> clazz,  HashSet<Class<?>> alreadySeen) {
		Class<?>[] interfaces = clazz.getInterfaces();
		for (Class<?> c : interfaces) {
			if (c.getName().equals(HADOOP_WRITABLE_CLASS)) {
				return true;
			}
			else if (alreadySeen.add(c) && hasHadoopWritableInterface(c, alreadySeen)) {
				return true;
			}
		}

		Class<?> superclass = clazz.getSuperclass();
		return superclass != null && alreadySeen.add(superclass) && hasHadoopWritableInterface(superclass, alreadySeen);
	}

	// visible for testing
	public static <T> TypeInformation<T> createHadoopWritableTypeInfo(Class<T> clazz) {
		checkNotNull(clazz);

		Class<?> typeInfoClass;
		try {
			typeInfoClass = Class.forName(HADOOP_WRITABLE_TYPEINFO_CLASS, false, Thread.currentThread().getContextClassLoader());
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException("Could not load the TypeInformation for the class '"
					+ HADOOP_WRITABLE_CLASS + "'. You may be missing the 'flink-hadoop-compatibility' dependency.");
		}

		try {
			Constructor<?> constr = typeInfoClass.getConstructor(Class.class);

			@SuppressWarnings("unchecked")
			TypeInformation<T> typeInfo = (TypeInformation<T>) constr.newInstance(clazz);
			return typeInfo;
		}
		catch (NoSuchMethodException | IllegalAccessException | InstantiationException e) {
			throw new RuntimeException("Incompatible versions of the Hadoop Compatibility classes found.");
		}
		catch (InvocationTargetException e) {
			throw new RuntimeException("Cannot create Hadoop WritableTypeInfo.", e.getTargetException());
		}
	}

	// visible for testing
	static void validateIfWritable(TypeInformation<?> typeInfo, Type type) {
		try {
			// try to load the writable type info

			Class<?> writableTypeInfoClass = Class
					.forName(HADOOP_WRITABLE_TYPEINFO_CLASS, false, typeInfo.getClass().getClassLoader());

			if (writableTypeInfoClass.isAssignableFrom(typeInfo.getClass())) {
				// this is actually a writable type info
				// check if the type is a writable
				if (!(type instanceof Class && isHadoopWritable((Class<?>) type))) {
					throw new InvalidTypesException(HADOOP_WRITABLE_CLASS + " type expected.");
				}

				// check writable type contents
				Class<?> clazz = (Class<?>) type;
				if (typeInfo.getTypeClass() != clazz) {
					throw new InvalidTypesException("Writable type '"
							+ typeInfo.getTypeClass().getCanonicalName() + "' expected but was '"
							+ clazz.getCanonicalName() + "'.");
				}
			}
		}
		catch (ClassNotFoundException e) {
			// class not present at all, so cannot be that type info
			// ignore
		}
	}
}
