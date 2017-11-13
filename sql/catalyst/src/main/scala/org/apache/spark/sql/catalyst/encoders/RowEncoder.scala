/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.encoders

import scala.collection.Map
import scala.reflect.ClassTag

import org.apache.spark.SparkException
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.analysis.GetColumnByOrdinal
import org.apache.spark.sql.catalyst.expressions._
<<<<<<< HEAD
import org.apache.spark.sql.catalyst.expressions.objects._
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, DateTimeUtils, GenericArrayData}
=======
import org.apache.spark.sql.catalyst.util.{GenericArrayData, ArrayBasedMapData, DateTimeUtils}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A factory for constructing encoders that convert external row to/from the Spark SQL
 * internal binary representation.
 *
 * The following is a mapping between Spark SQL types and its allowed external types:
 * {{{
 *   BooleanType -> java.lang.Boolean
 *   ByteType -> java.lang.Byte
 *   ShortType -> java.lang.Short
 *   IntegerType -> java.lang.Integer
 *   FloatType -> java.lang.Float
 *   DoubleType -> java.lang.Double
 *   StringType -> String
 *   DecimalType -> java.math.BigDecimal or scala.math.BigDecimal or Decimal
 *
 *   DateType -> java.sql.Date
 *   TimestampType -> java.sql.Timestamp
 *
 *   BinaryType -> byte array
 *   ArrayType -> scala.collection.Seq or Array
 *   MapType -> scala.collection.Map
 *   StructType -> org.apache.spark.sql.Row
 * }}}
 */
object RowEncoder {
  def apply(schema: StructType): ExpressionEncoder[Row] = {
    val cls = classOf[Row]
    val inputObject = BoundReference(0, ObjectType(cls), nullable = true)
    // We use an If expression to wrap extractorsFor result of StructType
    val extractExpressions = extractorsFor(inputObject, schema).asInstanceOf[If].falseValue
    val constructExpression = constructorFor(schema)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    new ExpressionEncoder[Row](
      schema,
      flat = false,
      serializer.asInstanceOf[CreateNamedStruct].flatten,
      deserializer,
      ClassTag(cls))
  }

  private def serializerFor(
      inputObject: Expression,
      inputType: DataType): Expression = inputType match {
    case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BinaryType => inputObject
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    case udt: UserDefinedType[_] =>
      val obj = NewInstance(
        udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt(),
        Nil,
        dataType = ObjectType(udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()))
      Invoke(obj, "serialize", udt.sqlType, inputObject :: Nil)

    case udt: UserDefinedType[_] =>
      val obj = NewInstance(
        udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt(),
        Nil,
        dataType = ObjectType(udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()))
      Invoke(obj, "serialize", udt.sqlType, inputObject :: Nil)

    case TimestampType =>
      StaticInvoke(
        DateTimeUtils.getClass,
        TimestampType,
        "fromJavaTimestamp",
        inputObject :: Nil,
        returnNullable = false)

    case DateType =>
      StaticInvoke(
        DateTimeUtils.getClass,
        DateType,
        "fromJavaDate",
        inputObject :: Nil,
        returnNullable = false)

    case d: DecimalType =>
      StaticInvoke(
        Decimal.getClass,
        DecimalType.SYSTEM_DEFAULT,
        "apply",
        inputObject :: Nil)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    case StringType =>
      StaticInvoke(
        classOf[UTF8String],
        StringType,
        "fromString",
        inputObject :: Nil,
        returnNullable = false)

    case t @ ArrayType(et, containsNull) =>
      et match {
        case BooleanType | ByteType | ShortType | IntegerType | LongType | FloatType | DoubleType =>
          StaticInvoke(
            classOf[ArrayData],
            t,
            "toArrayData",
            inputObject :: Nil,
            returnNullable = false)

        case _ => MapObjects(
          element => {
            val value = serializerFor(ValidateExternalType(element, et), et)
            if (!containsNull) {
              AssertNotNull(value, Seq.empty)
            } else {
              value
            }
          },
          inputObject,
          ObjectType(classOf[Object]))
      }

    case t @ MapType(kt, vt, valueNullable) =>
      val keys =
        Invoke(
          Invoke(inputObject, "keysIterator", ObjectType(classOf[scala.collection.Iterator[_]]),
            returnNullable = false),
          "toSeq",
          ObjectType(classOf[scala.collection.Seq[_]]), returnNullable = false)
      val convertedKeys = serializerFor(keys, ArrayType(kt, false))

      val values =
        Invoke(
          Invoke(inputObject, "valuesIterator", ObjectType(classOf[scala.collection.Iterator[_]]),
            returnNullable = false),
          "toSeq",
          ObjectType(classOf[scala.collection.Seq[_]]), returnNullable = false)
      val convertedValues = serializerFor(values, ArrayType(vt, valueNullable))

      val nonNullOutput = NewInstance(
        classOf[ArrayBasedMapData],
        convertedKeys :: convertedValues :: Nil,
        dataType = t,
        propagateNull = false)

      if (inputObject.nullable) {
        If(IsNull(inputObject),
          Literal.create(null, inputType),
          nonNullOutput)
      } else {
        nonNullOutput
      }

    case StructType(fields) =>
<<<<<<< HEAD
      val nonNullOutput = CreateNamedStruct(fields.zipWithIndex.flatMap { case (field, index) =>
        val fieldValue = serializerFor(
          ValidateExternalType(
            GetExternalRowField(inputObject, index, field.name),
            field.dataType),
          field.dataType)
        val convertedField = if (field.nullable) {
          If(
            Invoke(inputObject, "isNullAt", BooleanType, Literal(index) :: Nil),
            Literal.create(null, field.dataType),
            fieldValue
          )
        } else {
          fieldValue
        }
        Literal(field.name) :: convertedField :: Nil
      })

      if (inputObject.nullable) {
        If(IsNull(inputObject),
          Literal.create(null, inputType),
          nonNullOutput)
      } else {
        nonNullOutput
      }
  }

  /**
   * Returns the `DataType` that can be used when generating code that converts input data
   * into the Spark SQL internal format.  Unlike `externalDataTypeFor`, the `DataType` returned
   * by this function can be more permissive since multiple external types may map to a single
   * internal type.  For example, for an input with DecimalType in external row, its external types
   * can be `scala.math.BigDecimal`, `java.math.BigDecimal`, or
   * `org.apache.spark.sql.types.Decimal`.
   */
  def externalDataTypeForInput(dt: DataType): DataType = dt match {
    // In order to support both Decimal and java/scala BigDecimal in external row, we make this
    // as java.lang.Object.
    case _: DecimalType => ObjectType(classOf[java.lang.Object])
    // In order to support both Array and Seq in external row, we make this as java.lang.Object.
    case _: ArrayType => ObjectType(classOf[java.lang.Object])
    case _ => externalDataTypeFor(dt)
  }

  def externalDataTypeFor(dt: DataType): DataType = dt match {
=======
      val convertedFields = fields.zipWithIndex.map { case (f, i) =>
        val method = if (f.dataType.isInstanceOf[StructType]) {
          "getStruct"
        } else {
          "get"
        }
        If(
          Invoke(inputObject, "isNullAt", BooleanType, Literal(i) :: Nil),
          Literal.create(null, f.dataType),
          extractorsFor(
            Invoke(inputObject, method, externalDataTypeFor(f.dataType), Literal(i) :: Nil),
            f.dataType))
      }
      If(IsNull(inputObject),
        Literal.create(null, inputType),
        CreateStruct(convertedFields))
  }

  private def externalDataTypeFor(dt: DataType): DataType = dt match {
    case _ if ScalaReflection.isNativeType(dt) => dt
    case TimestampType => ObjectType(classOf[java.sql.Timestamp])
    case DateType => ObjectType(classOf[java.sql.Date])
    case _: DecimalType => ObjectType(classOf[java.math.BigDecimal])
    case StringType => ObjectType(classOf[java.lang.String])
    case _: ArrayType => ObjectType(classOf[scala.collection.Seq[_]])
    case _: MapType => ObjectType(classOf[scala.collection.Map[_, _]])
    case _: StructType => ObjectType(classOf[Row])
    case udt: UserDefinedType[_] => ObjectType(udt.userClass)
    case _: NullType => ObjectType(classOf[java.lang.Object])
  }

  private def deserializerFor(schema: StructType): Expression = {
    val fields = schema.zipWithIndex.map { case (f, i) =>
<<<<<<< HEAD
      val dt = f.dataType match {
        case p: PythonUserDefinedType => p.sqlType
        case other => other
      }
      deserializerFor(GetColumnByOrdinal(i, dt))
=======
      val field = BoundReference(i, f.dataType, f.nullable)
      If(
        IsNull(field),
        Literal.create(null, externalDataTypeFor(f.dataType)),
        constructorFor(BoundReference(i, f.dataType, f.nullable))
      )
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    }
    CreateExternalRow(fields, schema)
  }

  private def constructorFor(input: Expression): Expression = input.dataType match {
    case NullType | BooleanType | ByteType | ShortType | IntegerType | LongType |
         FloatType | DoubleType | BinaryType => input
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    case udt: UserDefinedType[_] =>
      val obj = NewInstance(
        udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt(),
        Nil,
        dataType = ObjectType(udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()))
      Invoke(obj, "deserialize", ObjectType(udt.userClass), input :: Nil)

    case udt: UserDefinedType[_] =>
      val obj = NewInstance(
        udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt(),
        Nil,
        dataType = ObjectType(udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()))
      Invoke(obj, "deserialize", ObjectType(udt.userClass), input :: Nil)

    case TimestampType =>
      StaticInvoke(
        DateTimeUtils.getClass,
        ObjectType(classOf[java.sql.Timestamp]),
        "toJavaTimestamp",
        input :: Nil,
        returnNullable = false)

    case DateType =>
      StaticInvoke(
        DateTimeUtils.getClass,
        ObjectType(classOf[java.sql.Date]),
        "toJavaDate",
        input :: Nil,
        returnNullable = false)

    case _: DecimalType =>
      Invoke(input, "toJavaBigDecimal", ObjectType(classOf[java.math.BigDecimal]),
        returnNullable = false)

    case StringType =>
      Invoke(input, "toString", ObjectType(classOf[String]), returnNullable = false)

    case ArrayType(et, nullable) =>
      val arrayData =
        Invoke(
          MapObjects(constructorFor(_), input, et),
          "array",
          ObjectType(classOf[Array[_]]), returnNullable = false)
      StaticInvoke(
        scala.collection.mutable.WrappedArray.getClass,
        ObjectType(classOf[Seq[_]]),
        "make",
        arrayData :: Nil,
        returnNullable = false)

    case MapType(kt, vt, valueNullable) =>
      val keyArrayType = ArrayType(kt, false)
      val keyData = constructorFor(Invoke(input, "keyArray", keyArrayType))

      val valueArrayType = ArrayType(vt, valueNullable)
      val valueData = constructorFor(Invoke(input, "valueArray", valueArrayType))

      StaticInvoke(
        ArrayBasedMapData.getClass,
        ObjectType(classOf[Map[_, _]]),
        "toScalaMap",
        keyData :: valueData :: Nil,
        returnNullable = false)

    case schema @ StructType(fields) =>
      val convertedFields = fields.zipWithIndex.map { case (f, i) =>
        If(
          Invoke(input, "isNullAt", BooleanType, Literal(i) :: Nil),
          Literal.create(null, externalDataTypeFor(f.dataType)),
          constructorFor(GetStructField(input, i)))
      }
      If(IsNull(input),
        Literal.create(null, externalDataTypeFor(input.dataType)),
        CreateExternalRow(convertedFields))
  }
}
