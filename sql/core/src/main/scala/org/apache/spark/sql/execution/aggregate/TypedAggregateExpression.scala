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

package org.apache.spark.sql.execution.aggregate

import scala.language.existentials

<<<<<<< HEAD
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.UnresolvedDeserializer
import org.apache.spark.sql.catalyst.encoders.encoderFor
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, DeclarativeAggregate, TypedImperativeAggregate}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.catalyst.expressions.objects.Invoke
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.types._

object TypedAggregateExpression {
  def apply[BUF : Encoder, OUT : Encoder](
      aggregator: Aggregator[_, BUF, OUT]): TypedAggregateExpression = {
    val bufferEncoder = encoderFor[BUF]
    val bufferSerializer = bufferEncoder.namedExpressions

    val outputEncoder = encoderFor[OUT]
    val outputType = if (outputEncoder.flat) {
      outputEncoder.schema.head.dataType
    } else {
      outputEncoder.schema
    }

    // Checks if the buffer object is simple, i.e. the buffer encoder is flat and the serializer
    // expression is an alias of `BoundReference`, which means the buffer object doesn't need
    // serialization.
    val isSimpleBuffer = {
      bufferSerializer.head match {
        case Alias(_: BoundReference, _) if bufferEncoder.flat => true
        case _ => false
      }
    }

    // If the buffer object is simple, use `SimpleTypedAggregateExpression`, which supports whole
    // stage codegen.
    if (isSimpleBuffer) {
      val bufferDeserializer = UnresolvedDeserializer(
        bufferEncoder.deserializer,
        bufferSerializer.map(_.toAttribute))

      SimpleTypedAggregateExpression(
        aggregator.asInstanceOf[Aggregator[Any, Any, Any]],
        None,
        None,
        None,
        bufferSerializer,
        bufferDeserializer,
        outputEncoder.serializer,
        outputEncoder.deserializer.dataType,
        outputType,
        !outputEncoder.flat || outputEncoder.schema.head.nullable)
    } else {
      ComplexTypedAggregateExpression(
        aggregator.asInstanceOf[Aggregator[Any, Any, Any]],
        None,
        None,
        None,
        bufferSerializer,
        bufferEncoder.resolveAndBind().deserializer,
        outputEncoder.serializer,
        outputType,
        !outputEncoder.flat || outputEncoder.schema.head.nullable)
    }
=======
import org.apache.spark.Logging
import org.apache.spark.sql.Encoder
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{OuterScopes, encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

object TypedAggregateExpression {
  def apply[A, B : Encoder, C : Encoder](
      aggregator: Aggregator[A, B, C]): TypedAggregateExpression = {
    new TypedAggregateExpression(
      aggregator.asInstanceOf[Aggregator[Any, Any, Any]],
      None,
      encoderFor[B].asInstanceOf[ExpressionEncoder[Any]],
      encoderFor[C].asInstanceOf[ExpressionEncoder[Any]],
      Nil,
      0,
      0)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}

/**
<<<<<<< HEAD
 * A helper class to hook [[Aggregator]] into the aggregation system.
 */
trait TypedAggregateExpression extends AggregateFunction {

  def aggregator: Aggregator[Any, Any, Any]

  def inputDeserializer: Option[Expression]
  def inputClass: Option[Class[_]]
  def inputSchema: Option[StructType]

  def withInputInfo(deser: Expression, cls: Class[_], schema: StructType): TypedAggregateExpression

  override def toString: String = {
    val input = inputDeserializer match {
      case Some(UnresolvedDeserializer(deserializer, _)) => deserializer.dataType.simpleString
      case Some(deserializer) => deserializer.dataType.simpleString
      case _ => "unknown"
    }

    s"$nodeName($input)"
  }

  override def nodeName: String = aggregator.getClass.getSimpleName.stripSuffix("$")
}

// TODO: merge these 2 implementations once we refactor the `AggregateFunction` interface.

case class SimpleTypedAggregateExpression(
    aggregator: Aggregator[Any, Any, Any],
    inputDeserializer: Option[Expression],
    inputClass: Option[Class[_]],
    inputSchema: Option[StructType],
    bufferSerializer: Seq[NamedExpression],
    bufferDeserializer: Expression,
    outputSerializer: Seq[Expression],
    outputExternalType: DataType,
    dataType: DataType,
    nullable: Boolean)
  extends DeclarativeAggregate with TypedAggregateExpression with NonSQLExpression {

  override def deterministic: Boolean = true

  override def children: Seq[Expression] = inputDeserializer.toSeq :+ bufferDeserializer

  override lazy val resolved: Boolean = inputDeserializer.isDefined && childrenResolved

  override def references: AttributeSet = AttributeSet(inputDeserializer.toSeq)

  private def aggregatorLiteral =
    Literal.create(aggregator, ObjectType(classOf[Aggregator[Any, Any, Any]]))

  private def bufferExternalType = bufferDeserializer.dataType

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    bufferSerializer.map(_.toAttribute.asInstanceOf[AttributeReference])

  private def serializeToBuffer(expr: Expression): Seq[Expression] = {
    bufferSerializer.map(_.transform {
      case _: BoundReference => expr
    })
  }

  override lazy val initialValues: Seq[Expression] = {
    val zero = Literal.fromObject(aggregator.zero, bufferExternalType)
    serializeToBuffer(zero)
  }

  override lazy val updateExpressions: Seq[Expression] = {
    val reduced = Invoke(
      aggregatorLiteral,
      "reduce",
      bufferExternalType,
      bufferDeserializer :: inputDeserializer.get :: Nil)
    serializeToBuffer(reduced)
  }

  override lazy val mergeExpressions: Seq[Expression] = {
    val leftBuffer = bufferDeserializer transform {
      case a: AttributeReference => a.left
    }
    val rightBuffer = bufferDeserializer transform {
      case a: AttributeReference => a.right
    }
    val merged = Invoke(
      aggregatorLiteral,
      "merge",
      bufferExternalType,
      leftBuffer :: rightBuffer :: Nil)
    serializeToBuffer(merged)
  }

  override lazy val evaluateExpression: Expression = {
    val resultObj = Invoke(
      aggregatorLiteral,
      "finish",
      outputExternalType,
      bufferDeserializer :: Nil)

    val outputSerializeExprs = outputSerializer.map(_.transform {
      case _: BoundReference => resultObj
    })

    dataType match {
      case _: StructType =>
        val objRef = outputSerializer.head.find(_.isInstanceOf[BoundReference]).get
        If(IsNull(objRef), Literal.create(null, dataType), CreateStruct(outputSerializeExprs))
      case _ =>
        assert(outputSerializeExprs.length == 1)
        outputSerializeExprs.head
    }
  }

  override def withInputInfo(
      deser: Expression,
      cls: Class[_],
      schema: StructType): TypedAggregateExpression = {
    copy(inputDeserializer = Some(deser), inputClass = Some(cls), inputSchema = Some(schema))
  }
}

case class ComplexTypedAggregateExpression(
    aggregator: Aggregator[Any, Any, Any],
    inputDeserializer: Option[Expression],
    inputClass: Option[Class[_]],
    inputSchema: Option[StructType],
    bufferSerializer: Seq[NamedExpression],
    bufferDeserializer: Expression,
    outputSerializer: Seq[Expression],
    dataType: DataType,
    nullable: Boolean,
    mutableAggBufferOffset: Int = 0,
    inputAggBufferOffset: Int = 0)
  extends TypedImperativeAggregate[Any] with TypedAggregateExpression with NonSQLExpression {

  override def deterministic: Boolean = true

  override def children: Seq[Expression] = inputDeserializer.toSeq

  override lazy val resolved: Boolean = inputDeserializer.isDefined && childrenResolved

  override def references: AttributeSet = AttributeSet(inputDeserializer.toSeq)

  override def createAggregationBuffer(): Any = aggregator.zero

  private lazy val inputRowToObj = GenerateSafeProjection.generate(inputDeserializer.get :: Nil)

  override def update(buffer: Any, input: InternalRow): Any = {
    val inputObj = inputRowToObj(input).get(0, ObjectType(classOf[Any]))
    if (inputObj != null) {
      aggregator.reduce(buffer, inputObj)
    } else {
      buffer
    }
  }

  override def merge(buffer: Any, input: Any): Any = {
    aggregator.merge(buffer, input)
  }

  private lazy val resultObjToRow = dataType match {
    case _: StructType =>
      UnsafeProjection.create(CreateStruct(outputSerializer))
    case _ =>
      assert(outputSerializer.length == 1)
      UnsafeProjection.create(outputSerializer.head)
  }

  override def eval(buffer: Any): Any = {
    val resultObj = aggregator.finish(buffer)
    if (resultObj == null) {
      null
    } else {
      resultObjToRow(InternalRow(resultObj)).get(0, dataType)
    }
  }

  private lazy val bufferObjToRow = UnsafeProjection.create(bufferSerializer)

  override def serialize(buffer: Any): Array[Byte] = {
    bufferObjToRow(InternalRow(buffer)).getBytes
  }

  private lazy val bufferRow = new UnsafeRow(bufferSerializer.length)
  private lazy val bufferRowToObject = GenerateSafeProjection.generate(bufferDeserializer :: Nil)

  override def deserialize(storageFormat: Array[Byte]): Any = {
    bufferRow.pointTo(storageFormat, storageFormat.length)
    bufferRowToObject(bufferRow).get(0, ObjectType(classOf[Any]))
  }

  override def withNewMutableAggBufferOffset(
      newMutableAggBufferOffset: Int): ComplexTypedAggregateExpression =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(
      newInputAggBufferOffset: Int): ComplexTypedAggregateExpression =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def withInputInfo(
      deser: Expression,
      cls: Class[_],
      schema: StructType): TypedAggregateExpression = {
    copy(inputDeserializer = Some(deser), inputClass = Some(cls), inputSchema = Some(schema))
  }
=======
 * This class is a rough sketch of how to hook `Aggregator` into the Aggregation system.  It has
 * the following limitations:
 *  - It assumes the aggregator has a zero, `0`.
 */
case class TypedAggregateExpression(
    aggregator: Aggregator[Any, Any, Any],
    aEncoder: Option[ExpressionEncoder[Any]], // Should be bound.
    unresolvedBEncoder: ExpressionEncoder[Any],
    cEncoder: ExpressionEncoder[Any],
    children: Seq[Attribute],
    mutableAggBufferOffset: Int,
    inputAggBufferOffset: Int)
  extends ImperativeAggregate with Logging {

  override def withNewMutableAggBufferOffset(newMutableAggBufferOffset: Int): ImperativeAggregate =
    copy(mutableAggBufferOffset = newMutableAggBufferOffset)

  override def withNewInputAggBufferOffset(newInputAggBufferOffset: Int): ImperativeAggregate =
    copy(inputAggBufferOffset = newInputAggBufferOffset)

  override def nullable: Boolean = true

  override def dataType: DataType = if (cEncoder.flat) {
    cEncoder.schema.head.dataType
  } else {
    cEncoder.schema
  }

  override def deterministic: Boolean = true

  override lazy val resolved: Boolean = aEncoder.isDefined

  override lazy val inputTypes: Seq[DataType] = Nil

  override val aggBufferSchema: StructType = unresolvedBEncoder.schema

  override val aggBufferAttributes: Seq[AttributeReference] = aggBufferSchema.toAttributes

  val bEncoder = unresolvedBEncoder
    .resolve(aggBufferAttributes, OuterScopes.outerScopes)
    .bind(aggBufferAttributes)

  // Note: although this simply copies aggBufferAttributes, this common code can not be placed
  // in the superclass because that will lead to initialization ordering issues.
  override val inputAggBufferAttributes: Seq[AttributeReference] =
    aggBufferAttributes.map(_.newInstance())

  // We let the dataset do the binding for us.
  lazy val boundA = aEncoder.get

  private def updateBuffer(buffer: MutableRow, value: InternalRow): Unit = {
    var i = 0
    while (i < aggBufferAttributes.length) {
      val offset = mutableAggBufferOffset + i
      aggBufferSchema(i).dataType match {
        case BooleanType => buffer.setBoolean(offset, value.getBoolean(i))
        case ByteType => buffer.setByte(offset, value.getByte(i))
        case ShortType => buffer.setShort(offset, value.getShort(i))
        case IntegerType => buffer.setInt(offset, value.getInt(i))
        case LongType => buffer.setLong(offset, value.getLong(i))
        case FloatType => buffer.setFloat(offset, value.getFloat(i))
        case DoubleType => buffer.setDouble(offset, value.getDouble(i))
        case other => buffer.update(offset, value.get(i, other))
      }
      i += 1
    }
  }

  override def initialize(buffer: MutableRow): Unit = {
    val zero = bEncoder.toRow(aggregator.zero)
    updateBuffer(buffer, zero)
  }

  override def update(buffer: MutableRow, input: InternalRow): Unit = {
    val inputA = boundA.fromRow(input)
    val currentB = bEncoder.shift(mutableAggBufferOffset).fromRow(buffer)
    val merged = aggregator.reduce(currentB, inputA)
    val returned = bEncoder.toRow(merged)

    updateBuffer(buffer, returned)
  }

  override def merge(buffer1: MutableRow, buffer2: InternalRow): Unit = {
    val b1 = bEncoder.shift(mutableAggBufferOffset).fromRow(buffer1)
    val b2 = bEncoder.shift(inputAggBufferOffset).fromRow(buffer2)
    val merged = aggregator.merge(b1, b2)
    val returned = bEncoder.toRow(merged)

    updateBuffer(buffer1, returned)
  }

  override def eval(buffer: InternalRow): Any = {
    val b = bEncoder.shift(mutableAggBufferOffset).fromRow(buffer)
    val result = cEncoder.toRow(aggregator.finish(b))
    dataType match {
      case _: StructType => result
      case _ => result.get(0, dataType)
    }
  }

  override def toString: String = {
    s"""${aggregator.getClass.getSimpleName}(${children.mkString(",")})"""
  }

  override def nodeName: String = aggregator.getClass.getSimpleName
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
}
