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

package org.apache.spark.sql

import scala.collection.immutable.{HashSet => HSet}
import scala.collection.immutable.Queue
import scala.collection.mutable.{LinkedHashMap => LHMap}
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.test.SharedSQLContext

case class IntClass(value: Int)

case class SeqClass(s: Seq[Int])

case class ListClass(l: List[Int])

case class QueueClass(q: Queue[Int])

case class MapClass(m: Map[Int, Int])

case class LHMapClass(m: LHMap[Int, Int])

case class ComplexClass(seq: SeqClass, list: ListClass, queue: QueueClass)

case class ComplexMapClass(map: MapClass, lhmap: LHMapClass)

case class InnerData(name: String, value: Int)
case class NestedData(id: Int, param: Map[String, InnerData])

package object packageobject {
  case class PackageClass(value: Int)
}

class DatasetPrimitiveSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  test("toDS") {
    val data = Seq(1, 2, 3, 4, 5, 6)
    checkDataset(
      data.toDS(),
      data: _*)
  }

  test("as case class / collect") {
    val ds = Seq(1, 2, 3).toDS().as[IntClass]
    checkDataset(
      ds,
      IntClass(1), IntClass(2), IntClass(3))

    assert(ds.collect().head == IntClass(1))
  }

  test("map") {
    val ds = Seq(1, 2, 3).toDS()
    checkDataset(
      ds.map(_ + 1),
      2, 3, 4)
  }

  test("mapPrimitive") {
    val dsInt = Seq(1, 2, 3).toDS()
    checkDataset(dsInt.map(_ > 1), false, true, true)
    checkDataset(dsInt.map(_ + 1), 2, 3, 4)
    checkDataset(dsInt.map(_ + 8589934592L), 8589934593L, 8589934594L, 8589934595L)
    checkDataset(dsInt.map(_ + 1.1F), 2.1F, 3.1F, 4.1F)
    checkDataset(dsInt.map(_ + 1.23D), 2.23D, 3.23D, 4.23D)

    val dsLong = Seq(1L, 2L, 3L).toDS()
    checkDataset(dsLong.map(_ > 1), false, true, true)
    checkDataset(dsLong.map(e => (e + 1).toInt), 2, 3, 4)
    checkDataset(dsLong.map(_ + 8589934592L), 8589934593L, 8589934594L, 8589934595L)
    checkDataset(dsLong.map(_ + 1.1F), 2.1F, 3.1F, 4.1F)
    checkDataset(dsLong.map(_ + 1.23D), 2.23D, 3.23D, 4.23D)

    val dsFloat = Seq(1F, 2F, 3F).toDS()
    checkDataset(dsFloat.map(_ > 1), false, true, true)
    checkDataset(dsFloat.map(e => (e + 1).toInt), 2, 3, 4)
    checkDataset(dsFloat.map(e => (e + 123456L).toLong), 123457L, 123458L, 123459L)
    checkDataset(dsFloat.map(_ + 1.1F), 2.1F, 3.1F, 4.1F)
    checkDataset(dsFloat.map(_ + 1.23D), 2.23D, 3.23D, 4.23D)

    val dsDouble = Seq(1D, 2D, 3D).toDS()
    checkDataset(dsDouble.map(_ > 1), false, true, true)
    checkDataset(dsDouble.map(e => (e + 1).toInt), 2, 3, 4)
    checkDataset(dsDouble.map(e => (e + 8589934592L).toLong),
      8589934593L, 8589934594L, 8589934595L)
    checkDataset(dsDouble.map(e => (e + 1.1F).toFloat), 2.1F, 3.1F, 4.1F)
    checkDataset(dsDouble.map(_ + 1.23D), 2.23D, 3.23D, 4.23D)

    val dsBoolean = Seq(true, false).toDS()
    checkDataset(dsBoolean.map(e => !e), false, true)
  }

  test("mapPrimitiveArray") {
    val dsInt = Seq(Array(1, 2), Array(3, 4)).toDS()
    checkDataset(dsInt.map(e => e), Array(1, 2), Array(3, 4))
    checkDataset(dsInt.map(e => null: Array[Int]), null, null)

    val dsDouble = Seq(Array(1D, 2D), Array(3D, 4D)).toDS()
    checkDataset(dsDouble.map(e => e), Array(1D, 2D), Array(3D, 4D))
    checkDataset(dsDouble.map(e => null: Array[Double]), null, null)
  }

  test("filter") {
    val ds = Seq(1, 2, 3, 4).toDS()
    checkDataset(
      ds.filter(_ % 2 == 0),
      2, 4)
  }

  test("filterPrimitive") {
    val dsInt = Seq(1, 2, 3).toDS()
    checkDataset(dsInt.filter(_ > 1), 2, 3)

    val dsLong = Seq(1L, 2L, 3L).toDS()
    checkDataset(dsLong.filter(_ > 1), 2L, 3L)

    val dsFloat = Seq(1F, 2F, 3F).toDS()
    checkDataset(dsFloat.filter(_ > 1), 2F, 3F)

    val dsDouble = Seq(1D, 2D, 3D).toDS()
    checkDataset(dsDouble.filter(_ > 1), 2D, 3D)

    val dsBoolean = Seq(true, false).toDS()
    checkDataset(dsBoolean.filter(e => !e), false)
  }

  test("foreach") {
    val ds = Seq(1, 2, 3).toDS()
<<<<<<< HEAD
    val acc = sparkContext.longAccumulator
    ds.foreach(acc.add(_))
=======
    val acc = sparkContext.accumulator(0)
    ds.foreach(acc += _)
    assert(acc.value == 6)
  }

  test("foreachPartition") {
    val ds = Seq(1, 2, 3).toDS()
    val acc = sparkContext.longAccumulator
    ds.foreachPartition((it: Iterator[Int]) => it.foreach(acc.add(_)))
    assert(acc.value == 6)
  }

  test("reduce") {
    val ds = Seq(1, 2, 3).toDS()
    assert(ds.reduce(_ + _) == 6)
  }

  test("groupBy function, keys") {
    val ds = Seq(1, 2, 3, 4, 5).toDS()
    val grouped = ds.groupByKey(_ % 2)
    checkDatasetUnorderly(
      grouped.keys,
      0, 1)
  }

  test("groupBy function, map") {
    val ds = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11).toDS()
    val grouped = ds.groupByKey(_ % 2)
    val agged = grouped.mapGroups { case (g, iter) =>
      val name = if (g == 0) "even" else "odd"
      (name, iter.size)
    }

    checkDatasetUnorderly(
      agged,
      ("even", 5), ("odd", 6))
  }

  test("groupBy function, flatMap") {
    val ds = Seq("a", "b", "c", "xyz", "hello").toDS()
    val grouped = ds.groupBy(_.length)
    val agged = grouped.flatMapGroups { case (g, iter) => Iterator(g.toString, iter.mkString) }

    checkAnswer(
      agged,
      "1", "abc", "3", "xyz", "5", "hello")
  }

  test("Arrays and Lists") {
    checkAnswer(Seq(Seq(1)).toDS(), Seq(1))
    checkAnswer(Seq(Seq(1.toLong)).toDS(), Seq(1.toLong))
    checkAnswer(Seq(Seq(1.toDouble)).toDS(), Seq(1.toDouble))
    checkAnswer(Seq(Seq(1.toFloat)).toDS(), Seq(1.toFloat))
    checkAnswer(Seq(Seq(1.toByte)).toDS(), Seq(1.toByte))
    checkAnswer(Seq(Seq(1.toShort)).toDS(), Seq(1.toShort))
    checkAnswer(Seq(Seq(true)).toDS(), Seq(true))
    checkAnswer(Seq(Seq("test")).toDS(), Seq("test"))
    checkAnswer(Seq(Seq(Tuple1(1))).toDS(), Seq(Tuple1(1)))

    checkAnswer(Seq(Array(1)).toDS(), Array(1))
    checkAnswer(Seq(Array(1.toLong)).toDS(), Array(1.toLong))
    checkAnswer(Seq(Array(1.toDouble)).toDS(), Array(1.toDouble))
    checkAnswer(Seq(Array(1.toFloat)).toDS(), Array(1.toFloat))
    checkAnswer(Seq(Array(1.toByte)).toDS(), Array(1.toByte))
    checkAnswer(Seq(Array(1.toShort)).toDS(), Array(1.toShort))
    checkAnswer(Seq(Array(true)).toDS(), Array(true))
    checkAnswer(Seq(Array("test")).toDS(), Array("test"))
    checkAnswer(Seq(Array(Tuple1(1))).toDS(), Array(Tuple1(1)))
  }
}
