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

package org.apache.spark.ml.source.libsvm

import java.io.{File, IOException}
import java.nio.charset.StandardCharsets
import java.util.List

import com.google.common.io.Files

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.util.Utils


class LibSVMRelationSuite extends SparkFunSuite with MLlibTestSparkContext {
  // Path for dataset
  var path: String = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val lines0 =
      """
        |1 1:1.0 3:2.0 5:3.0
        |0
      """.stripMargin
    val lines1 =
      """
        |0 2:4.0 4:5.0 6:6.0
      """.stripMargin
    val dir = Utils.createTempDir()
    val file = new File(dir, "part-00000")
    Files.write(lines, file, StandardCharsets.UTF_8)
    path = dir.getPath
  }

  override def afterAll(): Unit = {
    try {
      Utils.deleteRecursively(new File(path))
    } finally {
      super.afterAll()
    }
  }

  test("select as sparse vector") {
    val df = spark.read.format("libsvm").load(path)
    assert(df.columns(0) == "label")
    assert(df.columns(1) == "features")
    val row1 = df.first()
    assert(row1.getDouble(0) == 1.0)
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
  }

  test("select as dense vector") {
    val df = spark.read.format("libsvm").options(Map("vectorType" -> "dense"))
      .load(path)
    assert(df.columns(0) == "label")
    assert(df.columns(1) == "features")
    assert(df.count() == 3)
    val row1 = df.first()
    assert(row1.getDouble(0) == 1.0)
    val v = row1.getAs[DenseVector](1)
    assert(v == Vectors.dense(1.0, 0.0, 2.0, 0.0, 3.0, 0.0))
  }

  test("illegal vector types") {
    val e = intercept[IllegalArgumentException] {
      spark.read.format("libsvm").options(Map("VectorType" -> "sparser")).load(path)
    }.getMessage
    assert(e.contains("Invalid value `sparser` for parameter `vectorType`. Expected " +
      "types are `sparse` and `dense`."))
  }

  test("select a vector with specifying the longer dimension") {
    val df = spark.read.option("numFeatures", "100").format("libsvm")
      .load(path)
    val row1 = df.first()
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(100, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
  }

  test("case insensitive option") {
    val df = spark.read.option("NuMfEaTuReS", "100").format("libsvm").load(path)
    assert(df.first().getAs[SparseVector](1) ==
      Vectors.sparse(100, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
  }

  test("write libsvm data and read it again") {
    val df = spark.read.format("libsvm").load(path)
    val tempDir2 = new File(tempDir, "read_write_test")
    val writepath = tempDir2.toURI.toString
    // TODO: Remove requirement to coalesce by supporting multiple reads.
    df.coalesce(1).write.format("libsvm").mode(SaveMode.Overwrite).save(writePath)

    val df2 = spark.read.format("libsvm").load(writepath)
    val row1 = df2.first()
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(6, Seq((0, 1.0), (2, 2.0), (4, 3.0))))
  }

  test("write libsvm data failed due to invalid schema") {
    val df = spark.read.format("text").load(path)
    intercept[SparkException] {
      df.write.format("libsvm").save(path + "_2")
    }
  }

  test("write libsvm data from scratch and read it again") {
    val rawData = new java.util.ArrayList[Row]()
    rawData.add(Row(1.0, Vectors.sparse(3, Seq((0, 2.0), (1, 3.0)))))
    rawData.add(Row(4.0, Vectors.sparse(3, Seq((0, 5.0), (2, 6.0)))))

    val struct = StructType(
      StructField("labelFoo", DoubleType, false) ::
      StructField("featuresBar", VectorType, false) :: Nil
    )
    val df = spark.sqlContext.createDataFrame(rawData, struct)

    val writePath = Utils.createTempDir().getPath

    df.coalesce(1).write.format("libsvm").mode(SaveMode.Overwrite).save(writePath)

    val df2 = spark.read.format("libsvm").load(writePath)
    val row1 = df2.first()
    val v = row1.getAs[SparseVector](1)
    assert(v == Vectors.sparse(3, Seq((0, 2.0), (1, 3.0))))
  }

  test("select features from libsvm relation") {
    val df = spark.read.format("libsvm").load(path)
    df.select("features").rdd.map { case Row(d: Vector) => d }.first
    df.select("features").collect
  }
}
