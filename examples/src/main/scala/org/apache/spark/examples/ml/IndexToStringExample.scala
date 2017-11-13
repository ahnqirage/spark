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

// scalastyle:off println
package org.apache.spark.examples.ml

<<<<<<< HEAD
// $example on$
import org.apache.spark.ml.attribute.Attribute
import org.apache.spark.ml.feature.{IndexToString, StringIndexer}
// $example off$
import org.apache.spark.sql.SparkSession

object IndexToStringExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName("IndexToStringExample")
      .getOrCreate()

    // $example on$
    val df = spark.createDataFrame(Seq(
=======
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
// $example on$
import org.apache.spark.ml.feature.{StringIndexer, IndexToString}
// $example off$

object IndexToStringExample {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("IndexToStringExample")
    val sc = new SparkContext(conf)

    val sqlContext = SQLContext.getOrCreate(sc)

    // $example on$
    val df = sqlContext.createDataFrame(Seq(
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      (0, "a"),
      (1, "b"),
      (2, "c"),
      (3, "a"),
      (4, "a"),
      (5, "c")
    )).toDF("id", "category")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .fit(df)
    val indexed = indexer.transform(df)

<<<<<<< HEAD
    println(s"Transformed string column '${indexer.getInputCol}' " +
        s"to indexed column '${indexer.getOutputCol}'")
    indexed.show()

    val inputColSchema = indexed.schema(indexer.getOutputCol)
    println(s"StringIndexer will store labels in output column metadata: " +
        s"${Attribute.fromStructField(inputColSchema).toString}\n")

=======
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    val converter = new IndexToString()
      .setInputCol("categoryIndex")
      .setOutputCol("originalCategory")

    val converted = converter.transform(indexed)
<<<<<<< HEAD

    println(s"Transformed indexed column '${converter.getInputCol}' back to original string " +
        s"column '${converter.getOutputCol}' using labels in metadata")
    converted.select("id", "categoryIndex", "originalCategory").show()
    // $example off$

    spark.stop()
=======
    converted.select("id", "originalCategory").show()
    // $example off$
    sc.stop()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
// scalastyle:on println
