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

// $example on$
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer}
// $example off$
<<<<<<< HEAD
import org.apache.spark.sql.SparkSession

object OneHotEncoderExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("OneHotEncoderExample")
      .getOrCreate()

    // $example on$
    val df = spark.createDataFrame(Seq(
=======
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object OneHotEncoderExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("OneHotEncoderExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

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

    val encoder = new OneHotEncoder()
      .setInputCol("categoryIndex")
      .setOutputCol("categoryVec")
<<<<<<< HEAD

    val encoded = encoder.transform(indexed)
    encoded.show()
    // $example off$

    spark.stop()
=======
    val encoded = encoder.transform(indexed)
    encoded.select("id", "categoryVec").show()
    // $example off$
    sc.stop()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
// scalastyle:on println
