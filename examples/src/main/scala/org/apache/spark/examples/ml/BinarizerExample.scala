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
import org.apache.spark.ml.feature.Binarizer
// $example off$
<<<<<<< HEAD
import org.apache.spark.sql.SparkSession

object BinarizerExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("BinarizerExample")
      .getOrCreate()

    // $example on$
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame = spark.createDataFrame(data).toDF("id", "feature")
=======
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object BinarizerExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("BinarizerExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    // $example on$
    val data = Array((0, 0.1), (1, 0.8), (2, 0.2))
    val dataFrame: DataFrame = sqlContext.createDataFrame(data).toDF("label", "feature")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    val binarizer: Binarizer = new Binarizer()
      .setInputCol("feature")
      .setOutputCol("binarized_feature")
      .setThreshold(0.5)

    val binarizedDataFrame = binarizer.transform(dataFrame)
<<<<<<< HEAD

    println(s"Binarizer output with Threshold = ${binarizer.getThreshold}")
    binarizedDataFrame.show()
    // $example off$

    spark.stop()
=======
    val binarizedFeatures = binarizedDataFrame.select("binarized_feature")
    binarizedFeatures.collect().foreach(println)
    // $example off$
    sc.stop()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
// scalastyle:on println
