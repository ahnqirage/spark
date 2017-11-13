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
import org.apache.spark.ml.feature.RFormula
// $example off$
<<<<<<< HEAD
import org.apache.spark.sql.SparkSession

object RFormulaExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("RFormulaExample")
      .getOrCreate()

    // $example on$
    val dataset = spark.createDataFrame(Seq(
=======
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object RFormulaExample {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RFormulaExample")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    val dataset = sqlContext.createDataFrame(Seq(
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      (7, "US", 18, 1.0),
      (8, "CA", 12, 0.0),
      (9, "NZ", 15, 0.0)
    )).toDF("id", "country", "hour", "clicked")
<<<<<<< HEAD

=======
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    val formula = new RFormula()
      .setFormula("clicked ~ country + hour")
      .setFeaturesCol("features")
      .setLabelCol("label")
<<<<<<< HEAD

    val output = formula.fit(dataset).transform(dataset)
    output.select("features", "label").show()
    // $example off$

    spark.stop()
=======
    val output = formula.fit(dataset).transform(dataset)
    output.select("features", "label").show()
    // $example off$
    sc.stop()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
// scalastyle:on println
