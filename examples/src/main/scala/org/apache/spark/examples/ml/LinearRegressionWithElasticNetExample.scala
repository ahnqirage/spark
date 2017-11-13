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
import org.apache.spark.ml.regression.LinearRegression
// $example off$
<<<<<<< HEAD
import org.apache.spark.sql.SparkSession
=======
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

object LinearRegressionWithElasticNetExample {

  def main(args: Array[String]): Unit = {
<<<<<<< HEAD
    val spark = SparkSession
      .builder
      .appName("LinearRegressionWithElasticNetExample")
      .getOrCreate()

    // $example on$
    // Load training data
    val training = spark.read.format("libsvm")
=======
    val conf = new SparkConf().setAppName("LinearRegressionWithElasticNetExample")
    val sc = new SparkContext(conf)
    val sqlCtx = new SQLContext(sc)

    // $example on$
    // Load training data
    val training = sqlCtx.read.format("libsvm")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      .load("data/mllib/sample_linear_regression_data.txt")

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8)

    // Fit the model
    val lrModel = lr.fit(training)

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    val trainingSummary = lrModel.summary
    println(s"numIterations: ${trainingSummary.totalIterations}")
<<<<<<< HEAD
    println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
=======
    println(s"objectiveHistory: ${trainingSummary.objectiveHistory.toList}")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    trainingSummary.residuals.show()
    println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    println(s"r2: ${trainingSummary.r2}")
    // $example off$

<<<<<<< HEAD
    spark.stop()
=======
    sc.stop()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
// scalastyle:on println
