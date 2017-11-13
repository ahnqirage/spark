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

package org.apache.spark.examples.ml;

<<<<<<< HEAD
=======
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
// $example on$
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
<<<<<<< HEAD
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
=======
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
// $example off$

public class JavaLinearRegressionWithElasticNetExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaLinearRegressionWithElasticNetExample")
      .getOrCreate();

    // $example on$
    // Load training data.
    Dataset<Row> training = spark.read().format("libsvm")
=======
    SparkConf conf = new SparkConf().setAppName("JavaLinearRegressionWithElasticNetExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    // Load training data
    DataFrame training = sqlContext.read().format("libsvm")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      .load("data/mllib/sample_linear_regression_data.txt");

    LinearRegression lr = new LinearRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8);

<<<<<<< HEAD
    // Fit the model.
    LinearRegressionModel lrModel = lr.fit(training);

    // Print the coefficients and intercept for linear regression.
    System.out.println("Coefficients: "
      + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

    // Summarize the model over the training set and print out some metrics.
=======
    // Fit the model
    LinearRegressionModel lrModel = lr.fit(training);

    // Print the coefficients and intercept for linear regression
    System.out.println("Coefficients: "
      + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

    // Summarize the model over the training set and print out some metrics
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
    System.out.println("numIterations: " + trainingSummary.totalIterations());
    System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
    trainingSummary.residuals().show();
    System.out.println("RMSE: " + trainingSummary.rootMeanSquaredError());
    System.out.println("r2: " + trainingSummary.r2());
    // $example off$

<<<<<<< HEAD
    spark.stop();
=======
    jsc.stop();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
