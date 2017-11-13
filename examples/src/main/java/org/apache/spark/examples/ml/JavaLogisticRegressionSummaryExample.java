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
import org.apache.spark.ml.classification.BinaryLogisticRegressionSummary;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.classification.LogisticRegressionTrainingSummary;
<<<<<<< HEAD
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
=======
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.sql.functions;
// $example off$

public class JavaLogisticRegressionSummaryExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaLogisticRegressionSummaryExample")
      .getOrCreate();

    // Load training data
    Dataset<Row> training = spark.read().format("libsvm")
=======
    SparkConf conf = new SparkConf().setAppName("JavaLogisticRegressionSummaryExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // Load training data
    DataFrame training = sqlContext.read().format("libsvm")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      .load("data/mllib/sample_libsvm_data.txt");

    LogisticRegression lr = new LogisticRegression()
      .setMaxIter(10)
      .setRegParam(0.3)
      .setElasticNetParam(0.8);

    // Fit the model
    LogisticRegressionModel lrModel = lr.fit(training);

    // $example on$
    // Extract the summary from the returned LogisticRegressionModel instance trained in the earlier
    // example
    LogisticRegressionTrainingSummary trainingSummary = lrModel.summary();

    // Obtain the loss per iteration.
    double[] objectiveHistory = trainingSummary.objectiveHistory();
    for (double lossPerIteration : objectiveHistory) {
      System.out.println(lossPerIteration);
    }

    // Obtain the metrics useful to judge performance on test data.
    // We cast the summary to a BinaryLogisticRegressionSummary since the problem is a binary
    // classification problem.
    BinaryLogisticRegressionSummary binarySummary =
      (BinaryLogisticRegressionSummary) trainingSummary;

    // Obtain the receiver-operating characteristic as a dataframe and areaUnderROC.
<<<<<<< HEAD
    Dataset<Row> roc = binarySummary.roc();
=======
    DataFrame roc = binarySummary.roc();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    roc.show();
    roc.select("FPR").show();
    System.out.println(binarySummary.areaUnderROC());

    // Get the threshold corresponding to the maximum F-Measure and rerun LogisticRegression with
    // this selected threshold.
<<<<<<< HEAD
    Dataset<Row> fMeasure = binarySummary.fMeasureByThreshold();
=======
    DataFrame fMeasure = binarySummary.fMeasureByThreshold();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    double maxFMeasure = fMeasure.select(functions.max("F-Measure")).head().getDouble(0);
    double bestThreshold = fMeasure.where(fMeasure.col("F-Measure").equalTo(maxFMeasure))
      .select("threshold").head().getDouble(0);
    lrModel.setThreshold(bestThreshold);
    // $example off$

<<<<<<< HEAD
    spark.stop();
=======
    jsc.stop();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
