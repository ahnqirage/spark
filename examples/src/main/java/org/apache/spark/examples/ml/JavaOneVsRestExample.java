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
import org.apache.commons.cli.*;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
// $example on$
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.OneVsRest;
import org.apache.spark.ml.classification.OneVsRestModel;
<<<<<<< HEAD
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
// $example off$
import org.apache.spark.sql.SparkSession;

=======
import org.apache.spark.ml.util.MetadataUtils;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.StructField;
// $example off$
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

/**
 * An example of Multiclass to Binary Reduction with One Vs Rest,
 * using Logistic Regression as the base classifier.
 * Run with
 * <pre>
 * bin/run-example ml.JavaOneVsRestExample
 * </pre>
 */
public class JavaOneVsRestExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaOneVsRestExample")
      .getOrCreate();

    // $example on$
    // load data file.
    Dataset<Row> inputData = spark.read().format("libsvm")
      .load("data/mllib/sample_multiclass_classification_data.txt");

    // generate the train/test split.
    Dataset<Row>[] tmp = inputData.randomSplit(new double[]{0.8, 0.2});
    Dataset<Row> train = tmp[0];
    Dataset<Row> test = tmp[1];

    // configure the base classifier.
=======
    // parse the arguments
    Params params = parse(args);
    SparkConf conf = new SparkConf().setAppName("JavaOneVsRestExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // $example on$
    // configure the base classifier
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    LogisticRegression classifier = new LogisticRegression()
      .setMaxIter(10)
      .setTol(1E-6)
      .setFitIntercept(true);

    // instantiate the One Vs Rest Classifier.
    OneVsRest ovr = new OneVsRest().setClassifier(classifier);

<<<<<<< HEAD
    // train the multiclass model.
    OneVsRestModel ovrModel = ovr.fit(train);

    // score the model on test data.
    Dataset<Row> predictions = ovrModel.transform(test)
=======
    String input = params.input;
    DataFrame inputData = jsql.read().format("libsvm").load(input);
    DataFrame train;
    DataFrame test;

    // compute the train/ test split: if testInput is not provided use part of input
    String testInput = params.testInput;
    if (testInput != null) {
      train = inputData;
      // compute the number of features in the training set.
      int numFeatures = inputData.first().<Vector>getAs(1).size();
      test = jsql.read().format("libsvm").option("numFeatures",
        String.valueOf(numFeatures)).load(testInput);
    } else {
      double f = params.fracTest;
      DataFrame[] tmp = inputData.randomSplit(new double[]{1 - f, f}, 12345);
      train = tmp[0];
      test = tmp[1];
    }

    // train the multiclass model
    OneVsRestModel ovrModel = ovr.fit(train.cache());

    // score the model on test data
    DataFrame predictions = ovrModel.transform(test.cache())
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      .select("prediction", "label");

    // obtain evaluator.
    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
            .setMetricName("accuracy");

<<<<<<< HEAD
    // compute the classification error on test data.
    double accuracy = evaluator.evaluate(predictions);
    System.out.println("Test Error = " + (1 - accuracy));
=======
    Matrix confusionMatrix = metrics.confusionMatrix();
    // output the Confusion Matrix
    System.out.println("Confusion Matrix");
    System.out.println(confusionMatrix);
    System.out.println();
    System.out.println(results);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    // $example off$

    spark.stop();
  }

}
