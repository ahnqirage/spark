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
package org.apache.spark.examples.ml;
// $example on$
<<<<<<< HEAD
=======
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.ml.regression.DecisionTreeRegressionModel;
import org.apache.spark.ml.regression.DecisionTreeRegressor;
<<<<<<< HEAD
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
=======
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
// $example off$

public class JavaDecisionTreeRegressionExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaDecisionTreeRegressionExample")
      .getOrCreate();
    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    Dataset<Row> data = spark.read().format("libsvm")
=======
    SparkConf conf = new SparkConf().setAppName("JavaDecisionTreeRegressionExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);
    // $example on$
    // Load the data stored in LIBSVM format as a DataFrame.
    DataFrame data = sqlContext.read().format("libsvm")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      .load("data/mllib/sample_libsvm_data.txt");

    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    VectorIndexerModel featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data);

<<<<<<< HEAD
    // Split the data into training and test sets (30% held out for testing).
    Dataset<Row>[] splits = data.randomSplit(new double[]{0.7, 0.3});
    Dataset<Row> trainingData = splits[0];
    Dataset<Row> testData = splits[1];
=======
    // Split the data into training and test sets (30% held out for testing)
    DataFrame[] splits = data.randomSplit(new double[]{0.7, 0.3});
    DataFrame trainingData = splits[0];
    DataFrame testData = splits[1];
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    // Train a DecisionTree model.
    DecisionTreeRegressor dt = new DecisionTreeRegressor()
      .setFeaturesCol("indexedFeatures");

<<<<<<< HEAD
    // Chain indexer and tree in a Pipeline.
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[]{featureIndexer, dt});

    // Train model. This also runs the indexer.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    Dataset<Row> predictions = model.transform(testData);
=======
    // Chain indexer and tree in a Pipeline
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[]{featureIndexer, dt});

    // Train model.  This also runs the indexer.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
    DataFrame predictions = model.transform(testData);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    // Select example rows to display.
    predictions.select("label", "features").show(5);

<<<<<<< HEAD
    // Select (prediction, true label) and compute test error.
=======
    // Select (prediction, true label) and compute test error
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    RegressionEvaluator evaluator = new RegressionEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("rmse");
    double rmse = evaluator.evaluate(predictions);
    System.out.println("Root Mean Squared Error (RMSE) on test data = " + rmse);

    DecisionTreeRegressionModel treeModel =
      (DecisionTreeRegressionModel) (model.stages()[1]);
    System.out.println("Learned regression tree model:\n" + treeModel.toDebugString());
    // $example off$
<<<<<<< HEAD

    spark.stop();
=======
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
