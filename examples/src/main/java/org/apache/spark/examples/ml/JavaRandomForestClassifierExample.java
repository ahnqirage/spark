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
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.RandomForestClassificationModel;
import org.apache.spark.ml.classification.RandomForestClassifier;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.*;
<<<<<<< HEAD
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
=======
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
// $example off$

public class JavaRandomForestClassifierExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaRandomForestClassifierExample")
      .getOrCreate();

    // $example on$
    // Load and parse the data file, converting it to a DataFrame.
    Dataset<Row> data = spark.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
=======
    SparkConf conf = new SparkConf().setAppName("JavaRandomForestClassifierExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    // Load and parse the data file, converting it to a DataFrame.
    DataFrame data = sqlContext.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    // Index labels, adding metadata to the label column.
    // Fit on whole dataset to include all labels in index.
    StringIndexerModel labelIndexer = new StringIndexer()
      .setInputCol("label")
      .setOutputCol("indexedLabel")
      .fit(data);
    // Automatically identify categorical features, and index them.
    // Set maxCategories so features with > 4 distinct values are treated as continuous.
    VectorIndexerModel featureIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexedFeatures")
      .setMaxCategories(4)
      .fit(data);

    // Split the data into training and test sets (30% held out for testing)
<<<<<<< HEAD
    Dataset<Row>[] splits = data.randomSplit(new double[] {0.7, 0.3});
    Dataset<Row> trainingData = splits[0];
    Dataset<Row> testData = splits[1];
=======
    DataFrame[] splits = data.randomSplit(new double[] {0.7, 0.3});
    DataFrame trainingData = splits[0];
    DataFrame testData = splits[1];
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    // Train a RandomForest model.
    RandomForestClassifier rf = new RandomForestClassifier()
      .setLabelCol("indexedLabel")
      .setFeaturesCol("indexedFeatures");

    // Convert indexed labels back to original labels.
    IndexToString labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(labelIndexer.labels());

    // Chain indexers and forest in a Pipeline
    Pipeline pipeline = new Pipeline()
      .setStages(new PipelineStage[] {labelIndexer, featureIndexer, rf, labelConverter});

    // Train model. This also runs the indexers.
    PipelineModel model = pipeline.fit(trainingData);

    // Make predictions.
<<<<<<< HEAD
    Dataset<Row> predictions = model.transform(testData);
=======
    DataFrame predictions = model.transform(testData);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    // Select example rows to display.
    predictions.select("predictedLabel", "label", "features").show(5);

    // Select (prediction, true label) and compute test error
    MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
<<<<<<< HEAD
      .setMetricName("accuracy");
=======
      .setMetricName("precision");
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    double accuracy = evaluator.evaluate(predictions);
    System.out.println("Test Error = " + (1.0 - accuracy));

    RandomForestClassificationModel rfModel = (RandomForestClassificationModel)(model.stages()[2]);
    System.out.println("Learned classification forest model:\n" + rfModel.toDebugString());
    // $example off$

<<<<<<< HEAD
    spark.stop();
=======
    jsc.stop();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
