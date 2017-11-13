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

package org.apache.spark.examples.mllib;

// $example on$
import java.util.HashMap;
<<<<<<< HEAD
import java.util.Map;
=======
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
<<<<<<< HEAD
=======
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
// $example off$

public class JavaRandomForestClassificationExample {
  public static void main(String[] args) {
    // $example on$
    SparkConf sparkConf = new SparkConf().setAppName("JavaRandomForestClassificationExample");
    JavaSparkContext jsc = new JavaSparkContext(sparkConf);
    // Load and parse the data file.
    String datapath = "data/mllib/sample_libsvm_data.txt";
    JavaRDD<LabeledPoint> data = MLUtils.loadLibSVMFile(jsc.sc(), datapath).toJavaRDD();
    // Split the data into training and test sets (30% held out for testing)
    JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
    JavaRDD<LabeledPoint> trainingData = splits[0];
    JavaRDD<LabeledPoint> testData = splits[1];

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    Integer numClasses = 2;
<<<<<<< HEAD
    Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
=======
    HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    Integer numTrees = 3; // Use more in practice.
    String featureSubsetStrategy = "auto"; // Let the algorithm choose.
    String impurity = "gini";
    Integer maxDepth = 5;
    Integer maxBins = 32;
    Integer seed = 12345;

<<<<<<< HEAD
    RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
=======
    final RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
      seed);

    // Evaluate model on test instances and compute test error
    JavaPairRDD<Double, Double> predictionAndLabel =
<<<<<<< HEAD
      testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()));
    double testErr =
      predictionAndLabel.filter(pl -> !pl._1().equals(pl._2())).count() / (double) testData.count();
=======
      testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
        @Override
        public Tuple2<Double, Double> call(LabeledPoint p) {
          return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
        }
      });
    Double testErr =
      1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
        @Override
        public Boolean call(Tuple2<Double, Double> pl) {
          return !pl._1().equals(pl._2());
        }
      }).count() / testData.count();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    System.out.println("Test Error: " + testErr);
    System.out.println("Learned classification forest model:\n" + model.toDebugString());

    // Save and load model
    model.save(jsc.sc(), "target/tmp/myRandomForestClassificationModel");
    RandomForestModel sameModel = RandomForestModel.load(jsc.sc(),
      "target/tmp/myRandomForestClassificationModel");
    // $example off$
<<<<<<< HEAD

    jsc.stop();
=======
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
