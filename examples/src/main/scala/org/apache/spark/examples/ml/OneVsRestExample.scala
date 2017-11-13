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

<<<<<<< HEAD
// $example on$
import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
// $example off$
import org.apache.spark.sql.SparkSession
=======
import java.util.concurrent.TimeUnit.{NANOSECONDS => NANO}

import scopt.OptionParser

import org.apache.spark.{SparkContext, SparkConf}
// $example on$
import org.apache.spark.examples.mllib.AbstractParams
import org.apache.spark.ml.classification.{OneVsRest, LogisticRegression}
import org.apache.spark.ml.util.MetadataUtils
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.sql.DataFrame
// $example off$
import org.apache.spark.sql.SQLContext
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

/**
 * An example of Multiclass to Binary Reduction with One Vs Rest,
 * using Logistic Regression as the base classifier.
 * Run with
 * {{{
 * ./bin/run-example ml.OneVsRestExample
 * }}}
 */

object OneVsRestExample {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder
      .appName(s"OneVsRestExample")
      .getOrCreate()

    // $example on$
    // load data file.
    val inputData = spark.read.format("libsvm")
      .load("data/mllib/sample_multiclass_classification_data.txt")

<<<<<<< HEAD
    // generate the train/test split.
    val Array(train, test) = inputData.randomSplit(Array(0.8, 0.2))
=======
  private def run(params: Params) {
    val conf = new SparkConf().setAppName(s"OneVsRestExample with $params")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    // $example on$
    val inputData = sqlContext.read.format("libsvm").load(params.input)
    // compute the train/test split: if testInput is not provided use part of input.
    val data = params.testInput match {
      case Some(t) => {
        // compute the number of features in the training set.
        val numFeatures = inputData.first().getAs[Vector](1).size
        val testData = sqlContext.read.option("numFeatures", numFeatures.toString)
          .format("libsvm").load(t)
        Array[DataFrame](inputData, testData)
      }
      case None => {
        val f = params.fracTest
        inputData.randomSplit(Array(1 - f, f), seed = 12345)
      }
    }
    val Array(train, test) = data.map(_.cache())
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    // instantiate the base classifier
    val classifier = new LogisticRegression()
      .setMaxIter(10)
      .setTol(1E-6)
      .setFitIntercept(true)

    // instantiate the One Vs Rest Classifier.
    val ovr = new OneVsRest().setClassifier(classifier)

    // train the multiclass model.
    val ovrModel = ovr.fit(train)

    // score the model on test data.
    val predictions = ovrModel.transform(test)

    // obtain evaluator.
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    // compute the classification error on test data.
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${1 - accuracy}")
    // $example off$

<<<<<<< HEAD
    spark.stop()
=======
    println(s" Confusion Matrix\n ${confusionMatrix.toString}\n")

    println("label\tfpr")

    println(fprs.map {case (label, fpr) => label + "\t" + fpr}.mkString("\n"))
    // $example off$

    sc.stop()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

}
// scalastyle:on println
