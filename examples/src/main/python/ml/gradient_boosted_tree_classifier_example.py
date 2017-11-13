#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Gradient Boosted Tree Classifier Example.
"""
from __future__ import print_function

<<<<<<< HEAD
=======
import sys

from pyspark import SparkContext, SQLContext
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
# $example on$
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# $example off$
<<<<<<< HEAD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("GradientBoostedTreeClassifierExample")\
        .getOrCreate()

    # $example on$
    # Load and parse the data file, converting it to a DataFrame.
    data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
=======

if __name__ == "__main__":
    sc = SparkContext(appName="gradient_boosted_tree_classifier_example")
    sqlContext = SQLContext(sc)

    # $example on$
    # Load and parse the data file, converting it to a DataFrame.
    data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
    # Automatically identify categorical features, and index them.
    # Set maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer =\
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a GBT model.
    gbt = GBTClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures", maxIter=10)

    # Chain indexers and GBT in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, gbt])

    # Train model.  This also runs the indexers.
    model = pipeline.fit(trainingData)

    # Make predictions.
    predictions = model.transform(testData)

    # Select example rows to display.
    predictions.select("prediction", "indexedLabel", "features").show(5)

    # Select (prediction, true label) and compute test error
    evaluator = MulticlassClassificationEvaluator(
<<<<<<< HEAD
        labelCol="indexedLabel", predictionCol="prediction", metricName="accuracy")
=======
        labelCol="indexedLabel", predictionCol="prediction", metricName="precision")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g" % (1.0 - accuracy))

    gbtModel = model.stages[2]
    print(gbtModel)  # summary only
    # $example off$

<<<<<<< HEAD
    spark.stop()
=======
    sc.stop()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
