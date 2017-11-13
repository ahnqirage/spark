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
Decision Tree Classification Example.
"""
from __future__ import print_function

<<<<<<< HEAD
# $example on$
=======
import sys

# $example on$
from pyspark import SparkContext, SQLContext
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
from pyspark.ml import Pipeline
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.feature import StringIndexer, VectorIndexer
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
# $example off$
<<<<<<< HEAD
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("DecisionTreeClassificationExample")\
        .getOrCreate()

    # $example on$
    # Load the data stored in LIBSVM format as a DataFrame.
    data = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
=======

if __name__ == "__main__":
    sc = SparkContext(appName="decision_tree_classification_example")
    sqlContext = SQLContext(sc)

    # $example on$
    # Load the data stored in LIBSVM format as a DataFrame.
    data = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    # Index labels, adding metadata to the label column.
    # Fit on whole dataset to include all labels in index.
    labelIndexer = StringIndexer(inputCol="label", outputCol="indexedLabel").fit(data)
    # Automatically identify categorical features, and index them.
    # We specify maxCategories so features with > 4 distinct values are treated as continuous.
    featureIndexer =\
        VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=4).fit(data)

    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a DecisionTree model.
    dt = DecisionTreeClassifier(labelCol="indexedLabel", featuresCol="indexedFeatures")

    # Chain indexers and tree in a Pipeline
    pipeline = Pipeline(stages=[labelIndexer, featureIndexer, dt])

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
    print("Test Error = %g " % (1.0 - accuracy))

    treeModel = model.stages[2]
    # summary only
    print(treeModel)
    # $example off$
<<<<<<< HEAD

    spark.stop()
=======
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
