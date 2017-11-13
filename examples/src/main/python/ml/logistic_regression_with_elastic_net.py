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

from __future__ import print_function

<<<<<<< HEAD
# $example on$
from pyspark.ml.classification import LogisticRegression
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("LogisticRegressionWithElasticNet")\
        .getOrCreate()

    # $example on$
    # Load training data
    training = spark.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
=======
from pyspark import SparkContext
from pyspark.sql import SQLContext
# $example on$
from pyspark.ml.classification import LogisticRegression
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="LogisticRegressionWithElasticNet")
    sqlContext = SQLContext(sc)

    # $example on$
    # Load training data
    training = sqlContext.read.format("libsvm").load("data/mllib/sample_libsvm_data.txt")
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    lr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

    # Fit the model
    lrModel = lr.fit(training)

    # Print the coefficients and intercept for logistic regression
    print("Coefficients: " + str(lrModel.coefficients))
    print("Intercept: " + str(lrModel.intercept))
<<<<<<< HEAD

    # We can also use the multinomial family for binary classification
    mlr = LogisticRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8, family="multinomial")

    # Fit the model
    mlrModel = mlr.fit(training)

    # Print the coefficients and intercepts for logistic regression with multinomial family
    print("Multinomial coefficients: " + str(mlrModel.coefficientMatrix))
    print("Multinomial intercepts: " + str(mlrModel.interceptVector))
    # $example off$

    spark.stop()
=======
    # $example off$

    sc.stop()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
