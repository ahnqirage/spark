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
from pyspark.ml.feature import PCA
from pyspark.ml.linalg import Vectors
# $example off$
from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .appName("PCAExample")\
        .getOrCreate()
=======
from pyspark import SparkContext
from pyspark.sql import SQLContext
# $example on$
from pyspark.ml.feature import PCA
from pyspark.mllib.linalg import Vectors
# $example off$

if __name__ == "__main__":
    sc = SparkContext(appName="PCAExample")
    sqlContext = SQLContext(sc)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    # $example on$
    data = [(Vectors.sparse(5, [(1, 1.0), (3, 7.0)]),),
            (Vectors.dense([2.0, 0.0, 3.0, 4.0, 5.0]),),
            (Vectors.dense([4.0, 0.0, 0.0, 6.0, 7.0]),)]
<<<<<<< HEAD
    df = spark.createDataFrame(data, ["features"])

    pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(df)

=======
    df = sqlContext.createDataFrame(data, ["features"])
    pca = PCA(k=3, inputCol="features", outputCol="pcaFeatures")
    model = pca.fit(df)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    result = model.transform(df).select("pcaFeatures")
    result.show(truncate=False)
    # $example off$

<<<<<<< HEAD
    spark.stop()
=======
    sc.stop()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
