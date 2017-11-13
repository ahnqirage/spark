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
import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
=======
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

// $example on$
import org.apache.spark.ml.feature.Normalizer;
import org.apache.spark.sql.DataFrame;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
// $example off$

public class JavaNormalizerExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaNormalizerExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
        RowFactory.create(0, Vectors.dense(1.0, 0.1, -8.0)),
        RowFactory.create(1, Vectors.dense(2.0, 1.0, -4.0)),
        RowFactory.create(2, Vectors.dense(4.0, 10.0, 8.0))
    );
    StructType schema = new StructType(new StructField[]{
        new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
        new StructField("features", new VectorUDT(), false, Metadata.empty())
    });
    Dataset<Row> dataFrame = spark.createDataFrame(data, schema);
=======
    SparkConf conf = new SparkConf().setAppName("JavaNormalizerExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // $example on$
    DataFrame dataFrame = jsql.read().format("libsvm").load("data/mllib/sample_libsvm_data.txt");
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    // Normalize each Vector using $L^1$ norm.
    Normalizer normalizer = new Normalizer()
      .setInputCol("features")
      .setOutputCol("normFeatures")
      .setP(1.0);

<<<<<<< HEAD
    Dataset<Row> l1NormData = normalizer.transform(dataFrame);
    l1NormData.show();

    // Normalize each Vector using $L^\infty$ norm.
    Dataset<Row> lInfNormData =
      normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
    lInfNormData.show();
    // $example off$

    spark.stop();
  }
}
=======
    DataFrame l1NormData = normalizer.transform(dataFrame);
    l1NormData.show();

    // Normalize each Vector using $L^\infty$ norm.
    DataFrame lInfNormData =
      normalizer.transform(dataFrame, normalizer.p().w(Double.POSITIVE_INFINITY));
    lInfNormData.show();
    // $example off$
    jsc.stop();
  }
}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
