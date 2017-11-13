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

import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
=======
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

// $example on$
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.PCA;
import org.apache.spark.ml.feature.PCAModel;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaPCAExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaPCAExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(Vectors.sparse(5, new int[]{1, 3}, new double[]{1.0, 7.0})),
      RowFactory.create(Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0)),
      RowFactory.create(Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
    );
=======
    SparkConf conf = new SparkConf().setAppName("JavaPCAExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);

    // $example on$
    JavaRDD<Row> data = jsc.parallelize(Arrays.asList(
      RowFactory.create(Vectors.sparse(5, new int[]{1, 3}, new double[]{1.0, 7.0})),
      RowFactory.create(Vectors.dense(2.0, 0.0, 3.0, 4.0, 5.0)),
      RowFactory.create(Vectors.dense(4.0, 0.0, 0.0, 6.0, 7.0))
    ));
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    StructType schema = new StructType(new StructField[]{
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
    });

<<<<<<< HEAD
    Dataset<Row> df = spark.createDataFrame(data, schema);
=======
    DataFrame df = jsql.createDataFrame(data, schema);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    PCAModel pca = new PCA()
      .setInputCol("features")
      .setOutputCol("pcaFeatures")
      .setK(3)
      .fit(df);

<<<<<<< HEAD
    Dataset<Row> result = pca.transform(df).select("pcaFeatures");
    result.show(false);
    // $example off$
    spark.stop();
=======
    DataFrame result = pca.transform(df).select("pcaFeatures");
    result.show();
    // $example off$
    jsc.stop();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}

