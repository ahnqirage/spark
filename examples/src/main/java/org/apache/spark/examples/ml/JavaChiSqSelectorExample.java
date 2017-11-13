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
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

// $example on$
import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
=======
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

// $example on$
import java.util.Arrays;

import org.apache.spark.ml.feature.ChiSqSelector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaChiSqSelectorExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaChiSqSelectorExample")
      .getOrCreate();

    // $example on$
    List<Row> data = Arrays.asList(
      RowFactory.create(7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      RowFactory.create(8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      RowFactory.create(9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    );
=======
    SparkConf conf = new SparkConf().setAppName("JavaChiSqSelectorExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
      RowFactory.create(7, Vectors.dense(0.0, 0.0, 18.0, 1.0), 1.0),
      RowFactory.create(8, Vectors.dense(0.0, 1.0, 12.0, 0.0), 0.0),
      RowFactory.create(9, Vectors.dense(1.0, 0.0, 15.0, 0.1), 0.0)
    ));
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    StructType schema = new StructType(new StructField[]{
      new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
      new StructField("clicked", DataTypes.DoubleType, false, Metadata.empty())
    });

<<<<<<< HEAD
    Dataset<Row> df = spark.createDataFrame(data, schema);
=======
    DataFrame df = sqlContext.createDataFrame(jrdd, schema);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    ChiSqSelector selector = new ChiSqSelector()
      .setNumTopFeatures(1)
      .setFeaturesCol("features")
      .setLabelCol("clicked")
      .setOutputCol("selectedFeatures");

<<<<<<< HEAD
    Dataset<Row> result = selector.fit(df).transform(df);

    System.out.println("ChiSqSelector output with top " + selector.getNumTopFeatures()
        + " features selected");
    result.show();

    // $example off$
    spark.stop();
=======
    DataFrame result = selector.fit(df).transform(df);
    result.show();
    // $example off$
    jsc.stop();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
