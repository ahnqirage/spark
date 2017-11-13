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

import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.Dataset;
=======
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

// $example on$
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.StopWordsRemover;
import org.apache.spark.sql.DataFrame;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaStopWordsRemoverExample {

  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaStopWordsRemoverExample")
      .getOrCreate();
=======
    SparkConf conf = new SparkConf().setAppName("JavaStopWordsRemoverExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    // $example on$
    StopWordsRemover remover = new StopWordsRemover()
      .setInputCol("raw")
      .setOutputCol("filtered");

<<<<<<< HEAD
    List<Row> data = Arrays.asList(
      RowFactory.create(Arrays.asList("I", "saw", "the", "red", "balloon")),
      RowFactory.create(Arrays.asList("Mary", "had", "a", "little", "lamb"))
    );
=======
    JavaRDD<Row> rdd = jsc.parallelize(Arrays.asList(
      RowFactory.create(Arrays.asList("I", "saw", "the", "red", "baloon")),
      RowFactory.create(Arrays.asList("Mary", "had", "a", "little", "lamb"))
    ));
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    StructType schema = new StructType(new StructField[]{
      new StructField(
        "raw", DataTypes.createArrayType(DataTypes.StringType), false, Metadata.empty())
    });

<<<<<<< HEAD
    Dataset<Row> dataset = spark.createDataFrame(data, schema);
    remover.transform(dataset).show(false);
    // $example off$
    spark.stop();
=======
    DataFrame dataset = jsql.createDataFrame(rdd, schema);
    remover.transform(dataset).show();
    // $example off$
    jsc.stop();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}
