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

import org.apache.spark.ml.feature.PolynomialExpansion;
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
import org.apache.spark.ml.feature.PolynomialExpansion;
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

public class JavaPolynomialExpansionExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaPolynomialExpansionExample")
      .getOrCreate();
=======
    SparkConf conf = new SparkConf().setAppName("JavaPolynomialExpansionExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext jsql = new SQLContext(jsc);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    // $example on$
    PolynomialExpansion polyExpansion = new PolynomialExpansion()
      .setInputCol("features")
      .setOutputCol("polyFeatures")
      .setDegree(3);

<<<<<<< HEAD
    List<Row> data = Arrays.asList(
      RowFactory.create(Vectors.dense(2.0, 1.0)),
      RowFactory.create(Vectors.dense(0.0, 0.0)),
      RowFactory.create(Vectors.dense(3.0, -1.0))
    );
    StructType schema = new StructType(new StructField[]{
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
    });
    Dataset<Row> df = spark.createDataFrame(data, schema);

    Dataset<Row> polyDF = polyExpansion.transform(df);
    polyDF.show(false);
    // $example off$

    spark.stop();
  }
}
=======
    JavaRDD<Row> data = jsc.parallelize(Arrays.asList(
      RowFactory.create(Vectors.dense(-2.0, 2.3)),
      RowFactory.create(Vectors.dense(0.0, 0.0)),
      RowFactory.create(Vectors.dense(0.6, -1.1))
    ));

    StructType schema = new StructType(new StructField[]{
      new StructField("features", new VectorUDT(), false, Metadata.empty()),
    });

    DataFrame df = jsql.createDataFrame(data, schema);
    DataFrame polyDF = polyExpansion.transform(df);

    Row[] row = polyDF.select("polyFeatures").take(3);
    for (Row r : row) {
      System.out.println(r.get(0));
    }
    // $example off$
    jsc.stop();
  }
}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
