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
=======
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

// $example on$
import java.util.Arrays;

<<<<<<< HEAD
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;
=======
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.*;

>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import static org.apache.spark.sql.types.DataTypes.*;
// $example off$

public class JavaVectorAssemblerExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaVectorAssemblerExample")
      .getOrCreate();
=======
    SparkConf conf = new SparkConf().setAppName("JavaVectorAssemblerExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    // $example on$
    StructType schema = createStructType(new StructField[]{
      createStructField("id", IntegerType, false),
      createStructField("hour", IntegerType, false),
      createStructField("mobile", DoubleType, false),
      createStructField("userFeatures", new VectorUDT(), false),
      createStructField("clicked", DoubleType, false)
    });
    Row row = RowFactory.create(0, 18, 1.0, Vectors.dense(0.0, 10.0, 0.5), 1.0);
<<<<<<< HEAD
    Dataset<Row> dataset = spark.createDataFrame(Arrays.asList(row), schema);
=======
    JavaRDD<Row> rdd = jsc.parallelize(Arrays.asList(row));
    DataFrame dataset = sqlContext.createDataFrame(rdd, schema);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    VectorAssembler assembler = new VectorAssembler()
      .setInputCols(new String[]{"hour", "mobile", "userFeatures"})
      .setOutputCol("features");

<<<<<<< HEAD
    Dataset<Row> output = assembler.transform(dataset);
    System.out.println("Assembled columns 'hour', 'mobile', 'userFeatures' to vector column " +
        "'features'");
    output.select("features", "clicked").show(false);
    // $example off$

    spark.stop();
=======
    DataFrame output = assembler.transform(dataset);
    System.out.println(output.select("features", "clicked").first());
    // $example off$
    jsc.stop();
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}

