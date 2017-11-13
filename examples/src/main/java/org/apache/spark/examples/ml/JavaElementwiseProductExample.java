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
=======
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

// $example on$
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

<<<<<<< HEAD
import org.apache.spark.ml.feature.ElementwiseProduct;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.linalg.Vectors;
=======
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.feature.ElementwiseProduct;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
// $example off$

public class JavaElementwiseProductExample {
  public static void main(String[] args) {
<<<<<<< HEAD
    SparkSession spark = SparkSession
      .builder()
      .appName("JavaElementwiseProductExample")
      .getOrCreate();

    // $example on$
    // Create some vector data; also works for sparse vectors
    List<Row> data = Arrays.asList(
      RowFactory.create("a", Vectors.dense(1.0, 2.0, 3.0)),
      RowFactory.create("b", Vectors.dense(4.0, 5.0, 6.0))
    );

    List<StructField> fields = new ArrayList<>(2);
=======
    SparkConf conf = new SparkConf().setAppName("JavaElementwiseProductExample");
    JavaSparkContext jsc = new JavaSparkContext(conf);
    SQLContext sqlContext = new SQLContext(jsc);

    // $example on$
    // Create some vector data; also works for sparse vectors
    JavaRDD<Row> jrdd = jsc.parallelize(Arrays.asList(
      RowFactory.create("a", Vectors.dense(1.0, 2.0, 3.0)),
      RowFactory.create("b", Vectors.dense(4.0, 5.0, 6.0))
    ));

    List<StructField> fields = new ArrayList<StructField>(2);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    fields.add(DataTypes.createStructField("id", DataTypes.StringType, false));
    fields.add(DataTypes.createStructField("vector", new VectorUDT(), false));

    StructType schema = DataTypes.createStructType(fields);

<<<<<<< HEAD
    Dataset<Row> dataFrame = spark.createDataFrame(data, schema);
=======
    DataFrame dataFrame = sqlContext.createDataFrame(jrdd, schema);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    Vector transformingVector = Vectors.dense(0.0, 1.0, 2.0);

    ElementwiseProduct transformer = new ElementwiseProduct()
      .setScalingVec(transformingVector)
      .setInputCol("vector")
      .setOutputCol("transformedVector");

    // Batch transform the vectors to create new column:
    transformer.transform(dataFrame).show();
    // $example off$
<<<<<<< HEAD
    spark.stop();
  }
}
=======
    jsc.stop();
  }
}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
