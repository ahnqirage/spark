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

package org.apache.spark.sql.execution

import scala.util.Random

import org.apache.spark.AccumulatorSuite
<<<<<<< HEAD
import org.apache.spark.sql.{RandomDataGenerator, Row}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
=======
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{RandomDataGenerator, Row}

>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

/**
 * Test sorting. Many of the test cases generate random data and compares the sorted result with one
 * sorted by a reference implementation ([[ReferenceSort]]).
 */
class SortSuite extends SparkPlanTest with SharedSQLContext {
  import testImplicits.newProductEncoder
  import testImplicits.localSeqToDatasetHolder

  test("basic sorting using ExternalSort") {

    val input = Seq(
      ("Hello", 4, 2.0),
      ("Hello", 1, 1.0),
      ("World", 8, 3.0)
    )

    checkAnswer(
      input.toDF("a", "b", "c"),
<<<<<<< HEAD
      (child: SparkPlan) => SortExec('a.asc :: 'b.asc :: Nil, global = true, child = child),
=======
      (child: SparkPlan) => Sort('a.asc :: 'b.asc :: Nil, global = true, child = child),
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      input.sortBy(t => (t._1, t._2)).map(Row.fromTuple),
      sortAnswers = false)

    checkAnswer(
      input.toDF("a", "b", "c"),
<<<<<<< HEAD
      (child: SparkPlan) => SortExec('b.asc :: 'a.asc :: Nil, global = true, child = child),
=======
      (child: SparkPlan) => Sort('b.asc :: 'a.asc :: Nil, global = true, child = child),
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      input.sortBy(t => (t._2, t._1)).map(Row.fromTuple),
      sortAnswers = false)
  }

<<<<<<< HEAD
  test("sorting all nulls") {
    checkThatPlansAgree(
      (1 to 100).map(v => Tuple1(v)).toDF().selectExpr("NULL as a"),
      (child: SparkPlan) =>
        GlobalLimitExec(10, SortExec('a.asc :: Nil, global = true, child = child)),
      (child: SparkPlan) =>
        GlobalLimitExec(10, ReferenceSort('a.asc :: Nil, global = true, child)),
      sortAnswers = false
    )
  }

  test("sort followed by limit") {
    checkThatPlansAgree(
      (1 to 100).map(v => Tuple1(v)).toDF("a"),
      (child: SparkPlan) =>
        GlobalLimitExec(10, SortExec('a.asc :: Nil, global = true, child = child)),
      (child: SparkPlan) =>
        GlobalLimitExec(10, ReferenceSort('a.asc :: Nil, global = true, child)),
=======
  test("sort followed by limit") {
    checkThatPlansAgree(
      (1 to 100).map(v => Tuple1(v)).toDF("a"),
      (child: SparkPlan) => Limit(10, Sort('a.asc :: Nil, global = true, child = child)),
      (child: SparkPlan) => Limit(10, ReferenceSort('a.asc :: Nil, global = true, child)),
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      sortAnswers = false
    )
  }

  test("sorting does not crash for large inputs") {
    val sortOrder = 'a.asc :: Nil
    val stringLength = 1024 * 1024 * 2
    checkThatPlansAgree(
      Seq(Tuple1("a" * stringLength), Tuple1("b" * stringLength)).toDF("a").repartition(1),
<<<<<<< HEAD
      SortExec(sortOrder, global = true, _: SparkPlan, testSpillFrequency = 1),
=======
      Sort(sortOrder, global = true, _: SparkPlan, testSpillFrequency = 1),
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      ReferenceSort(sortOrder, global = true, _: SparkPlan),
      sortAnswers = false
    )
  }

  test("sorting updates peak execution memory") {
    AccumulatorSuite.verifyPeakExecutionMemorySet(sparkContext, "unsafe external sort") {
      checkThatPlansAgree(
        (1 to 100).map(v => Tuple1(v)).toDF("a"),
<<<<<<< HEAD
        (child: SparkPlan) => SortExec('a.asc :: Nil, global = true, child = child),
=======
        (child: SparkPlan) => Sort('a.asc :: Nil, global = true, child = child),
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
        (child: SparkPlan) => ReferenceSort('a.asc :: Nil, global = true, child),
        sortAnswers = false)
    }
  }

  // Test sorting on different data types
  for (
    dataType <- DataTypeTestUtils.atomicTypes ++ Set(NullType);
    nullable <- Seq(true, false);
<<<<<<< HEAD
    sortOrder <-
      Seq('a.asc :: Nil, 'a.asc_nullsLast :: Nil, 'a.desc :: Nil, 'a.desc_nullsFirst :: Nil);
=======
    sortOrder <- Seq('a.asc :: Nil, 'a.desc :: Nil);
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    randomDataGenerator <- RandomDataGenerator.forType(dataType, nullable)
  ) {
    test(s"sorting on $dataType with nullable=$nullable, sortOrder=$sortOrder") {
      val inputData = Seq.fill(1000)(randomDataGenerator())
<<<<<<< HEAD
      val inputDf = spark.createDataFrame(
=======
      val inputDf = sqlContext.createDataFrame(
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
        sparkContext.parallelize(Random.shuffle(inputData).map(v => Row(v))),
        StructType(StructField("a", dataType, nullable = true) :: Nil)
      )
      checkThatPlansAgree(
        inputDf,
<<<<<<< HEAD
        p => SortExec(sortOrder, global = true, p: SparkPlan, testSpillFrequency = 23),
=======
        p => ConvertToSafe(Sort(sortOrder, global = true, p: SparkPlan, testSpillFrequency = 23)),
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
        ReferenceSort(sortOrder, global = true, _: SparkPlan),
        sortAnswers = false
      )
    }
  }
}
