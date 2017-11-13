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

package org.apache.spark.sql

import java.util.{ArrayDeque, Locale, TimeZone}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

<<<<<<< HEAD
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.aggregate.TypedAggregateExpression
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.streaming.MemoryPlan
import org.apache.spark.sql.types.{Metadata, ObjectType}

=======
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.expressions.aggregate.ImperativeAggregate
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{LogicalRDD, Queryable}
import org.apache.spark.sql.types.Metadata
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

abstract class QueryTest extends PlanTest {

  protected def spark: SparkSession

  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("America/Los_Angeles"))
  // Add Locale setting
  Locale.setDefault(Locale.US)

  /**
   * Runs the plan and makes sure the answer contains all of the keywords.
   */
  def checkKeywordsExist(df: DataFrame, keywords: String*): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
    }
  }

  /**
   * Runs the plan and makes sure the answer does NOT contain any of the keywords.
   */
  def checkKeywordsNotExist(df: DataFrame, keywords: String*): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
    }
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer.
   */
<<<<<<< HEAD
  protected def checkDataset[T](
      ds: => Dataset[T],
      expectedAnswer: T*): Unit = {
    val result = getResult(ds)
=======
  protected def checkAnswer[T](
      ds: Dataset[T],
      expectedAnswer: T*): Unit = {
    checkAnswer(
      ds.toDF(),
      sqlContext.createDataset(expectedAnswer)(ds.unresolvedTEncoder).toDF().collect().toSeq)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    if (!compare(result.toSeq, expectedAnswer)) {
      fail(
        s"""
           |Decoded objects do not match expected objects:
           |expected: $expectedAnswer
           |actual:   ${result.toSeq}
           |${ds.exprEnc.deserializer.treeString}
         """.stripMargin)
    }
  }

  /**
   * Evaluates a dataset to make sure that the result of calling collect matches the given
   * expected answer, after sort.
   */
  protected def checkDatasetUnorderly[T : Ordering](
      ds: => Dataset[T],
      expectedAnswer: T*): Unit = {
    val result = getResult(ds)

    if (!compare(result.toSeq.sorted, expectedAnswer.sorted)) {
      fail(
        s"""
           |Decoded objects do not match expected objects:
           |expected: $expectedAnswer
           |actual:   ${result.toSeq}
           |${ds.exprEnc.deserializer.treeString}
         """.stripMargin)
    }
  }

  private def getResult[T](ds: => Dataset[T]): Array[T] = {
    val analyzedDS = try ds catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
             """.stripMargin)
        } else {
          throw ae
        }
    }
    assertEmptyMissingInput(analyzedDS)

    try ds.collect() catch {
      case e: Exception =>
        fail(
          s"""
             |Exception collecting dataset as objects
<<<<<<< HEAD
             |${ds.exprEnc}
             |${ds.exprEnc.deserializer.treeString}
=======
             |${ds.resolvedTEncoder}
             |${ds.resolvedTEncoder.fromRowExpression.treeString}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
             |${ds.queryExecution}
           """.stripMargin, e)
    }
  }

<<<<<<< HEAD
  private def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a, b) => a == b
=======
    // Handle the case where the return type is an array
    val isArray = decoded.headOption.map(_.getClass.isArray).getOrElse(false)
    def normalEquality = decoded == expectedAnswer.toSet
    def expectedAsSeq = expectedAnswer.map(_.asInstanceOf[Array[_]].toSeq).toSet
    def decodedAsSeq = decoded.map(_.asInstanceOf[Array[_]].toSeq)

    if (!((isArray && expectedAsSeq == decodedAsSeq) || normalEquality)) {
      val expected = expectedAnswer.toSet.toSeq.map((a: Any) => a.toString).sorted
      val actual = decoded.toSet.toSeq.map((a: Any) => a.toString).sorted

      val comparision = sideBySide("expected" +: expected, "spark" +: actual).mkString("\n")
      fail(
        s"""Decoded objects do not match expected objects:
            |$comparision
            |${ds.resolvedTEncoder.fromRowExpression.treeString}
         """.stripMargin)
    }
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  /**
   * Runs the plan and makes sure the answer matches the expected result.
   *
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }

<<<<<<< HEAD
    assertEmptyMissingInput(analyzedDF)
=======
    checkJsonFormat(analyzedDF)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    QueryTest.checkAnswer(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect())
  }

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   *
   * @param dataFrame the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param absTol the absolute tolerance between actual and expected answers.
   */
  protected def checkAggregatesWithTol(dataFrame: DataFrame,
      expectedAnswer: Seq[Row],
      absTol: Double): Unit = {
    // TODO: catch exceptions in data frame execution
    val actualAnswer = dataFrame.collect()
    require(actualAnswer.length == expectedAnswer.length,
      s"actual num rows ${actualAnswer.length} != expected num of rows ${expectedAnswer.length}")

    actualAnswer.zip(expectedAnswer).foreach {
      case (actualRow, expectedRow) =>
        QueryTest.checkAggregatesWithTol(actualRow, expectedRow, absTol)
    }
  }

  protected def checkAggregatesWithTol(dataFrame: DataFrame,
      expectedAnswer: Row,
      absTol: Double): Unit = {
    checkAggregatesWithTol(dataFrame, Seq(expectedAnswer), absTol)
  }

  /**
<<<<<<< HEAD
   * Asserts that a given [[Dataset]] will be executed using the given number of cached results.
   */
  def assertCached(query: Dataset[_], numCachedTables: Int = 1): Unit = {
=======
   * Asserts that a given [[Queryable]] will be executed using the given number of cached results.
   */
  def assertCached(query: Queryable, numCachedTables: Int = 1): Unit = {
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    val planWithCaching = query.queryExecution.withCachedData
    val cachedData = planWithCaching collect {
      case cached: InMemoryRelation => cached
    }

    assert(
      cachedData.size == numCachedTables,
      s"Expected query to contain $numCachedTables, but it actually had ${cachedData.size}\n" +
        planWithCaching)
  }

<<<<<<< HEAD
  /**
   * Asserts that a given [[Dataset]] does not have missing inputs in all the analyzed plans.
   */
  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    assert(query.queryExecution.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
    assert(query.queryExecution.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
    assert(query.queryExecution.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
=======
  private def checkJsonFormat(df: DataFrame): Unit = {
    val logicalPlan = df.queryExecution.analyzed
    // bypass some cases that we can't handle currently.
    logicalPlan.transform {
      case _: MapPartitions[_, _] => return
      case _: MapGroups[_, _, _] => return
      case _: AppendColumns[_, _] => return
      case _: CoGroup[_, _, _, _] => return
      case _: LogicalRelation => return
    }.transformAllExpressions {
      case _: ImperativeAggregate => return
      case _: UserDefinedGenerator => return
    }

    // bypass hive tests before we fix all corner cases in hive module.
    if (this.getClass.getName.startsWith("org.apache.spark.sql.hive")) return

    val jsonString = try {
      logicalPlan.toJSON
    } catch {
      case e =>
        fail(
          s"""
             |Failed to parse logical plan to JSON:
             |${logicalPlan.treeString}
           """.stripMargin, e)
    }

    // scala function is not serializable to JSON, use null to replace them so that we can compare
    // the plans later.
    val normalized1 = logicalPlan.transformAllExpressions {
      case udf: ScalaUDF => udf.copy(function = null)
      case gen: UserDefinedGenerator => gen.copy(function = null)
      // After SPARK-17356: the JSON representation no longer has the Metadata. We need to remove
      // the Metadata from the normalized plan so that we can compare this plan with the
      // JSON-deserialzed plan.
      case a @ Alias(child, name) if a.explicitMetadata.isDefined =>
        Alias(child, name)(a.exprId, a.qualifiers, Some(Metadata.empty))
      case a: AttributeReference if a.metadata != Metadata.empty =>
        AttributeReference(a.name, a.dataType, a.nullable, Metadata.empty)(a.exprId, a.qualifiers)
    }

    // RDDs/data are not serializable to JSON, so we need to collect LogicalPlans that contains
    // these non-serializable stuff, and use these original ones to replace the null-placeholders
    // in the logical plans parsed from JSON.
    var logicalRDDs = logicalPlan.collect { case l: LogicalRDD => l }
    var localRelations = logicalPlan.collect { case l: LocalRelation => l }
    var inMemoryRelations = logicalPlan.collect { case i: InMemoryRelation => i }

    val jsonBackPlan = try {
      TreeNode.fromJSON[LogicalPlan](jsonString, sqlContext.sparkContext)
    } catch {
      case e =>
        fail(
          s"""
             |Failed to rebuild the logical plan from JSON:
             |${logicalPlan.treeString}
             |
             |${logicalPlan.prettyJson}
           """.stripMargin, e)
    }

    val normalized2 = jsonBackPlan transformDown {
      case l: LogicalRDD =>
        val origin = logicalRDDs.head
        logicalRDDs = logicalRDDs.drop(1)
        LogicalRDD(l.output, origin.rdd)(sqlContext)
      case l: LocalRelation =>
        val origin = localRelations.head
        localRelations = localRelations.drop(1)
        l.copy(data = origin.data)
      case l: InMemoryRelation =>
        val origin = inMemoryRelations.head
        inMemoryRelations = inMemoryRelations.drop(1)
        InMemoryRelation(
          l.output,
          l.useCompression,
          l.batchSize,
          l.storageLevel,
          origin.child,
          l.tableName)(
          origin.cachedColumnBuffers,
          l._statistics,
          origin._batchStats)
    }

    assert(logicalRDDs.isEmpty)
    assert(localRelations.isEmpty)
    assert(inMemoryRelations.isEmpty)

    if (normalized1 != normalized2) {
      fail(
        s"""
           |== FAIL: the logical plan parsed from json does not match the original one ===
           |${sideBySide(logicalPlan.treeString, normalized2.treeString).mkString("\n")}
          """.stripMargin)
    }
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}

object QueryTest {
  /**
   * Runs the plan and makes sure the answer matches the expected result.
   * If there was exception during the execution or the contents of the DataFrame does not
   * match the expected result, an error message will be returned. Otherwise, a [[None]] will
   * be returned.
   *
   * @param df the [[DataFrame]] to be executed
   * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
   * @param checkToRDD whether to verify deserialization to an RDD. This runs the query twice.
   */
  def checkAnswer(
      df: DataFrame,
      expectedAnswer: Seq[Row],
      checkToRDD: Boolean = true): Option[String] = {
    val isSorted = df.logicalPlan.collect { case s: logical.Sort => s }.nonEmpty
    if (checkToRDD) {
      df.rdd.count()  // Also attempt to deserialize as an RDD [SPARK-15791]
    }

    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
            |Exception thrown while executing query:
            |${df.queryExecution}
            |== Exception ==
            |$e
            |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    sameRows(expectedAnswer, sparkAnswer, isSorted).map { results =>
        s"""
        |Results do not match for query:
        |Timezone: ${TimeZone.getDefault}
        |Timezone Env: ${sys.env.getOrElse("TZ", "")}
        |
        |${df.queryExecution}
        |== Results ==
        |$results
       """.stripMargin
    }
  }


  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case d: java.math.BigDecimal => BigDecimal(d)
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  def sameRows(
      expectedAnswer: Seq[Row],
      sparkAnswer: Seq[Row],
      isSorted: Boolean = false): Option[String] = {
    if (prepareAnswer(expectedAnswer, isSorted) != prepareAnswer(sparkAnswer, isSorted)) {
      val getRowType: Option[Row] => String = row =>
        row.map(row =>
            if (row.schema == null) {
              "struct<>"
            } else {
                s"${row.schema.catalogString}"
            }).getOrElse("struct<>")

      val errorMessage =
        s"""
         |== Results ==
         |${sideBySide(
        s"== Correct Answer - ${expectedAnswer.size} ==" +:
         getRowType(expectedAnswer.headOption) +:
         prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
        s"== Spark Answer - ${sparkAnswer.size} ==" +:
         getRowType(sparkAnswer.headOption) +:
         prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")}
        """.stripMargin
      return Some(errorMessage)
    }
    None
  }

  /**
   * Runs the plan and makes sure the answer is within absTol of the expected result.
   *
   * @param actualAnswer the actual result in a [[Row]].
   * @param expectedAnswer the expected result in a[[Row]].
   * @param absTol the absolute tolerance between actual and expected answers.
   */
  protected def checkAggregatesWithTol(actualAnswer: Row, expectedAnswer: Row, absTol: Double) = {
    require(actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")

    // TODO: support other numeric types besides Double
    // TODO: support struct types?
    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        assert(math.abs(actual - expected) < absTol,
          s"actual answer $actual not within $absTol of correct answer $expected")
      case (actual, expected) =>
        assert(actual == expected, s"$actual did not equal $expected")
    }
  }

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): String = {
    checkAnswer(df, expectedAnswer.asScala) match {
      case Some(errorMessage) => errorMessage
      case None => null
    }
  }
}

class QueryTestSuite extends QueryTest with test.SharedSQLContext {
  test("SPARK-16940: checkAnswer should raise TestFailedException for wrong results") {
    intercept[org.scalatest.exceptions.TestFailedException] {
      checkAnswer(sql("SELECT 1"), Row(2) :: Nil)
    }
  }
}
