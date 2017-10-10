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

package org.apache.spark.sql.execution.datasources

import scala.util.control.NonFatal

import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{CatalogRelation, SessionCatalog}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Cast, RowOrdering}
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.command.CreateHiveTableAsSelectLogicalPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.types.{AtomicType, StructType}
import org.apache.spark.sql.util.SchemaUtils

/**
 * Try to replaces [[UnresolvedRelation]]s with [[ResolveDataSource]].
 */
class ResolveDataSource(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case u: UnresolvedRelation if maybeSQLFile(u) =>
      try {
        val dataSource = DataSource(
          sparkSession,
          paths = u.tableIdentifier.table :: Nil,
          className = u.tableIdentifier.database.get)

        val notSupportDirectQuery = try {
          !classOf[FileFormat].isAssignableFrom(dataSource.providingClass)
        } catch {
          case NonFatal(e) => false
        }
        if (notSupportDirectQuery) {
          throw new AnalysisException("Unsupported data source type for direct query on files: " +
            s"${u.tableIdentifier.database.get}")
        }
        val plan = LogicalRelation(dataSource.resolveRelation())
        u.alias.map(a => SubqueryAlias(u.alias.get, plan)).getOrElse(plan)
      } catch {
        case _: ClassNotFoundException => u
        case e: Exception =>
          // the provider is valid, but failed to create a logical plan
          u.failAnalysis(e.getMessage)
      }
  }
}

/**
 * Analyze the query in CREATE TABLE AS SELECT (CTAS). After analysis, [[PreWriteCheck]] also
 * can detect the cases that are not allowed.
 */
case class AnalyzeCreateTableAsSelect(sparkSession: SparkSession) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case c: CreateTableUsingAsSelect if !c.query.resolved =>
      c.copy(query = analyzeQuery(c.query))
    case c: CreateHiveTableAsSelectLogicalPlan if !c.query.resolved =>
      c.copy(query = analyzeQuery(c.query))
  }

  private def analyzeQuery(query: LogicalPlan): LogicalPlan = {
    val qe = sparkSession.sessionState.executePlan(query)
    qe.assertAnalyzed()
    qe.analyzed
  }
}

/**
 * Preprocess the [[InsertIntoTable]] plan. Throws exception if the number of columns mismatch, or
 * specified partition columns are different from the existing partition columns in the target
 * table. It also does data type casting and field renaming, to make sure that the columns to be
 * inserted have the correct data type and fields have the correct names.
 */
case class PreprocessTableInsertion(conf: SQLConf) extends Rule[LogicalPlan] {
  private def preprocess(
      insert: InsertIntoTable,
      tblName: String,
      partColNames: Seq[String]): InsertIntoTable = {

    val expectedColumns = insert.expectedColumns
    if (expectedColumns.isDefined && expectedColumns.get.length != insert.child.schema.length) {
      throw new AnalysisException(
        s"Cannot insert into table $tblName because the number of columns are different: " +
          s"need ${expectedColumns.get.length} columns, " +
          s"but query has ${insert.child.schema.length} columns.")
    }

    if (insert.partition.nonEmpty) {
      // the query's partitioning must match the table's partitioning
      // this is set for queries like: insert into ... partition (one = "a", two = <expr>)
      val samePartitionColumns =
        if (conf.caseSensitiveAnalysis) {
          insert.partition.keySet == partColNames.toSet
        } else {
          insert.partition.keySet.map(_.toLowerCase) == partColNames.map(_.toLowerCase).toSet
        }
      if (!samePartitionColumns) {
        throw new AnalysisException(
          s"""
             |Requested partitioning does not match the table $tblName:
             |Requested partitions: ${insert.partition.keys.mkString(",")}
             |Table partitions: ${partColNames.mkString(",")}
           """.stripMargin)
      }
      expectedColumns.map(castAndRenameChildOutput(insert, _)).getOrElse(insert)
    } else {
      // All partition columns are dynamic because because the InsertIntoTable command does
      // not explicitly specify partitioning columns.
      expectedColumns.map(castAndRenameChildOutput(insert, _)).getOrElse(insert)
        .copy(partition = partColNames.map(_ -> None).toMap)
    }
  }

  def castAndRenameChildOutput(
      insert: InsertIntoTable,
      expectedOutput: Seq[Attribute]): InsertIntoTable = {
    val newChildOutput = expectedOutput.zip(insert.child.output).map {
      case (expected, actual) =>
        if (expected.dataType.sameType(actual.dataType) &&
          expected.name == actual.name &&
          expected.metadata == actual.metadata) {
          actual
        } else {
          // Renaming is needed for handling the following cases like
          // 1) Column names/types do not match, e.g., INSERT INTO TABLE tab1 SELECT 1, 2
          // 2) Target tables have column metadata
          Alias(Cast(actual, expected.dataType), expected.name)(
            explicitMetadata = Option(expected.metadata))
        }
    }

    if (newChildOutput == insert.child.output) {
      insert
    } else {
      insert.copy(child = Project(newChildOutput, insert.child))
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case i @ InsertIntoTable(table, partition, child, _, _) if table.resolved && child.resolved =>
      table match {
        case relation: CatalogRelation =>
          val metadata = relation.catalogTable
          preprocess(i, metadata.identifier.quotedString, metadata.partitionColumnNames)
        case LogicalRelation(h: HadoopFsRelation, _, identifier) =>
          val tblName = identifier.map(_.quotedString).getOrElse("unknown")
          preprocess(i, tblName, h.partitionSchema.map(_.name))
        case LogicalRelation(_: InsertableRelation, _, identifier) =>
          val tblName = identifier.map(_.quotedString).getOrElse("unknown")
          preprocess(i, tblName, Nil)
        case other => i
      }
  }
}

/**
 * A rule to check whether the functions are supported only when Hive support is enabled
 */
case class PreWriteCheck(conf: SQLConf, catalog: SessionCatalog)
  extends (LogicalPlan => Unit) {


/**
 * A rule to do various checks before reading a table.
 */
object PreReadCheck extends (LogicalPlan => Unit) {
  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case operator: LogicalPlan =>
        operator transformExpressionsUp {
          case e @ (_: InputFileName | _: InputFileBlockLength | _: InputFileBlockStart) =>
            checkNumInputFileBlockSources(e, operator)
            e
        }
    }
  }

  private def checkNumInputFileBlockSources(e: Expression, operator: LogicalPlan): Int = {
    operator match {
      case _: HiveTableRelation => 1
      case _ @ LogicalRelation(_: HadoopFsRelation, _, _, _) => 1
      case _: LeafNode => 0
      // UNION ALL has multiple children, but these children do not concurrently use InputFileBlock.
      case u: Union =>
        if (u.children.map(checkNumInputFileBlockSources(e, _)).sum >= 1) 1 else 0
      case o =>
        val numInputFileBlockSources = o.children.map(checkNumInputFileBlockSources(e, _)).sum
        if (numInputFileBlockSources > 1) {
          e.failAnalysis(s"'${e.prettyName}' does not support more than one sources")
        } else {
          numInputFileBlockSources
        }
    }
  }
}

        PartitioningUtils.validatePartitionColumn(
          r.schema, part.keySet.toSeq, conf.caseSensitiveAnalysis)

/**
 * A rule to do various checks before inserting into or writing to a data source table.
 */
object PreWriteCheck extends (LogicalPlan => Unit) {

  def failAnalysis(msg: String): Unit = { throw new AnalysisException(msg) }

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case InsertIntoTable(l @ LogicalRelation(relation, _, _, _), partition, query, _, _) =>
        // Get all input data source relations of the query.
        val srcRelations = query.collect {
          case LogicalRelation(src, _, _, _) => src
        }
        if (srcRelations.contains(relation)) {
          failAnalysis("Cannot insert into table that is also being read from.")
        } else {
          // OK
        }

        relation match {
          case _: HadoopFsRelation => // OK

      case c: CreateTableUsingAsSelect =>
        // When the SaveMode is Overwrite, we need to check if the table is an input table of
        // the query. If so, we will throw an AnalysisException to let users know it is not allowed.
        if (c.mode == SaveMode.Overwrite && catalog.tableExists(c.tableIdent)) {
          // Need to remove SubQuery operator.
          EliminateSubqueryAliases(catalog.lookupRelation(c.tableIdent)) match {
            // Only do the check if the table is a data source table
            // (the relation is a BaseRelation).
            case l @ LogicalRelation(dest: BaseRelation, _, _) =>
              // Get all input data source relations of the query.
              val srcRelations = c.query.collect {
                case LogicalRelation(src: BaseRelation, _, _) => src
              }
              if (srcRelations.contains(dest)) {
                failAnalysis(
                  s"Cannot overwrite table ${c.tableIdent} that is also being read from.")
              } else {
                // OK
              }

            case _ => // OK
          }
        } else {
          // OK
        }

        PartitioningUtils.validatePartitionColumn(
          c.query.schema, c.partitionColumns, conf.caseSensitiveAnalysis)

        for {
          spec <- c.bucketSpec
          sortColumnName <- spec.sortColumnNames
          sortColumn <- c.query.schema.find(_.name == sortColumnName)
        } {
          if (!RowOrdering.isOrderable(sortColumn.dataType)) {
            failAnalysis(s"Cannot use ${sortColumn.dataType.simpleString} for sorting column.")
          }
        }

      case _ => // OK
    }
  }
}
