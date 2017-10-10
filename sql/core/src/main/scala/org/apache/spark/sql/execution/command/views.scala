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

package org.apache.spark.sql.execution.command

import scala.collection.mutable

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.{SQLBuilder, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project}


/**
 * ViewType is used to specify the expected view type when we want to create or replace a view in
 * [[CreateViewCommand]].
 */
sealed trait ViewType {
  override def toString: String = getClass.getSimpleName.stripSuffix("$")
}

/**
 * LocalTempView means session-scoped local temporary views. Its lifetime is the lifetime of the
 * session that created it, i.e. it will be automatically dropped when the session terminates. It's
 * not tied to any databases, i.e. we can't use `db1.view1` to reference a local temporary view.
 */
object LocalTempView extends ViewType

/**
 * GlobalTempView means cross-session global temporary views. Its lifetime is the lifetime of the
 * Spark application, i.e. it will be automatically dropped when the application terminates. It's
 * tied to a system preserved database `global_temp`, and we must use the qualified name to refer a
 * global temp view, e.g. SELECT * FROM global_temp.view1.
 */
object GlobalTempView extends ViewType

/**
 * PersistedView means cross-session persisted views. Persisted views stay until they are
 * explicitly dropped by user command. It's always tied to a database, default to the current
 * database if not specified.
 *
 * Note that, Existing persisted view with the same name are not visible to the current session
 * while the local temporary view exists, unless the view name is qualified by database.
 */
object PersistedView extends ViewType


/**
 * Create or replace a view with given query plan. This command will generate some view-specific
 * properties(e.g. view default database, view query output column names) and store them as
 * properties in metastore, if we need to create a permanent view.
 *
 * @param name the name of this view.
 * @param userSpecifiedColumns the output column names and optional comments specified by users,
 *                             can be Nil if not specified.
 * @param comment the comment of this view.
 * @param properties the properties of this view.
 * @param originalText the original SQL text of this view, can be None if this view is created via
 *                     Dataset API.
 * @param child the logical plan that represents the view; this is used to generate the logical
 *              plan for temporary view and the view schema.
 * @param allowExisting if true, and if the view already exists, noop; if false, and if the view
 *                already exists, throws analysis exception.
 * @param replace if true, and if the view already exists, updates it; if false, and if the view
 *                already exists, throws analysis exception.
 * @param isTemporary if true, the view is created as a temporary view. Temporary views are dropped
 *                 at the end of current Spark session. Existing permanent relations with the same
 *                 name are not visible to the current session while the temporary view exists,
 *                 unless they are specified with full qualified table name with database prefix.
 */
case class CreateViewCommand(
    name: TableIdentifier,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: Option[String],
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    isTemporary: Boolean)
  extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(child)

  // TODO: Note that this class can NOT canonicalize the view SQL string entirely, which is
  // different from Hive and may not work for some cases like create view on self join.

  require(tableDesc.tableType == CatalogTableType.VIEW,
    "The type of the table to created with CREATE VIEW must be 'CatalogTableType.VIEW'.")
  if (!isTemporary) {
    require(tableDesc.viewText.isDefined,
      "The table to created with CREATE VIEW must have 'viewText'.")
  }

  if (allowExisting && replace) {
    throw new AnalysisException("CREATE VIEW with both IF NOT EXISTS and REPLACE is not allowed.")
  }

  // Disallows 'CREATE TEMPORARY VIEW IF NOT EXISTS' to be consistent with 'CREATE TEMPORARY TABLE'
  if (allowExisting && isTemporary) {
    throw new AnalysisException(
      "It is not allowed to define a TEMPORARY view with IF NOT EXISTS.")
  }

  // Temporary view names should NOT contain database prefix like "database.table"
  if (isTemporary && tableDesc.identifier.database.isDefined) {
    val database = tableDesc.identifier.database.get
    throw new AnalysisException(
      s"It is not allowed to add database prefix `$database` for the TEMPORARY view name.")
  }

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = sparkSession.sessionState.executePlan(child)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (tableDesc.schema != Nil && tableDesc.schema.length != analyzedPlan.output.length) {
      throw new AnalysisException(s"The number of columns produced by the SELECT clause " +
        s"(num: `${analyzedPlan.output.length}`) does not match the number of column names " +
        s"specified by CREATE VIEW (num: `${tableDesc.schema.length}`).")
    }
    val sessionState = sparkSession.sessionState

    if (isTemporary) {
      createTemporaryView(tableDesc.identifier, sparkSession, analyzedPlan)
    } else {
      // Adds default database for permanent table if it doesn't exist, so that tableExists()
      // only check permanent tables.
      val database = tableDesc.identifier.database.getOrElse(
        sessionState.catalog.getCurrentDatabase)
      val tableIdentifier = tableDesc.identifier.copy(database = Option(database))

      if (sessionState.catalog.tableExists(tableIdentifier)) {
        if (allowExisting) {
          // Handles `CREATE VIEW IF NOT EXISTS v0 AS SELECT ...`. Does nothing when the target view
          // already exists.
        } else if (replace) {
          // Handles `CREATE OR REPLACE VIEW v0 AS SELECT ...`
          sessionState.catalog.alterTable(prepareTable(sparkSession, analyzedPlan))
        } else {
          // Handles `CREATE VIEW v0 AS SELECT ...`. Throws exception when the target view already
          // exists.
          throw new AnalysisException(
            s"View $tableIdentifier already exists. If you want to update the view definition, " +
              "please use ALTER VIEW AS or CREATE OR REPLACE VIEW AS")
        }
      } else {
        // Create the view if it doesn't exist.
        sessionState.catalog.createTable(
          prepareTable(sparkSession, analyzedPlan), ignoreIfExists = false)
      }
    }
    Seq.empty[Row]
  }

  private def createTemporaryView(
      table: TableIdentifier, sparkSession: SparkSession, analyzedPlan: LogicalPlan): Unit = {

    val sessionState = sparkSession.sessionState
    val catalog = sessionState.catalog

    // Projects column names to alias names
    val logicalPlan = {
      if (tableDesc.schema.isEmpty) {
        analyzedPlan
      } else {
        val projectList = analyzedPlan.output.zip(tableDesc.schema).map {
          case (attr, col) => Alias(attr, col.name)()
        }
        sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
      }
    }

    catalog.createTempView(table.table, logicalPlan, replace)
  }

  /**
   * Permanent views are not allowed to reference temp objects, including temp function and views
   */
  private def prepareTable(sparkSession: SparkSession, analyzedPlan: LogicalPlan): CatalogTable = {
    val viewSQL: String =
      if (sparkSession.sessionState.conf.canonicalView) {
        val logicalPlan =
          if (tableDesc.schema.isEmpty) {
            analyzedPlan
          } else {
            val projectList = analyzedPlan.output.zip(tableDesc.schema).map {
              case (attr, col) => Alias(attr, col.name)()
            }
            sparkSession.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
          }
        new SQLBuilder(logicalPlan).toSQL
      } else {
        // When user specified column names for view, we should create a project to do the renaming.
        // When no column name specified, we still need to create a project to declare the columns
        // we need, to make us more robust to top level `*`s.
        val viewOutput = {
          val columnNames = analyzedPlan.output.map(f => quote(f.name))
          if (tableDesc.schema.isEmpty) {
            columnNames.mkString(", ")
          } else {
            columnNames.zip(tableDesc.schema.map(f => quote(f.name))).map {
              case (name, alias) => s"$name AS $alias"
            }.mkString(", ")
          }
        }

  /**
   * If `userSpecifiedColumns` is defined, alias the analyzed plan to the user specified columns,
   * else return the analyzed plan directly.
   */
  private def aliasPlan(session: SparkSession, analyzedPlan: LogicalPlan): LogicalPlan = {
    if (userSpecifiedColumns.isEmpty) {
      analyzedPlan
    } else {
      val projectList = analyzedPlan.output.zip(userSpecifiedColumns).map {
        case (attr, (colName, None)) => Alias(attr, colName)()
        case (attr, (colName, Some(colComment))) =>
          val meta = new MetadataBuilder().putString("comment", colComment).build()
          Alias(attr, colName)(explicitMetadata = Some(meta))
      }
      session.sessionState.executePlan(Project(projectList, analyzedPlan)).analyzed
    }
  }

    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      sparkSession.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }

    val newProperties = generateViewProperties(properties, session, analyzedPlan)

    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = aliasPlan(session, analyzedPlan).schema,
      properties = newProperties,
      viewText = originalText,
      comment = comment
    )
  }
}

/**
 * Alter a view with given query plan. If the view name contains database prefix, this command will
 * alter a permanent view matching the given name, or throw an exception if view not exist. Else,
 * this command will try to alter a temporary view first, if view not exist, try permanent view
 * next, if still not exist, throw an exception.
 *
 * @param name the name of this view.
 * @param originalText the original SQL text of this view. Note that we can only alter a view by
 *                     SQL API, which means we always have originalText.
 * @param query the logical plan that represents the view; this is used to generate the new view
 *              schema.
 */
case class AlterViewAsCommand(
    name: TableIdentifier,
    originalText: String,
    query: LogicalPlan) extends RunnableCommand {

  import ViewHelper._

  override def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(session: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = session.sessionState.executePlan(query)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (session.sessionState.catalog.alterTempViewDefinition(name, analyzedPlan)) {
      // a local/global temp view has been altered, we are done.
    } else {
      alterPermanentView(session, analyzedPlan)
    }

    Seq.empty[Row]
  }

  private def alterPermanentView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val viewMeta = session.sessionState.catalog.getTableMetadata(name)
    if (viewMeta.tableType != CatalogTableType.VIEW) {
      throw new AnalysisException(s"${viewMeta.identifier} is not a view.")
    }

    // Detect cyclic view reference on ALTER VIEW.
    val viewIdent = viewMeta.identifier
    checkCyclicViewReference(analyzedPlan, Seq(viewIdent), viewIdent)

    val newProperties = generateViewProperties(viewMeta.properties, session, analyzedPlan)

    val updatedViewMeta = viewMeta.copy(
      schema = analyzedPlan.schema,
      properties = newProperties,
      viewText = Some(originalText))

    session.sessionState.catalog.alterTable(updatedViewMeta)
  }
}

object ViewHelper {

  import CatalogTable._

  /**
   * Generate the view default database in `properties`.
   */
  private def generateViewDefaultDatabase(databaseName: String): Map[String, String] = {
    Map(VIEW_DEFAULT_DATABASE -> databaseName)
  }

  /**
   * Generate the view query output column names in `properties`.
   */
  private def generateQueryColumnNames(columns: Seq[String]): Map[String, String] = {
    val props = new mutable.HashMap[String, String]
    if (columns.nonEmpty) {
      props.put(VIEW_QUERY_OUTPUT_NUM_COLUMNS, columns.length.toString)
      columns.zipWithIndex.foreach { case (colName, index) =>
        props.put(s"$VIEW_QUERY_OUTPUT_COLUMN_NAME_PREFIX$index", colName)
      }
    }
    props.toMap
  }

  /**
   * Remove the view query output column names in `properties`.
   */
  private def removeQueryColumnNames(properties: Map[String, String]): Map[String, String] = {
    // We can't use `filterKeys` here, as the map returned by `filterKeys` is not serializable,
    // while `CatalogTable` should be serializable.
    properties.filterNot { case (key, _) =>
      key.startsWith(VIEW_QUERY_OUTPUT_PREFIX)
    }
  }

  /**
   * Generate the view properties in CatalogTable, including:
   * 1. view default database that is used to provide the default database name on view resolution.
   * 2. the output column names of the query that creates a view, this is used to map the output of
   *    the view child to the view output during view resolution.
   *
   * @param properties the `properties` in CatalogTable.
   * @param session the spark session.
   * @param analyzedPlan the analyzed logical plan that represents the child of a view.
   * @return new view properties including view default database and query column names properties.
   */
  def generateViewProperties(
      properties: Map[String, String],
      session: SparkSession,
      analyzedPlan: LogicalPlan): Map[String, String] = {
    val queryOutput = analyzedPlan.schema.fieldNames

    // Generate the query column names, throw an AnalysisException if there exists duplicate column
    // names.
    SchemaUtils.checkColumnNameDuplication(
      queryOutput, "in the view definition", session.sessionState.conf.resolver)

    // Generate the view default database name.
    val viewDefaultDatabase = session.sessionState.catalog.getCurrentDatabase
    removeQueryColumnNames(properties) ++
      generateViewDefaultDatabase(viewDefaultDatabase) ++
      generateQueryColumnNames(queryOutput)
  }

  /**
   * Recursively search the logical plan to detect cyclic view references, throw an
   * AnalysisException if cycle detected.
   *
   * A cyclic view reference is a cycle of reference dependencies, for example, if the following
   * statements are executed:
   * CREATE VIEW testView AS SELECT id FROM tbl
   * CREATE VIEW testView2 AS SELECT id FROM testView
   * ALTER VIEW testView AS SELECT * FROM testView2
   * The view `testView` references `testView2`, and `testView2` also references `testView`,
   * therefore a reference cycle (testView -> testView2 -> testView) exists.
   *
   * @param plan the logical plan we detect cyclic view references from.
   * @param path the path between the altered view and current node.
   * @param viewIdent the table identifier of the altered view, we compare two views by the
   *                  `desc.identifier`.
   */
  def checkCyclicViewReference(
      plan: LogicalPlan,
      path: Seq[TableIdentifier],
      viewIdent: TableIdentifier): Unit = {
    plan match {
      case v: View =>
        val ident = v.desc.identifier
        val newPath = path :+ ident
        // If the table identifier equals to the `viewIdent`, current view node is the same with
        // the altered view. We detect a view reference cycle, should throw an AnalysisException.
        if (ident == viewIdent) {
          throw new AnalysisException(s"Recursive view $viewIdent detected " +
            s"(cycle: ${newPath.mkString(" -> ")})")
        } else {
          v.children.foreach { child =>
            checkCyclicViewReference(child, newPath, viewIdent)
          }
        }
      case _ =>
        plan.children.foreach(child => checkCyclicViewReference(child, path, viewIdent))
    }

    // Detect cyclic references from subqueries.
    plan.expressions.foreach { expr =>
      expr match {
        case s: SubqueryExpression =>
          checkCyclicViewReference(s.plan, path, viewIdent)
        case _ => // Do nothing.
      }
    }
  }
}

/**
 * Alter a view with given query plan. If the view name contains database prefix, this command will
 * alter a permanent view matching the given name, or throw an exception if view not exist. Else,
 * this command will try to alter a temporary view first, if view not exist, try permanent view
 * next, if still not exist, throw an exception.
 *
 * @param tableDesc the catalog table
 * @param query the logical plan that represents the view; this is used to generate a canonicalized
 *              version of the SQL that can be saved in the catalog.
 */
case class AlterViewAsCommand(
    tableDesc: CatalogTable,
    query: LogicalPlan) extends RunnableCommand {

  override protected def innerChildren: Seq[QueryPlan[_]] = Seq(query)

  override def run(session: SparkSession): Seq[Row] = {
    // If the plan cannot be analyzed, throw an exception and don't proceed.
    val qe = session.sessionState.executePlan(query)
    qe.assertAnalyzed()
    val analyzedPlan = qe.analyzed

    if (session.sessionState.catalog.isTemporaryTable(tableDesc.identifier)) {
      session.sessionState.catalog.createTempView(
        tableDesc.identifier.table,
        analyzedPlan,
        overrideIfExists = true)
    } else {
      alterPermanentView(session, analyzedPlan)
    }

    Seq.empty[Row]
  }

  private def alterPermanentView(session: SparkSession, analyzedPlan: LogicalPlan): Unit = {
    val viewMeta = session.sessionState.catalog.getTableMetadata(tableDesc.identifier)
    if (viewMeta.tableType != CatalogTableType.VIEW) {
      throw new AnalysisException(s"${viewMeta.identifier} is not a view.")
    }

    val viewSQL: String = new SQLBuilder(analyzedPlan).toSQL
    // Validate the view SQL - make sure we can parse it and analyze it.
    // If we cannot analyze the generated query, there is probably a bug in SQL generation.
    try {
      session.sql(viewSQL).queryExecution.assertAnalyzed()
    } catch {
      case NonFatal(e) =>
        throw new RuntimeException(s"Failed to analyze the canonicalized SQL: $viewSQL", e)
    }

    val viewSchema: Seq[CatalogColumn] = {
      analyzedPlan.output.map { a =>
        CatalogColumn(a.name, a.dataType.catalogString)
      }
    }

    val updatedViewMeta = viewMeta.copy(
      schema = viewSchema,
      viewOriginalText = tableDesc.viewOriginalText,
      viewText = Some(viewSQL))

    session.sessionState.catalog.alterTable(updatedViewMeta)
  }
}
