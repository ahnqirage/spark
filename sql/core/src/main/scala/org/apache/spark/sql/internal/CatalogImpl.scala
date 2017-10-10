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

package org.apache.spark.sql.internal

import scala.reflect.runtime.universe.TypeTag
import scala.util.control.NonFatal

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql._
import org.apache.spark.sql.catalog.{Catalog, Column, Database, Function, Table}
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.execution.command.AlterTableRecoverPartitionsCommand
import org.apache.spark.sql.execution.datasources.{CreateTable, DataSource}
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel


/**
 * Internal implementation of the user-facing `Catalog`.
 */
class CatalogImpl(sparkSession: SparkSession) extends Catalog {

  private def sessionCatalog: SessionCatalog = sparkSession.sessionState.catalog

  private def requireDatabaseExists(dbName: String): Unit = {
    if (!sessionCatalog.databaseExists(dbName)) {
      throw new AnalysisException(s"Database '$dbName' does not exist.")
    }
  }

  private def requireTableExists(dbName: String, tableName: String): Unit = {
    if (!sessionCatalog.tableExists(TableIdentifier(tableName, Some(dbName)))) {
      throw new AnalysisException(s"Table '$tableName' does not exist in database '$dbName'.")
    }
  }

  /**
   * Returns the current default database in this session.
   */
  override def currentDatabase: String = sessionCatalog.getCurrentDatabase

  /**
   * Sets the current default database in this session.
   */
  @throws[AnalysisException]("database does not exist")
  override def setCurrentDatabase(dbName: String): Unit = {
    requireDatabaseExists(dbName)
    sessionCatalog.setCurrentDatabase(dbName)
  }

  /**
   * Returns a list of databases available across all sessions.
   */
  override def listDatabases(): Dataset[Database] = {
    val databases = sessionCatalog.listDatabases().map { dbName =>
      val metadata = sessionCatalog.getDatabaseMetadata(dbName)
      new Database(
        name = metadata.name,
        description = metadata.description,
        locationUri = metadata.locationUri)
    }
    CatalogImpl.makeDataset(databases, sparkSession)
  }

  /**
   * Returns a list of tables in the current database.
   * This includes all temporary tables.
   */
  override def listTables(): Dataset[Table] = {
    listTables(currentDatabase)
  }

  /**
   * Returns a list of tables in the specified database.
   * This includes all temporary tables.
   */
  @throws[AnalysisException]("database does not exist")
  override def listTables(dbName: String): Dataset[Table] = {
    val tables = sessionCatalog.listTables(dbName).map(makeTable)
    CatalogImpl.makeDataset(tables, sparkSession)
  }

  /**
   * Returns a Table for the given table/view or temporary view.
   *
   * Note that this function requires the table already exists in the Catalog.
   *
   * If the table metadata retrieval failed due to any reason (e.g., table serde class
   * is not accessible or the table type is not accepted by Spark SQL), this function
   * still returns the corresponding Table without the description and tableType)
   */
  private def makeTable(tableIdent: TableIdentifier): Table = {
    val metadata = try {
      Some(sessionCatalog.getTempViewOrPermanentTableMetadata(tableIdent))
    } catch {
      case NonFatal(_) => None
    }
    CatalogImpl.makeDataset(tables, sparkSession)
  }

  /**
   * Returns a list of functions registered in the current database.
   * This includes all temporary functions
   */
  override def listFunctions(): Dataset[Function] = {
    listFunctions(currentDatabase)
  }

  /**
   * Returns a list of functions registered in the specified database.
   * This includes all temporary functions
   */
  @throws[AnalysisException]("database does not exist")
  override def listFunctions(dbName: String): Dataset[Function] = {
    requireDatabaseExists(dbName)
    val functions = sessionCatalog.listFunctions(dbName).map { case (funcIdent, _) =>
      val metadata = sessionCatalog.lookupFunctionInfo(funcIdent)
      new Function(
        name = funcIdent.identifier,
        database = funcIdent.database.orNull,
        description = null, // for now, this is always undefined
        className = metadata.getClassName,
        isTemporary = funcIdent.database.isEmpty)
    }
    CatalogImpl.makeDataset(functions, sparkSession)
  }

  /**
   * Returns a list of columns for the given table/view or temporary view.
   */
  @throws[AnalysisException]("table does not exist")
  override def listColumns(tableName: String): Dataset[Column] = {
    listColumns(TableIdentifier(tableName, None))
  }

  /**
   * Returns a list of columns for the given table/view or temporary view in the specified database.
   */
  @throws[AnalysisException]("database or table does not exist")
  override def listColumns(dbName: String, tableName: String): Dataset[Column] = {
    requireTableExists(dbName, tableName)
    listColumns(TableIdentifier(tableName, Some(dbName)))
  }

  private def listColumns(tableIdentifier: TableIdentifier): Dataset[Column] = {
    val tableMetadata = sessionCatalog.getTempViewOrPermanentTableMetadata(tableIdentifier)
    val partitionColumnNames = tableMetadata.partitionColumnNames.toSet
    val bucketColumnNames = tableMetadata.bucketSpec.map(_.bucketColumnNames).getOrElse(Nil).toSet
    val columns = tableMetadata.schema.map { c =>
      new Column(
        name = c.name,
        description = c.getComment().orNull,
        dataType = c.dataType.catalogString,
        nullable = c.nullable,
        isPartition = partitionColumnNames.contains(c.name),
        isBucket = bucketColumnNames.contains(c.name))
    }
    CatalogImpl.makeDataset(columns, sparkSession)
  }

  /**
   * :: Experimental ::
   * Creates a table from the given path and returns the corresponding DataFrame.
   * It will use the default data source configured by spark.sql.sources.default.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  @Experimental
  override def createTable(tableName: String, path: String): DataFrame = {
    val dataSourceName = sparkSession.sessionState.conf.defaultDataSourceName
    createTable(tableName, path, dataSourceName)
  }

  /**
   * :: Experimental ::
   * Creates a table from the given path and returns the corresponding
   * DataFrame.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  @Experimental
  override def createTable(tableName: String, path: String, source: String): DataFrame = {
    createTable(tableName, source, Map("path" -> path))
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Creates a table based on the dataset in a data source and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  @Experimental
  override def createTable(
      tableName: String,
      source: String,
      options: Map[String, String]): DataFrame = {
    createTable(tableName, source, new StructType, options)
  }

  /**
   * :: Experimental ::
   * (Scala-specific)
   * Creates a table based on the dataset in a data source, a schema and a set of options.
   * Then, returns the corresponding DataFrame.
   *
   * @group ddl_ops
   * @since 2.2.0
   */
  @Experimental
  override def createTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    val tableIdent = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    val cmd =
      CreateTableUsing(
        tableIdent,
        userSpecifiedSchema = None,
        source,
        temporary = false,
        options = options,
        partitionColumns = Array.empty[String],
        bucketSpec = None,
        allowExisting = false,
        managedIfNoPath = false)
    sparkSession.sessionState.executePlan(cmd).toRdd
    sparkSession.table(tableIdent)
  }

  /**
   * Drops the local temporary view with the given view name in the catalog.
   * If the view has been cached/persisted before, it's also unpersisted.
   *
   * @param viewName the identifier of the temporary view to be dropped.
   * @group ddl_ops
   * @since 2.0.0
   */
  override def dropTempView(viewName: String): Boolean = {
    sparkSession.sessionState.catalog.getTempView(viewName).exists { viewDef =>
      sparkSession.sharedState.cacheManager.uncacheQuery(sparkSession, viewDef, blocking = true)
      sessionCatalog.dropTempView(viewName)
    }
  }

  /**
   * Drops the global temporary view with the given view name in the catalog.
   * If the view has been cached/persisted before, it's also unpersisted.
   *
   * @param viewName the identifier of the global temporary view to be dropped.
   * @group ddl_ops
   * @since 2.1.0
   */
  @Experimental
  override def createExternalTable(
      tableName: String,
      source: String,
      schema: StructType,
      options: Map[String, String]): DataFrame = {
    val tableIdent = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    val cmd =
      CreateTableUsing(
        tableIdent,
        userSpecifiedSchema = Some(schema),
        source,
        temporary = false,
        options,
        partitionColumns = Array.empty[String],
        bucketSpec = None,
        allowExisting = false,
        managedIfNoPath = false)
    sparkSession.sessionState.executePlan(cmd).toRdd
    sparkSession.table(tableIdent)
  }

  /**
   * Drops the temporary view with the given view name in the catalog.
   * If the view has been cached/persisted before, it's also unpersisted.
   *
   * @param viewName the name of the view to be dropped.
   * @group ddl_ops
   * @since 2.1.1
   */
  override def dropTempView(viewName: String): Unit = {
    sparkSession.sessionState.catalog.getTempView(viewName).foreach { tempView =>
      sparkSession.sharedState.cacheManager.uncacheQuery(Dataset.ofRows(sparkSession, tempView))
      sessionCatalog.dropTempView(viewName)
    }
  }

  /**
   * Returns true if the table or view is currently cached in-memory.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def isCached(tableName: String): Boolean = {
    sparkSession.sharedState.cacheManager.lookupCachedData(sparkSession.table(tableName)).nonEmpty
  }

  /**
   * Caches the specified table or view in-memory.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def cacheTable(tableName: String): Unit = {
    sparkSession.sharedState.cacheManager.cacheQuery(sparkSession.table(tableName), Some(tableName))
  }

  /**
   * Caches the specified table or view with the given storage level.
   *
   * @group cachemgmt
   * @since 2.3.0
   */
  override def cacheTable(tableName: String, storageLevel: StorageLevel): Unit = {
    sparkSession.sharedState.cacheManager.cacheQuery(
      sparkSession.table(tableName), Some(tableName), storageLevel)
  }

  /**
   * Removes the specified table or view from the in-memory cache.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def uncacheTable(tableName: String): Unit = {
    sparkSession.sharedState.cacheManager.uncacheQuery(query = sparkSession.table(tableName))
  }

  /**
   * Removes all cached tables or views from the in-memory cache.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def clearCache(): Unit = {
    sparkSession.sharedState.cacheManager.clearCache()
  }

  /**
   * Returns true if the [[Dataset]] is currently cached in-memory.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  protected[sql] def isCached(qName: Dataset[_]): Boolean = {
    sparkSession.sharedState.cacheManager.lookupCachedData(qName).nonEmpty
  }

  /**
   * Refresh the cache entry for a table, if any. For Hive metastore table, the metadata
   * is refreshed.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def refreshTable(tableName: String): Unit = {
    val tableIdent = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    sessionCatalog.refreshTable(tableIdent)

    // If this table is cached as an InMemoryRelation, drop the original
    // cached version and make the new version cached lazily.
    val logicalPlan = sparkSession.sessionState.catalog.lookupRelation(tableIdent)
    // Use lookupCachedData directly since RefreshTable also takes databaseName.
    val isCached = sparkSession.sharedState.cacheManager.lookupCachedData(logicalPlan).nonEmpty
    if (isCached) {
      // Create a data frame to represent the table.
      // TODO: Use uncacheTable once it supports database name.
      val df = Dataset.ofRows(sparkSession, logicalPlan)
      // Uncache the logicalPlan.
      sparkSession.sharedState.cacheManager.uncacheQuery(df, blocking = true)
      // Cache it again.
      sparkSession.sharedState.cacheManager.cacheQuery(df, Some(tableIdent.table))
    }
  }

  /**
   * Refresh the cache entry and the associated metadata for all dataframes (if any), that contain
   * the given data source path.
   *
   * @group cachemgmt
   * @since 2.0.0
   */
  override def refreshByPath(resourcePath: String): Unit = {
    sparkSession.sharedState.cacheManager.invalidateCachedPath(sparkSession, resourcePath)
  }
}


private[sql] object CatalogImpl {

  def makeDataset[T <: DefinedByConstructorParams: TypeTag](
      data: Seq[T],
      sparkSession: SparkSession): Dataset[T] = {
    val enc = ExpressionEncoder[T]()
    val encoded = data.map(d => enc.toRow(d).copy())
    val plan = new LocalRelation(enc.schema.toAttributes, encoded)
    val queryExecution = sparkSession.sessionState.executePlan(plan)
    new Dataset[T](sparkSession, queryExecution, enc)
  }

}
