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

package org.apache.spark.sql.hive

import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import com.google.common.util.concurrent.Striped
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.execution.command.CreateDataSourceTableUtils._
import org.apache.spark.sql.execution.command.CreateHiveTableAsSelectLogicalPlan
import org.apache.spark.sql.execution.datasources.{Partition => _, _}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFileFormat, ParquetOptions}
import org.apache.spark.sql.hive.orc.OrcFileFormat
import org.apache.spark.sql.types._

/**
 * Legacy catalog for interacting with the Hive metastore.
 *
 * This is still used for things like creating data source tables, but in the future will be
 * cleaned up to integrate more nicely with [[HiveExternalCatalog]].
 */
private[hive] class HiveMetastoreCatalog(sparkSession: SparkSession) extends Logging {
  private val sessionState = sparkSession.sessionState.asInstanceOf[HiveSessionState]
  private val client = sparkSession.sharedState.asInstanceOf[HiveSharedState].metadataHive

  /** A fully qualified identifier for a table (i.e., database.tableName) */
  case class QualifiedTableName(database: String, name: String)

  private def getCurrentDatabase: String = sessionState.catalog.getCurrentDatabase

  def getQualifiedTableName(tableIdent: TableIdentifier): QualifiedTableName = {
    QualifiedTableName(
      tableIdent.database.getOrElse(getCurrentDatabase).toLowerCase,
      tableIdent.table.toLowerCase)
  }

  private def getQualifiedTableName(t: CatalogTable): QualifiedTableName = {
    QualifiedTableName(
      t.identifier.database.getOrElse(getCurrentDatabase).toLowerCase,
      t.identifier.table.toLowerCase)
  }

  /** These locks guard against multiple attempts to instantiate a table, which wastes memory. */
  private val tableCreationLocks = Striped.lazyWeakLock(100)

  /** Acquires a lock on the table cache for the duration of `f`. */
  private def withTableCreationLock[A](tableName: QualifiedTableName, f: => A): A = {
    val lock = tableCreationLocks.get(tableName)
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table = client.getTable(in.database, in.name)

        // TODO: the following code is duplicated with FindDataSourceTable.readDataSourceTable

        def schemaStringFromParts: Option[String] = {
          table.properties.get(DATASOURCE_SCHEMA_NUMPARTS).map { numParts =>
            val parts = (0 until numParts.toInt).map { index =>
              val part = table.properties.get(s"$DATASOURCE_SCHEMA_PART_PREFIX$index").orNull
              if (part == null) {
                throw new AnalysisException(
                  "Could not read schema from the metastore because it is corrupted " +
                    s"(missing part $index of the schema, $numParts parts are expected).")
              }

              part
            }
            // Stick all parts back to a single schema string.
            parts.mkString
          }
        }

        def getColumnNames(colType: String): Seq[String] = {
          table.properties.get(s"$DATASOURCE_SCHEMA.num${colType.capitalize}Cols").map {
            numCols => (0 until numCols.toInt).map { index =>
              table.properties.getOrElse(s"$DATASOURCE_SCHEMA_PREFIX${colType}Col.$index",
                throw new AnalysisException(
                  s"Could not read $colType columns from the metastore because it is corrupted " +
                    s"(missing part $index of it, $numCols parts are expected)."))
            }
          }.getOrElse(Nil)
        }

        // Originally, we used spark.sql.sources.schema to store the schema of a data source table.
        // After SPARK-6024, we removed this flag.
        // Although we are not using spark.sql.sources.schema any more, we need to still support.
        val schemaString = table.properties.get(DATASOURCE_SCHEMA).orElse(schemaStringFromParts)

        val userSpecifiedSchema =
          schemaString.map(s => DataType.fromJson(s).asInstanceOf[StructType])

        // We only need names at here since userSpecifiedSchema we loaded from the metastore
        // contains partition columns. We can always get data types of partitioning columns
        // from userSpecifiedSchema.
        val partitionColumns = getColumnNames("part")

        val bucketSpec = table.properties.get(DATASOURCE_SCHEMA_NUMBUCKETS).map { n =>
          BucketSpec(n.toInt, getColumnNames("bucket"), getColumnNames("sort"))
        }

        val options = table.storage.serdeProperties
        val dataSource =
          DataSource(
            sparkSession,
            userSpecifiedSchema = userSpecifiedSchema,
            partitionColumns = partitionColumns,
            bucketSpec = bucketSpec,
            className = table.properties(DATASOURCE_PROVIDER),
            options = options)

        LogicalRelation(
          dataSource.resolveRelation(checkPathExist = true),
          metastoreTableIdentifier = Some(TableIdentifier(in.name, Some(in.database))))
      }
    }
  }

  def refreshTable(tableIdent: TableIdentifier): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidate the cache.
    // Next time when we use the table, it will be populated in the cache.
    // Since we also cache ParquetRelations converted from Hive Parquet tables and
    // adding converted ParquetRelations into the cache is not defined in the load function
    // of the cache (instead, we add the cache entry in convertToParquetRelation),
    // it is better at here to invalidate the cache to avoid confusing waring logs from the
    // cache loader (e.g. cannot find data source provider, which is only defined for
    // data source table.).
    cachedDataSourceTables.invalidate(getQualifiedTableName(tableIdent))
  }

  def hiveDefaultTableFilePath(tableIdent: TableIdentifier): String = {
    // Code based on: hiveWarehouse.getTablePath(currentDatabase, tableName)
    val QualifiedTableName(dbName, tblName) = getQualifiedTableName(tableIdent)
    new Path(new Path(client.getDatabase(dbName).locationUri), tblName).toString
  }

  def lookupRelation(
      tableIdent: TableIdentifier,
      alias: Option[String]): LogicalPlan = {
    val qualifiedTableName = getQualifiedTableName(tableIdent)
    val table = client.getTable(qualifiedTableName.database, qualifiedTableName.name)

    if (table.properties.get(DATASOURCE_PROVIDER).isDefined) {
      val dataSourceTable = cachedDataSourceTables(qualifiedTableName)
      val qualifiedTable = SubqueryAlias(qualifiedTableName.name, dataSourceTable)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Otherwise, wrap the table with a Subquery using the table name.
      alias.map(a => SubqueryAlias(a, qualifiedTable)).getOrElse(qualifiedTable)
    } else if (table.tableType == CatalogTableType.VIEW) {
      val viewText = table.viewText.getOrElse(sys.error("Invalid view without text."))
      alias match {
        // because hive use things like `_c0` to build the expanded text
        // currently we cannot support view from "create view v1(c1) as ..."
        case None =>
          SubqueryAlias(table.identifier.table,
            sparkSession.sessionState.sqlParser.parsePlan(viewText))
        case Some(aliasText) =>
          SubqueryAlias(aliasText, sessionState.sqlParser.parsePlan(viewText))
      }
    } else {
      MetastoreRelation(
        qualifiedTableName.database, qualifiedTableName.name, alias)(table, client, sparkSession)
    }
  }

  private def getCached(
      tableIdentifier: QualifiedTableName,
      pathsInMetastore: Seq[String],
      metastoreRelation: MetastoreRelation,
      schemaInMetastore: StructType,
      expectedFileFormat: Class[_ <: FileFormat],
      partitionSchema: Option[StructType]): Option[LogicalRelation] = {

    catalogProxy.getCachedTable(tableIdentifier) match {
      case null => None // Cache miss
      case logical @ LogicalRelation(relation: HadoopFsRelation, _, _) =>
        val cachedRelationFileFormatClass = relation.fileFormat.getClass

        expectedFileFormat match {
          case `cachedRelationFileFormatClass` =>
            // If we have the same paths, same schema, and same partition spec,
            // we will use the cached relation.
            val useCached =
              relation.location.rootPaths.toSet == pathsInMetastore.toSet &&
                logical.schema.sameType(schemaInMetastore) &&
                // We don't support hive bucketed tables. This function `getCached` is only used for
                // converting supported Hive tables to data source tables.
                relation.bucketSpec.isEmpty &&
                relation.partitionSchema == partitionSchema.getOrElse(StructType(Nil))

            if (useCached) {
              Some(logical)
            } else {
              // If the cached relation is not updated, we invalidate it right away.
              catalogProxy.invalidateCachedTable(tableIdentifier)
              None
            }
          case _ =>
            logWarning(s"Table $tableIdentifier should be stored as $expectedFileFormat. " +
              s"However, we are getting a ${relation.fileFormat} from the metastore cache. " +
              "This cached entry will be invalidated.")
            catalogProxy.invalidateCachedTable(tableIdentifier)
            None
        }
      case other =>
        logWarning(s"Table $tableIdentifier should be stored as $expectedFileFormat. " +
          s"However, we are getting a $other from the metastore cache. " +
          "This cached entry will be invalidated.")
        catalogProxy.invalidateCachedTable(tableIdentifier)
        None
    }
  }

  def convertToLogicalRelation(
      relation: HiveTableRelation,
      options: Map[String, String],
      fileFormatClass: Class[_ <: FileFormat],
      fileType: String): LogicalRelation = {
    val metastoreSchema = relation.tableMeta.schema
    val tableIdentifier =
      QualifiedTableName(relation.tableMeta.database, relation.tableMeta.identifier.table)

    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      // We're converting the entire table into HadoopFsRelation, so predicates to Hive metastore
      // are empty.
      val partitions = metastoreRelation.getHiveQlPartitions().map { p =>
        val location = p.getLocation
        val values = InternalRow.fromSeq(p.getValues.asScala.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        PartitionDirectory(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      val partitionPaths = partitions.map(_.path.toString)

      // By convention (for example, see MetaStorePartitionedTableFileCatalog), the definition of a
      // partitioned table's paths depends on whether that table has any actual partitions.
      // Partitioned tables without partitions use the location of the table's base path.
      // Partitioned tables with partitions use the locations of those partitions' data locations,
      // _omitting_ the table's base path.
      val paths = if (partitionPaths.isEmpty) {
        Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)
      } else {
        partitionPaths
      }

      withTableCreationLock(tableIdentifier, {
        val cached = getCached(
          tableIdentifier,
          paths,
          metastoreRelation,
          metastoreSchema,
          fileFormatClass,
          bucketSpec,
          Some(partitionSpec))

        val hadoopFsRelation = cached.getOrElse {
          val fileCatalog = new MetaStorePartitionedTableFileCatalog(
            sparkSession,
            new Path(metastoreRelation.catalogTable.storage.locationUri.get),
            partitionSpec)

          val inferredSchema = if (fileType.equals("parquet")) {
            val inferredSchema =
              defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles())
            inferredSchema.map { inferred =>
              ParquetFileFormat.mergeMetastoreParquetSchema(metastoreSchema, inferred)
            }.getOrElse(metastoreSchema)
          } else {
            defaultSource.inferSchema(sparkSession, options, fileCatalog.allFiles()).get
          }

          val relation = HadoopFsRelation(
            location = fileCatalog,
            partitionSchema = partitionSchema,
            dataSchema = inferredSchema,
            bucketSpec = bucketSpec,
            fileFormat = defaultSource,
            options = options)(sparkSession = sparkSession)

          val created = LogicalRelation(
            relation,
            metastoreTableIdentifier =
              Some(TableIdentifier(tableIdentifier.name, Some(tableIdentifier.database))))
          cachedDataSourceTables.put(tableIdentifier, created)
          created
        }

        hadoopFsRelation
      })
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)

      withTableCreationLock(tableIdentifier, {
        val cached = getCached(tableIdentifier,
          paths,
          metastoreRelation,
          metastoreSchema,
          fileFormatClass,
          bucketSpec,
          None)
        val logicalRelation = cached.getOrElse {
          val created =
            LogicalRelation(
              DataSource(
                sparkSession = sparkSession,
                paths = paths,
                userSpecifiedSchema = Some(metastoreRelation.schema),
                bucketSpec = bucketSpec,
                options = options,
                className = fileType).resolveRelation(),
                metastoreTableIdentifier =
                  Some(TableIdentifier(tableIdentifier.name, Some(tableIdentifier.database))))


          cachedDataSourceTables.put(tableIdentifier, created)
          created
        }

        logicalRelation
      })
    }
    result.copy(expectedOutputAttributes = Some(metastoreRelation.output))
  }

          val (dataSchema, updatedTable) =
            inferIfNeeded(relation, options, fileFormat, Option(fileIndex))

    private def convertToParquetRelation(relation: MetastoreRelation): LogicalRelation = {
      val defaultSource = new ParquetFileFormat()
      val fileFormatClass = classOf[ParquetFileFormat]

      val mergeSchema = sessionState.convertMetastoreParquetWithSchemaMerging
      val options = Map(ParquetOptions.MERGE_SCHEMA -> mergeSchema.toString)

        logicalRelation
      })
    }
    // The inferred schema may have different field names as the table schema, we should respect
    // it, but also respect the exprId in table relation output.
    assert(result.output.length == relation.output.length &&
      result.output.zip(relation.output).forall { case (a1, a2) => a1.dataType == a2.dataType })
    val newOutput = result.output.zip(relation.output).map {
      case (a1, a2) => a1.withExprId(a2.exprId)
    }
    result.copy(output = newOutput)
  }

  /**
   * When scanning Metastore ORC tables, convert them to ORC data source relations
   * for better performance.
   */
  object OrcConversions extends Rule[LogicalPlan] {
    private def shouldConvertMetastoreOrc(relation: MetastoreRelation): Boolean = {
      relation.tableDesc.getSerdeClassName.toLowerCase.contains("orc") &&
        sessionState.convertMetastoreOrc
    }

    private def convertToOrcRelation(relation: MetastoreRelation): LogicalRelation = {
      val defaultSource = new OrcFileFormat()
      val fileFormatClass = classOf[OrcFileFormat]
      val options = Map[String, String]()

      convertToLogicalRelation(relation, options, defaultSource, fileFormatClass, "orc")
    }

    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved || plan.analyzed) {
        return plan
      }

      plan transformUp {
        // Write path
        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Orc data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
          InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

        // Write path
        case InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          // Inserting into partitioned table is not supported in Orc data source (yet).
          if !r.hiveQlTable.isPartitioned && shouldConvertMetastoreOrc(r) =>
          InsertIntoTable(convertToOrcRelation(r), partition, child, overwrite, ifNotExists)

        // Read path
        case relation: MetastoreRelation if shouldConvertMetastoreOrc(relation) =>
          val orcRelation = convertToOrcRelation(relation)
          SubqueryAlias(relation.alias.getOrElse(relation.tableName), orcRelation)
      }
    }
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p
      case p: LogicalPlan if p.resolved => p

      case p @ CreateHiveTableAsSelectLogicalPlan(table, query, allowExisting) =>
        val desc = if (table.storage.serde.isEmpty) {
          // add default serde
          table.withNewStorage(
            serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
        } else {
          table
        }

        val QualifiedTableName(dbName, tblName) = getQualifiedTableName(table)

        execution.CreateHiveTableAsSelectCommand(
          desc.copy(identifier = TableIdentifier(tblName, Some(dbName))),
          query,
          allowExisting)
    }
  }
}

/**
 * An override of the standard HDFS listing based catalog, that overrides the partition spec with
 * the information from the metastore.
 *
 * @param tableBasePath The default base path of the Hive metastore table
 * @param partitionSpec The partition specifications from Hive metastore
 */
private[hive] class MetaStorePartitionedTableFileCatalog(
    sparkSession: SparkSession,
    tableBasePath: Path,
    override val partitionSpec: PartitionSpec)
  extends ListingFileCatalog(
    sparkSession,
    MetaStorePartitionedTableFileCatalog.getPaths(tableBasePath, partitionSpec),
    Map.empty,
    Some(partitionSpec.partitionColumns)) {
}

private[hive] object MetaStorePartitionedTableFileCatalog {
  /** Get the list of paths to list files in the for a metastore table */
  def getPaths(tableBasePath: Path, partitionSpec: PartitionSpec): Seq[Path] = {
    // If there are no partitions currently specified then use base path,
    // otherwise use the paths corresponding to the partitions.
    if (partitionSpec.partitions.isEmpty) {
      Seq(tableBasePath)
    } else {
      partitionSpec.partitions.map(_.path)
    }
  }
}


private[hive] object HiveMetastoreCatalog {
  def mergeWithMetastoreSchema(
      metastoreSchema: StructType,
      inferredSchema: StructType): StructType = try {
    // Find any nullable fields in mestastore schema that are missing from the inferred schema.
    val metastoreFields = metastoreSchema.map(f => f.name.toLowerCase -> f).toMap
    val missingNullables = metastoreFields
      .filterKeys(!inferredSchema.map(_.name.toLowerCase).contains(_))
      .values
      .filter(_.nullable)
    // Merge missing nullable fields to inferred schema and build a case-insensitive field map.
    val inferredFields = StructType(inferredSchema ++ missingNullables)
      .map(f => f.name.toLowerCase -> f).toMap
    StructType(metastoreSchema.map(f => f.copy(name = inferredFields(f.name).name)))
  } catch {
    case NonFatal(_) =>
      val msg = s"""Detected conflicting schemas when merging the schema obtained from the Hive
         | Metastore with the one inferred from the file format. Metastore schema:
         |${metastoreSchema.prettyJson}
         |
         |Inferred schema:
         |${inferredSchema.prettyJson}
       """.stripMargin
      throw new SparkException(msg)
  }
}
