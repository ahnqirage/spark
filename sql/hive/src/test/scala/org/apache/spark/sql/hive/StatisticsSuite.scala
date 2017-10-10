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

import java.io.{File, PrintWriter}

import scala.reflect.ClassTag
import scala.util.matching.Regex

import org.apache.hadoop.hive.common.StatsSetupConst

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.AnalyzeTableCommand
import org.apache.spark.sql.execution.joins._
import org.apache.spark.sql.hive.HiveExternalCatalog._
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

class StatisticsSuite extends QueryTest with TestHiveSingleton with SQLTestUtils {

  test("parse analyze commands") {
    def assertAnalyzeCommand(analyzeCommand: String, c: Class[_]) {
      val parsed = spark.sessionState.sqlParser.parsePlan(analyzeCommand)
      val operators = parsed.collect {
        case a: AnalyzeTableCommand => a
        case o => o
      }
    }
  }

  test("analyze Hive serde tables") {
    def queryTotalSize(tableName: String): BigInt =
      spark.table(tableName).queryExecution.analyzed.stats.sizeInBytes

    // Non-partitioned table
    val nonPartTable = "non_part_table"
    withTable(nonPartTable) {
      sql(s"CREATE TABLE $nonPartTable (key STRING, value STRING)")
      sql(s"INSERT INTO TABLE $nonPartTable SELECT * FROM src")
      sql(s"INSERT INTO TABLE $nonPartTable SELECT * FROM src")

      sql(s"ANALYZE TABLE $nonPartTable COMPUTE STATISTICS noscan")

      assert(queryTotalSize(nonPartTable) === BigInt(11624))
    }

    // Partitioned table
    val partTable = "part_table"
    withTable(partTable) {
      sql(s"CREATE TABLE $partTable (key STRING, value STRING) PARTITIONED BY (ds STRING)")
      sql(s"INSERT INTO TABLE $partTable PARTITION (ds='2010-01-01') SELECT * FROM src")
      sql(s"INSERT INTO TABLE $partTable PARTITION (ds='2010-01-02') SELECT * FROM src")
      sql(s"INSERT INTO TABLE $partTable PARTITION (ds='2010-01-03') SELECT * FROM src")

      assert(queryTotalSize(partTable) === spark.sessionState.conf.defaultSizeInBytes)

      sql(s"ANALYZE TABLE $partTable COMPUTE STATISTICS noscan")

      assert(queryTotalSize(partTable) === BigInt(17436))
    }

    // Try to analyze a temp table
    withView("tempTable") {
      sql("""SELECT * FROM src""").createOrReplaceTempView("tempTable")
      intercept[AnalysisException] {
        sql("ANALYZE TABLE tempTable COMPUTE STATISTICS")
      }
    }
  }

  test("SPARK-21079 - analyze table with location different than that of individual partitions") {
    def queryTotalSize(tableName: String): BigInt =
      spark.table(tableName).queryExecution.analyzed.stats(conf).sizeInBytes

    val tableName = "analyzeTable_part"
    withTable(tableName) {
      withTempPath { path =>
        sql(s"CREATE TABLE $tableName (key STRING, value STRING) PARTITIONED BY (ds STRING)")

    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 COMPUTE STATISTICS",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds='2008-04-09', hr=11) COMPUTE STATISTICS noscan",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds, hr) COMPUTE STATISTICS",
      classOf[AnalyzeTableCommand])
    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 PARTITION(ds, hr) COMPUTE STATISTICS noscan",
      classOf[AnalyzeTableCommand])

    assertAnalyzeCommand(
      "ANALYZE TABLE Table1 COMPUTE STATISTICS nOscAn",
      classOf[AnalyzeTableCommand])
  }

  test("MetastoreRelations fallback to HDFS for size estimation") {
    val enableFallBackToHdfsForStats = spark.sessionState.conf.fallBackToHdfsForStatsEnabled
    try {
      withTempDir { tempDir =>

        // EXTERNAL OpenCSVSerde table pointing to LOCATION

        val file1 = new File(tempDir + "/data1")
        val writer1 = new PrintWriter(file1)
        writer1.write("1,2")
        writer1.close()

        val file2 = new File(tempDir + "/data2")
        val writer2 = new PrintWriter(file2)
        writer2.write("1,2")
        writer2.close()

        sql(
          s"""CREATE EXTERNAL TABLE csv_table(page_id INT, impressions INT)
            ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
            WITH SERDEPROPERTIES (
              \"separatorChar\" = \",\",
              \"quoteChar\"     = \"\\\"\",
              \"escapeChar\"    = \"\\\\\")
            LOCATION '$tempDir'
          """)

        spark.conf.set(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key, true)

        val relation = spark.sessionState.catalog.lookupRelation(TableIdentifier("csv_table"))
          .asInstanceOf[MetastoreRelation]

        val properties = relation.hiveQlTable.getParameters
        assert(properties.get("totalSize").toLong <= 0, "external table totalSize must be <= 0")
        assert(properties.get("rawDataSize").toLong <= 0, "external table rawDataSize must be <= 0")

        val sizeInBytes = relation.statistics.sizeInBytes
        assert(sizeInBytes === BigInt(file1.length() + file2.length()))
      }
    } finally {
      spark.conf.set(SQLConf.ENABLE_FALL_BACK_TO_HDFS_FOR_STATS.key, enableFallBackToHdfsForStats)
      sql("DROP TABLE csv_table ")
    }
  }

  test("analyze MetastoreRelations") {
    def queryTotalSize(tableName: String): BigInt =
      spark.sessionState.catalog.lookupRelation(TableIdentifier(tableName)).statistics.sizeInBytes

    val sourceTableName = "analyzeTable_part"
    val tableName = "analyzeTable_part_vis"
    withTable(sourceTableName, tableName) {
      withTempPath { path =>
          // Create a table with 3 partitions all located under a single top-level directory 'path'
          sql(
            s"""
               |CREATE TABLE $sourceTableName (key STRING, value STRING)
               |PARTITIONED BY (ds STRING)
               |LOCATION '$path'
             """.stripMargin)

          val partitionDates = List("2010-01-01", "2010-01-02", "2010-01-03")
          partitionDates.foreach { ds =>
              sql(
                s"""
                   |INSERT INTO TABLE $sourceTableName PARTITION (ds='$ds')
                   |SELECT * FROM src
                 """.stripMargin)
          }

          // Create another table referring to the same location
          sql(
            s"""
               |CREATE TABLE $tableName (key STRING, value STRING)
               |PARTITIONED BY (ds STRING)
               |LOCATION '$path'
             """.stripMargin)

          // Register only one of the partitions found on disk
          val ds = partitionDates.head
          sql(s"ALTER TABLE $tableName ADD PARTITION (ds='$ds')").collect()

          // Analyze original table - expect 3 partitions
          sql(s"ANALYZE TABLE $sourceTableName COMPUTE STATISTICS noscan")
          assert(queryTotalSize(sourceTableName) === BigInt(3 * 5812))

          // Analyze partial-copy table - expect only 1 partition
          sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS noscan")
          assert(queryTotalSize(tableName) === BigInt(5812))
        }
    }
  }

  test("analyzing views is not supported") {
    def assertAnalyzeUnsupported(analyzeCommand: String): Unit = {
      val err = intercept[AnalysisException] {
        sql(analyzeCommand)
      }
    }
  }

  test("SPARK-21079 - analyze partitioned table with only a subset of partitions visible") {
    val sourceTableName = "analyzeTable_part"
    val tableName = "analyzeTable_part_vis"
    withTable(sourceTableName, tableName) {
      withTempPath { path =>
          // Create a table with 3 partitions all located under a single top-level directory 'path'
          sql(
            s"""
               |CREATE TABLE $sourceTableName (key STRING, value STRING)
               |PARTITIONED BY (ds STRING)
               |LOCATION '${path.toURI}'
             """.stripMargin)

          val partitionDates = List("2010-01-01", "2010-01-02", "2010-01-03")
          partitionDates.foreach { ds =>
              sql(
                s"""
                   |INSERT INTO TABLE $sourceTableName PARTITION (ds='$ds')
                   |SELECT * FROM src
                 """.stripMargin)
          }

          // Create another table referring to the same location
          sql(
            s"""
               |CREATE TABLE $tableName (key STRING, value STRING)
               |PARTITIONED BY (ds STRING)
               |LOCATION '${path.toURI}'
             """.stripMargin)

          // Register only one of the partitions found on disk
          val ds = partitionDates.head
          sql(s"ALTER TABLE $tableName ADD PARTITION (ds='$ds')")

          // Analyze original table - expect 3 partitions
          sql(s"ANALYZE TABLE $sourceTableName COMPUTE STATISTICS noscan")
          assert(getCatalogStatistics(sourceTableName).sizeInBytes === BigInt(3 * 5812))

          // Analyze partial-copy table - expect only 1 partition
          sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS noscan")
          assert(getCatalogStatistics(tableName).sizeInBytes === BigInt(5812))
        }
    }
  }

  test("analyze single partition") {
    val tableName = "analyzeTable_part"

    def queryStats(ds: String): CatalogStatistics = {
      val partition =
        spark.sessionState.catalog.getPartition(TableIdentifier(tableName), Map("ds" -> ds))
      partition.stats.get
    }

    def createPartition(ds: String, query: String): Unit = {
      sql(s"INSERT INTO TABLE $tableName PARTITION (ds='$ds') $query")
    }

    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) PARTITIONED BY (ds STRING)")

      createPartition("2010-01-01", "SELECT '1', 'A' from src")
      createPartition("2010-01-02", "SELECT '1', 'A' from src UNION ALL SELECT '1', 'A' from src")
      createPartition("2010-01-03", "SELECT '1', 'A' from src")

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-01') COMPUTE STATISTICS NOSCAN")

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-02') COMPUTE STATISTICS NOSCAN")

      assert(queryStats("2010-01-01").rowCount === None)
      assert(queryStats("2010-01-01").sizeInBytes === 2000)

      assert(queryStats("2010-01-02").rowCount === None)
      assert(queryStats("2010-01-02").sizeInBytes === 2*2000)

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-01') COMPUTE STATISTICS")

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-02') COMPUTE STATISTICS")

      assert(queryStats("2010-01-01").rowCount.get === 500)
      assert(queryStats("2010-01-01").sizeInBytes === 2000)

      assert(queryStats("2010-01-02").rowCount.get === 2*500)
      assert(queryStats("2010-01-02").sizeInBytes === 2*2000)
    }
  }

  test("analyze a set of partitions") {
    val tableName = "analyzeTable_part"

    def queryStats(ds: String, hr: String): Option[CatalogStatistics] = {
      val tableId = TableIdentifier(tableName)
      val partition =
        spark.sessionState.catalog.getPartition(tableId, Map("ds" -> ds, "hr" -> hr))
      partition.stats
    }

    def assertPartitionStats(
        ds: String,
        hr: String,
        rowCount: Option[BigInt],
        sizeInBytes: BigInt): Unit = {
      val stats = queryStats(ds, hr).get
      assert(stats.rowCount === rowCount)
      assert(stats.sizeInBytes === sizeInBytes)
    }

    def createPartition(ds: String, hr: Int, query: String): Unit = {
      sql(s"INSERT INTO TABLE $tableName PARTITION (ds='$ds', hr=$hr) $query")
    }

    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) PARTITIONED BY (ds STRING, hr INT)")

      createPartition("2010-01-01", 10, "SELECT '1', 'A' from src")
      createPartition("2010-01-01", 11, "SELECT '1', 'A' from src")
      createPartition("2010-01-02", 10, "SELECT '1', 'A' from src")
      createPartition("2010-01-02", 11,
        "SELECT '1', 'A' from src UNION ALL SELECT '1', 'A' from src")

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-01') COMPUTE STATISTICS NOSCAN")

      assertPartitionStats("2010-01-01", "10", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = None, sizeInBytes = 2000)
      assert(queryStats("2010-01-02", "10") === None)
      assert(queryStats("2010-01-02", "11") === None)

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-02') COMPUTE STATISTICS NOSCAN")

      assertPartitionStats("2010-01-01", "10", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "10", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "11", rowCount = None, sizeInBytes = 2*2000)

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-01') COMPUTE STATISTICS")

      assertPartitionStats("2010-01-01", "10", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "10", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "11", rowCount = None, sizeInBytes = 2*2000)

      sql(s"ANALYZE TABLE $tableName PARTITION (ds='2010-01-02') COMPUTE STATISTICS")

      assertPartitionStats("2010-01-01", "10", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "10", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "11", rowCount = Some(2*500), sizeInBytes = 2*2000)
    }
  }

  test("analyze all partitions") {
    val tableName = "analyzeTable_part"

    def assertPartitionStats(
        ds: String,
        hr: String,
        rowCount: Option[BigInt],
        sizeInBytes: BigInt): Unit = {
      val stats = spark.sessionState.catalog.getPartition(TableIdentifier(tableName),
        Map("ds" -> ds, "hr" -> hr)).stats.get
      assert(stats.rowCount === rowCount)
      assert(stats.sizeInBytes === sizeInBytes)
    }

    def createPartition(ds: String, hr: Int, query: String): Unit = {
      sql(s"INSERT INTO TABLE $tableName PARTITION (ds='$ds', hr=$hr) $query")
    }

    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) PARTITIONED BY (ds STRING, hr INT)")

      createPartition("2010-01-01", 10, "SELECT '1', 'A' from src")
      createPartition("2010-01-01", 11, "SELECT '1', 'A' from src")
      createPartition("2010-01-02", 10, "SELECT '1', 'A' from src")
      createPartition("2010-01-02", 11,
        "SELECT '1', 'A' from src UNION ALL SELECT '1', 'A' from src")

      sql(s"ANALYZE TABLE $tableName PARTITION (ds, hr) COMPUTE STATISTICS NOSCAN")

      assertPartitionStats("2010-01-01", "10", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = None, sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "11", rowCount = None, sizeInBytes = 2*2000)

      sql(s"ANALYZE TABLE $tableName PARTITION (ds, hr) COMPUTE STATISTICS")

      assertPartitionStats("2010-01-01", "10", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-01", "11", rowCount = Some(500), sizeInBytes = 2000)
      assertPartitionStats("2010-01-02", "11", rowCount = Some(2*500), sizeInBytes = 2*2000)
    }
  }

  test("analyze partitions for an empty table") {
    val tableName = "analyzeTable_part"

    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) PARTITIONED BY (ds STRING)")

      // make sure there is no exception
      sql(s"ANALYZE TABLE $tableName PARTITION (ds) COMPUTE STATISTICS NOSCAN")

      // make sure there is no exception
      sql(s"ANALYZE TABLE $tableName PARTITION (ds) COMPUTE STATISTICS")
    }
  }

  test("analyze partitions case sensitivity") {
    val tableName = "analyzeTable_part"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (key STRING, value STRING) PARTITIONED BY (ds STRING)")

      sql(s"INSERT INTO TABLE $tableName PARTITION (ds='2010-01-01') SELECT * FROM src")

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
        sql(s"ANALYZE TABLE $tableName PARTITION (DS='2010-01-01') COMPUTE STATISTICS")
      }

      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        val message = intercept[AnalysisException] {
          sql(s"ANALYZE TABLE $tableName PARTITION (DS='2010-01-01') COMPUTE STATISTICS")
        }.getMessage
        assert(message.contains(
          s"DS is not a valid partition column in table `default`.`${tableName.toLowerCase}`"))
      }
    }
  }

  test("test table-level statistics for hive tables created in HiveExternalCatalog") {
    val textTable = "textTable"
    withTable(textTable) {
      // Currently Spark's statistics are self-contained, we don't have statistics until we use
      // the `ANALYZE TABLE` command.
      sql(s"CREATE TABLE $textTable (key STRING, value STRING) STORED AS TEXTFILE")
      checkTableStats(
        textTable,
        hasSizeInBytes = false,
        expectedRowCounts = None)
      sql(s"INSERT INTO TABLE $textTable SELECT * FROM src")
      checkTableStats(
        textTable,
        hasSizeInBytes = true,
        expectedRowCounts = None)

      // noscan won't count the number of rows
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS noscan")
      val fetchedStats1 =
        checkTableStats(textTable, hasSizeInBytes = true, expectedRowCounts = None)

      // without noscan, we count the number of rows
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS")
      val fetchedStats2 =
        checkTableStats(textTable, hasSizeInBytes = true, expectedRowCounts = Some(500))
      assert(fetchedStats1.get.sizeInBytes == fetchedStats2.get.sizeInBytes)
    }
  }

  test("keep existing row count in stats with noscan if table is not changed") {
    val textTable = "textTable"
    withTable(textTable) {
      sql(s"CREATE TABLE $textTable (key STRING, value STRING)")
      sql(s"INSERT INTO TABLE $textTable SELECT * FROM src")
      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS")
      val fetchedStats1 =
        checkTableStats(textTable, hasSizeInBytes = true, expectedRowCounts = Some(500))

      sql(s"ANALYZE TABLE $textTable COMPUTE STATISTICS noscan")
      // when the table is not changed, total size is the same, and the old row count is kept
      val fetchedStats2 =
        checkTableStats(textTable, hasSizeInBytes = true, expectedRowCounts = Some(500))
      assert(fetchedStats1 == fetchedStats2)
    }
  }

  test("keep existing column stats if table is not changed") {
    val table = "update_col_stats_table"
    withTable(table) {
      sql(s"CREATE TABLE $table (c1 INT, c2 STRING, c3 DOUBLE)")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1")
      val fetchedStats0 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(0))
      assert(fetchedStats0.get.colStats == Map("c1" -> ColumnStat(0, None, None, 0, 4, 4)))

      // Insert new data and analyze: have the latest column stats.
      sql(s"INSERT INTO TABLE $table SELECT 1, 'a', 10.0")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1")
      val fetchedStats1 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(1)).get
      assert(fetchedStats1.colStats == Map(
        "c1" -> ColumnStat(distinctCount = 1, min = Some(1), max = Some(1), nullCount = 0,
          avgLen = 4, maxLen = 4)))

      // Analyze another column: since the table is not changed, the precious column stats are kept.
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c2")
      val fetchedStats2 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(1)).get
      assert(fetchedStats2.colStats == Map(
        "c1" -> ColumnStat(distinctCount = 1, min = Some(1), max = Some(1), nullCount = 0,
          avgLen = 4, maxLen = 4),
        "c2" -> ColumnStat(distinctCount = 1, min = None, max = None, nullCount = 0,
          avgLen = 1, maxLen = 1)))

      // Insert new data and analyze: stale column stats are removed and newly collected column
      // stats are added.
      sql(s"INSERT INTO TABLE $table SELECT 2, 'b', 20.0")
      sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS c1, c3")
      val fetchedStats3 =
        checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(2)).get
      assert(fetchedStats3.colStats == Map(
        "c1" -> ColumnStat(distinctCount = 2, min = Some(1), max = Some(2), nullCount = 0,
          avgLen = 4, maxLen = 4),
        "c3" -> ColumnStat(distinctCount = 2, min = Some(10.0), max = Some(20.0), nullCount = 0,
          avgLen = 8, maxLen = 8)))
    }
  }

  private def createNonPartitionedTable(
      tabName: String,
      analyzedBySpark: Boolean = true,
      analyzedByHive: Boolean = true): Unit = {
    sql(
      s"""
         |CREATE TABLE $tabName (key STRING, value STRING)
         |STORED AS TEXTFILE
         |TBLPROPERTIES ('prop1' = 'val1', 'prop2' = 'val2')
       """.stripMargin)
    sql(s"INSERT INTO TABLE $tabName SELECT * FROM src")
    if (analyzedBySpark) sql(s"ANALYZE TABLE $tabName COMPUTE STATISTICS")
    // This is to mimic the scenario in which Hive genrates statistics before we reading it
    if (analyzedByHive) hiveClient.runSqlHive(s"ANALYZE TABLE $tabName COMPUTE STATISTICS")
    val describeResult1 = hiveClient.runSqlHive(s"DESCRIBE FORMATTED $tabName")

    assert(queryTotalSize("analyzeTable_part") === spark.sessionState.conf.defaultSizeInBytes)

    if (analyzedByHive) {
      assert(StringUtils.filterPattern(describeResult1, "*numRows\\s+500*").nonEmpty)
    } else {
      assert(StringUtils.filterPattern(describeResult1, "*numRows\\s+500*").isEmpty)
    }
  }

    assert(queryTotalSize("analyzeTable_part") === BigInt(17436))

  test("get statistics when not analyzed in Hive or Spark") {
    val tabName = "tab1"
    withTable(tabName) {
      createNonPartitionedTable(tabName, analyzedByHive = false, analyzedBySpark = false)
      checkTableStats(tabName, hasSizeInBytes = true, expectedRowCounts = None)

    // Try to analyze a temp table
    sql("""SELECT * FROM src""").createOrReplaceTempView("tempTable")
    intercept[AnalysisException] {
      sql("ANALYZE TABLE tempTable COMPUTE STATISTICS")
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("tempTable"), ignoreIfNotExists = true)
  }

  test("change stats after add/drop partition command") {
    val table = "change_stats_part_table"
    Seq(false, true).foreach { autoUpdate =>
      withSQLConf(SQLConf.AUTO_UPDATE_SIZE.key -> autoUpdate.toString) {
        withTable(table) {
          sql(s"CREATE TABLE $table (i INT, j STRING) PARTITIONED BY (ds STRING, hr STRING)")
          // table has two partitions initially
          for (ds <- Seq("2008-04-08"); hr <- Seq("11", "12")) {
            sql(s"INSERT OVERWRITE TABLE $table PARTITION (ds='$ds',hr='$hr') SELECT 1, 'a'")
          }
          // analyze to get initial stats
          sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS i, j")
          val fetched1 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = Some(2))
          assert(fetched1.get.sizeInBytes > 0)
          assert(fetched1.get.colStats.size == 2)

          withTempPaths(numPaths = 2) { case Seq(dir1, dir2) =>
            val file1 = new File(dir1 + "/data")
            val writer1 = new PrintWriter(file1)
            writer1.write("1,a")
            writer1.close()

            val file2 = new File(dir2 + "/data")
            val writer2 = new PrintWriter(file2)
            writer2.write("1,a")
            writer2.close()

            // add partition command
            sql(
              s"""
                 |ALTER TABLE $table ADD
                 |PARTITION (ds='2008-04-09', hr='11') LOCATION '${dir1.toURI.toString}'
                 |PARTITION (ds='2008-04-09', hr='12') LOCATION '${dir2.toURI.toString}'
            """.stripMargin)
            if (autoUpdate) {
              val fetched2 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
              assert(fetched2.get.sizeInBytes > fetched1.get.sizeInBytes)
              assert(fetched2.get.colStats.isEmpty)
              val statsProp = getStatsProperties(table)
              assert(statsProp(STATISTICS_TOTAL_SIZE).toLong == fetched2.get.sizeInBytes)
            } else {
              assert(getStatsProperties(table).isEmpty)
            }

            // now the table has four partitions, generate stats again
            sql(s"ANALYZE TABLE $table COMPUTE STATISTICS FOR COLUMNS i, j")
            val fetched3 = checkTableStats(
              table, hasSizeInBytes = true, expectedRowCounts = Some(4))
            assert(fetched3.get.sizeInBytes > 0)
            assert(fetched3.get.colStats.size == 2)

            // drop partition command
            sql(s"ALTER TABLE $table DROP PARTITION (ds='2008-04-08'), PARTITION (hr='12')")
            assert(spark.sessionState.catalog.listPartitions(TableIdentifier(table))
              .map(_.spec).toSet == Set(Map("ds" -> "2008-04-09", "hr" -> "11")))
            // only one partition left
            if (autoUpdate) {
              val fetched4 = checkTableStats(table, hasSizeInBytes = true, expectedRowCounts = None)
              assert(fetched4.get.sizeInBytes < fetched1.get.sizeInBytes)
              assert(fetched4.get.colStats.isEmpty)
              val statsProp = getStatsProperties(table)
              assert(statsProp(STATISTICS_TOTAL_SIZE).toLong == fetched4.get.sizeInBytes)
            } else {
              assert(getStatsProperties(table).isEmpty)
            }
          }
        }
      }
    }
  }

  test("add/drop partitions - managed table") {
    val catalog = spark.sessionState.catalog
    val managedTable = "partitionedTable"
    withTable(managedTable) {
      sql(
        s"""
           |CREATE TABLE $managedTable (key INT, value STRING)
           |PARTITIONED BY (ds STRING, hr STRING)
         """.stripMargin)

      for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- Seq("11", "12")) {
        sql(
          s"""
             |INSERT OVERWRITE TABLE $managedTable
             |partition (ds='$ds',hr='$hr')
             |SELECT 1, 'a'
           """.stripMargin)
      }

      checkTableStats(
        managedTable, hasSizeInBytes = false, expectedRowCounts = None)

      sql(s"ANALYZE TABLE $managedTable COMPUTE STATISTICS")

      val stats1 = checkTableStats(
        managedTable, hasSizeInBytes = true, expectedRowCounts = Some(4))

      sql(
        s"""
           |ALTER TABLE $managedTable DROP PARTITION (ds='2008-04-08'),
           |PARTITION (hr='12')
        """.stripMargin)
      assert(catalog.listPartitions(TableIdentifier(managedTable)).map(_.spec).toSet ==
        Set(Map("ds" -> "2008-04-09", "hr" -> "11")))

      sql(s"ANALYZE TABLE $managedTable COMPUTE STATISTICS")

      val stats2 = checkTableStats(
        managedTable, hasSizeInBytes = true, expectedRowCounts = Some(1))
      assert(stats1.get.sizeInBytes > stats2.get.sizeInBytes)

      sql(s"ALTER TABLE $managedTable ADD PARTITION (ds='2008-04-08', hr='12')")
      sql(s"ANALYZE TABLE $managedTable COMPUTE STATISTICS")
      val stats4 = checkTableStats(
        managedTable, hasSizeInBytes = true, expectedRowCounts = Some(1))

      assert(stats1.get.sizeInBytes > stats4.get.sizeInBytes)
      assert(stats4.get.sizeInBytes == stats2.get.sizeInBytes)
    }
  }

  test("test statistics of LogicalRelation converted from Hive serde tables") {
    val parquetTable = "parquetTable"
    val orcTable = "orcTable"
    withTable(parquetTable, orcTable) {
      sql(s"CREATE TABLE $parquetTable (key STRING, value STRING) STORED AS PARQUET")
      sql(s"CREATE TABLE $orcTable (key STRING, value STRING) STORED AS ORC")
      sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src")
      sql(s"INSERT INTO TABLE $orcTable SELECT * FROM src")

      // the default value for `spark.sql.hive.convertMetastoreParquet` is true, here we just set it
      // for robustness
      withSQLConf(HiveUtils.CONVERT_METASTORE_PARQUET.key -> "true") {
        checkTableStats(parquetTable, hasSizeInBytes = false, expectedRowCounts = None)
        sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS")
        checkTableStats(parquetTable, hasSizeInBytes = true, expectedRowCounts = Some(500))
      }
      withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "true") {
        // We still can get tableSize from Hive before Analyze
        checkTableStats(orcTable, hasSizeInBytes = true, expectedRowCounts = None)
        sql(s"ANALYZE TABLE $orcTable COMPUTE STATISTICS")
        checkTableStats(orcTable, hasSizeInBytes = true, expectedRowCounts = Some(500))
      }
    }
  }

  test("verify serialized column stats after analyzing columns") {
    import testImplicits._

    val tableName = "column_stats_test2"
    // (data.head.productArity - 1) because the last column does not support stats collection.
    assert(stats.size == data.head.productArity - 1)
    val df = data.toDF(stats.keys.toSeq :+ "carray" : _*)

    withTable(tableName) {
      df.write.saveAsTable(tableName)

      // Collect statistics
      sql(s"analyze table $tableName compute STATISTICS FOR COLUMNS " + stats.keys.mkString(", "))

      // Validate statistics
      val table = hiveClient.getTable("default", tableName)

      val props = table.properties.filterKeys(_.startsWith("spark.sql.statistics.colStats"))
      assert(props == Map(
        "spark.sql.statistics.colStats.cbinary.avgLen" -> "3",
        "spark.sql.statistics.colStats.cbinary.distinctCount" -> "2",
        "spark.sql.statistics.colStats.cbinary.maxLen" -> "3",
        "spark.sql.statistics.colStats.cbinary.nullCount" -> "1",
        "spark.sql.statistics.colStats.cbinary.version" -> "1",
        "spark.sql.statistics.colStats.cbool.avgLen" -> "1",
        "spark.sql.statistics.colStats.cbool.distinctCount" -> "2",
        "spark.sql.statistics.colStats.cbool.max" -> "true",
        "spark.sql.statistics.colStats.cbool.maxLen" -> "1",
        "spark.sql.statistics.colStats.cbool.min" -> "false",
        "spark.sql.statistics.colStats.cbool.nullCount" -> "1",
        "spark.sql.statistics.colStats.cbool.version" -> "1",
        "spark.sql.statistics.colStats.cbyte.avgLen" -> "1",
        "spark.sql.statistics.colStats.cbyte.distinctCount" -> "2",
        "spark.sql.statistics.colStats.cbyte.max" -> "2",
        "spark.sql.statistics.colStats.cbyte.maxLen" -> "1",
        "spark.sql.statistics.colStats.cbyte.min" -> "1",
        "spark.sql.statistics.colStats.cbyte.nullCount" -> "1",
        "spark.sql.statistics.colStats.cbyte.version" -> "1",
        "spark.sql.statistics.colStats.cdate.avgLen" -> "4",
        "spark.sql.statistics.colStats.cdate.distinctCount" -> "2",
        "spark.sql.statistics.colStats.cdate.max" -> "2016-05-09",
        "spark.sql.statistics.colStats.cdate.maxLen" -> "4",
        "spark.sql.statistics.colStats.cdate.min" -> "2016-05-08",
        "spark.sql.statistics.colStats.cdate.nullCount" -> "1",
        "spark.sql.statistics.colStats.cdate.version" -> "1",
        "spark.sql.statistics.colStats.cdecimal.avgLen" -> "16",
        "spark.sql.statistics.colStats.cdecimal.distinctCount" -> "2",
        "spark.sql.statistics.colStats.cdecimal.max" -> "8.000000000000000000",
        "spark.sql.statistics.colStats.cdecimal.maxLen" -> "16",
        "spark.sql.statistics.colStats.cdecimal.min" -> "1.000000000000000000",
        "spark.sql.statistics.colStats.cdecimal.nullCount" -> "1",
        "spark.sql.statistics.colStats.cdecimal.version" -> "1",
        "spark.sql.statistics.colStats.cdouble.avgLen" -> "8",
        "spark.sql.statistics.colStats.cdouble.distinctCount" -> "2",
        "spark.sql.statistics.colStats.cdouble.max" -> "6.0",
        "spark.sql.statistics.colStats.cdouble.maxLen" -> "8",
        "spark.sql.statistics.colStats.cdouble.min" -> "1.0",
        "spark.sql.statistics.colStats.cdouble.nullCount" -> "1",
        "spark.sql.statistics.colStats.cdouble.version" -> "1",
        "spark.sql.statistics.colStats.cfloat.avgLen" -> "4",
        "spark.sql.statistics.colStats.cfloat.distinctCount" -> "2",
        "spark.sql.statistics.colStats.cfloat.max" -> "7.0",
        "spark.sql.statistics.colStats.cfloat.maxLen" -> "4",
        "spark.sql.statistics.colStats.cfloat.min" -> "1.0",
        "spark.sql.statistics.colStats.cfloat.nullCount" -> "1",
        "spark.sql.statistics.colStats.cfloat.version" -> "1",
        "spark.sql.statistics.colStats.cint.avgLen" -> "4",
        "spark.sql.statistics.colStats.cint.distinctCount" -> "2",
        "spark.sql.statistics.colStats.cint.max" -> "4",
        "spark.sql.statistics.colStats.cint.maxLen" -> "4",
        "spark.sql.statistics.colStats.cint.min" -> "1",
        "spark.sql.statistics.colStats.cint.nullCount" -> "1",
        "spark.sql.statistics.colStats.cint.version" -> "1",
        "spark.sql.statistics.colStats.clong.avgLen" -> "8",
        "spark.sql.statistics.colStats.clong.distinctCount" -> "2",
        "spark.sql.statistics.colStats.clong.max" -> "5",
        "spark.sql.statistics.colStats.clong.maxLen" -> "8",
        "spark.sql.statistics.colStats.clong.min" -> "1",
        "spark.sql.statistics.colStats.clong.nullCount" -> "1",
        "spark.sql.statistics.colStats.clong.version" -> "1",
        "spark.sql.statistics.colStats.cshort.avgLen" -> "2",
        "spark.sql.statistics.colStats.cshort.distinctCount" -> "2",
        "spark.sql.statistics.colStats.cshort.max" -> "3",
        "spark.sql.statistics.colStats.cshort.maxLen" -> "2",
        "spark.sql.statistics.colStats.cshort.min" -> "1",
        "spark.sql.statistics.colStats.cshort.nullCount" -> "1",
        "spark.sql.statistics.colStats.cshort.version" -> "1",
        "spark.sql.statistics.colStats.cstring.avgLen" -> "3",
        "spark.sql.statistics.colStats.cstring.distinctCount" -> "2",
        "spark.sql.statistics.colStats.cstring.maxLen" -> "3",
        "spark.sql.statistics.colStats.cstring.nullCount" -> "1",
        "spark.sql.statistics.colStats.cstring.version" -> "1",
        "spark.sql.statistics.colStats.ctimestamp.avgLen" -> "8",
        "spark.sql.statistics.colStats.ctimestamp.distinctCount" -> "2",
        "spark.sql.statistics.colStats.ctimestamp.max" -> "2016-05-09 00:00:02.0",
        "spark.sql.statistics.colStats.ctimestamp.maxLen" -> "8",
        "spark.sql.statistics.colStats.ctimestamp.min" -> "2016-05-08 00:00:01.0",
        "spark.sql.statistics.colStats.ctimestamp.nullCount" -> "1",
        "spark.sql.statistics.colStats.ctimestamp.version" -> "1"
      ))
    }
  }

  private def testUpdatingTableStats(tableDescription: String, createTableCmd: String): Unit = {
    test("test table-level statistics for " + tableDescription) {
      val parquetTable = "parquetTable"
      withTable(parquetTable) {
        sql(createTableCmd)
        val catalogTable = getCatalogTable(parquetTable)
        assert(DDLUtils.isDatasourceTable(catalogTable))

        // Add a filter to avoid creating too many partitions
        sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src WHERE key < 10")
        checkTableStats(parquetTable, hasSizeInBytes = false, expectedRowCounts = None)

        // noscan won't count the number of rows
        sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS noscan")
        val fetchedStats1 =
          checkTableStats(parquetTable, hasSizeInBytes = true, expectedRowCounts = None)

        sql(s"INSERT INTO TABLE $parquetTable SELECT * FROM src WHERE key < 10")
        sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS noscan")
        val fetchedStats2 =
          checkTableStats(parquetTable, hasSizeInBytes = true, expectedRowCounts = None)
        assert(fetchedStats2.get.sizeInBytes > fetchedStats1.get.sizeInBytes)

        // without noscan, we count the number of rows
        sql(s"ANALYZE TABLE $parquetTable COMPUTE STATISTICS")
        val fetchedStats3 =
          checkTableStats(parquetTable, hasSizeInBytes = true, expectedRowCounts = Some(20))
        assert(fetchedStats3.get.sizeInBytes == fetchedStats2.get.sizeInBytes)
      }
    }
  }

  testUpdatingTableStats(
    "data source table created in HiveExternalCatalog",
    "CREATE TABLE parquetTable (key STRING, value STRING) USING PARQUET")

  testUpdatingTableStats(
    "partitioned data source table",
    "CREATE TABLE parquetTable (key STRING, value STRING) USING PARQUET PARTITIONED BY (key)")

  /** Used to test refreshing cached metadata once table stats are updated. */
  private def getStatsBeforeAfterUpdate(isAnalyzeColumns: Boolean)
    : (CatalogStatistics, CatalogStatistics) = {
    val tableName = "tbl"
    var statsBeforeUpdate: CatalogStatistics = null
    var statsAfterUpdate: CatalogStatistics = null
    withTable(tableName) {
      val tableIndent = TableIdentifier(tableName, Some("default"))
      val catalog = spark.sessionState.catalog.asInstanceOf[HiveSessionCatalog]
      sql(s"CREATE TABLE $tableName (key int) USING PARQUET")
      sql(s"INSERT INTO $tableName SELECT 1")
      if (isAnalyzeColumns) {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS key")
      } else {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      }
      // Table lookup will make the table cached.
      spark.table(tableIndent)
      statsBeforeUpdate = catalog.metastoreCatalog.getCachedDataSourceTable(tableIndent)
        .asInstanceOf[LogicalRelation].catalogTable.get.stats.get

      sql(s"INSERT INTO $tableName SELECT 2")
      if (isAnalyzeColumns) {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS FOR COLUMNS key")
      } else {
        sql(s"ANALYZE TABLE $tableName COMPUTE STATISTICS")
      }
      spark.table(tableIndent)
      statsAfterUpdate = catalog.metastoreCatalog.getCachedDataSourceTable(tableIndent)
        .asInstanceOf[LogicalRelation].catalogTable.get.stats.get
    }
    (statsBeforeUpdate, statsAfterUpdate)
  }

  test("test refreshing table stats of cached data source table by `ANALYZE TABLE` statement") {
    val (statsBeforeUpdate, statsAfterUpdate) = getStatsBeforeAfterUpdate(isAnalyzeColumns = false)

    assert(statsBeforeUpdate.sizeInBytes > 0)
    assert(statsBeforeUpdate.rowCount == Some(1))

    assert(statsAfterUpdate.sizeInBytes > statsBeforeUpdate.sizeInBytes)
    assert(statsAfterUpdate.rowCount == Some(2))
  }

  test("estimates the size of a test Hive serde tables") {
    val df = sql("""SELECT * FROM src""")
    val sizes = df.queryExecution.analyzed.collect {
      case relation: HiveTableRelation => relation.stats(conf).sizeInBytes
    }
    assert(sizes.size === 1, s"Size wrong for:\n ${df.queryExecution}")
    assert(sizes(0).equals(BigInt(5812)),
      s"expected exact size 5812 for test table 'src', got: ${sizes(0)}")
  }

  test("auto converts to broadcast hash join, by size estimate of a relation") {
    def mkTest(
        before: () => Unit,
        after: () => Unit,
        query: String,
        expectedAnswer: Seq[Row],
        ct: ClassTag[_]): Unit = {
      before()

      var df = sql(query)

      // Assert src has a size smaller than the threshold.
      val sizes = df.queryExecution.analyzed.collect {
        case r if ct.runtimeClass.isAssignableFrom(r.getClass) => r.stats.sizeInBytes
      }
      assert(sizes.size === 2 && sizes(0) <= spark.sessionState.conf.autoBroadcastJoinThreshold
        && sizes(1) <= spark.sessionState.conf.autoBroadcastJoinThreshold,
        s"query should contain two relations, each of which has size smaller than autoConvertSize")

      // Using `sparkPlan` because for relevant patterns in HashJoin to be
      // matched, other strategies need to be applied.
      var bhj = df.queryExecution.sparkPlan.collect { case j: BroadcastHashJoinExec => j }
      assert(bhj.size === 1,
        s"actual query plans do not contain broadcast join: ${df.queryExecution}")

      checkAnswer(df, expectedAnswer) // check correctness of output

      spark.sessionState.conf.settings.synchronized {
        val tmp = spark.sessionState.conf.autoBroadcastJoinThreshold

        sql(s"""SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=-1""")
        df = sql(query)
        bhj = df.queryExecution.sparkPlan.collect { case j: BroadcastHashJoinExec => j }
        assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

        val shj = df.queryExecution.sparkPlan.collect { case j: SortMergeJoinExec => j }
        assert(shj.size === 1,
          "SortMergeJoin should be planned when BroadcastHashJoin is turned off")

        sql(s"""SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=$tmp""")
      }

      after()
    }

    /** Tests for Hive serde tables */
    val metastoreQuery = """SELECT * FROM src a JOIN src b ON a.key = 238 AND a.key = b.key"""
    val metastoreAnswer = Seq.fill(4)(Row(238, "val_238", 238, "val_238"))
    mkTest(
      () => (),
      () => (),
      metastoreQuery,
      metastoreAnswer,
      implicitly[ClassTag[HiveTableRelation]]
    )
  }

  test("auto converts to broadcast left semi join, by size estimate of a relation") {
    val leftSemiJoinQuery =
      """SELECT * FROM src a
        |left semi JOIN src b ON a.key=86 and a.key = b.key""".stripMargin
    val answer = Row(86, "val_86")

    var df = sql(leftSemiJoinQuery)

    // Assert src has a size smaller than the threshold.
    val sizes = df.queryExecution.analyzed.collect {
      case relation: HiveTableRelation => relation.stats(conf).sizeInBytes
    }
    assert(sizes.size === 2 && sizes(1) <= spark.sessionState.conf.autoBroadcastJoinThreshold
      && sizes(0) <= spark.sessionState.conf.autoBroadcastJoinThreshold,
      s"query should contain two relations, each of which has size smaller than autoConvertSize")

    // Using `sparkPlan` because for relevant patterns in HashJoin to be
    // matched, other strategies need to be applied.
    var bhj = df.queryExecution.sparkPlan.collect {
      case j: BroadcastHashJoinExec => j
    }
    assert(bhj.size === 1,
      s"actual query plans do not contain broadcast join: ${df.queryExecution}")

    checkAnswer(df, answer) // check correctness of output

    spark.sessionState.conf.settings.synchronized {
      val tmp = spark.sessionState.conf.autoBroadcastJoinThreshold

      sql(s"SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=-1")
      df = sql(leftSemiJoinQuery)
      bhj = df.queryExecution.sparkPlan.collect {
        case j: BroadcastHashJoinExec => j
      }
      assert(bhj.isEmpty, "BroadcastHashJoin still planned even though it is switched off")

      val shj = df.queryExecution.sparkPlan.collect {
        case j: SortMergeJoinExec => j
      }
      assert(shj.size === 1,
        "SortMergeJoinExec should be planned when BroadcastHashJoin is turned off")

      sql(s"SET ${SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key}=$tmp")
    }

  }
}
