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

package org.apache.spark.sql.catalyst.catalog

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Range, SubqueryAlias}


/**
 * Tests for [[SessionCatalog]]
 *
 * Note: many of the methods here are very similar to the ones in [[ExternalCatalogSuite]].
 * This is because [[SessionCatalog]] and [[ExternalCatalog]] share many similar method
 * signatures but do not extend a common parent. This is largely by design but
 * unfortunately leads to very similar test code in two places.
 */
abstract class SessionCatalogSuite extends AnalysisTest {
  protected val utils: CatalogTestUtils

  protected val isHiveExternalCatalog = false

  import utils._

  private def withBasicCatalog(f: SessionCatalog => Unit): Unit = {
    val catalog = new SessionCatalog(newBasicCatalog())
    try {
      f(catalog)
    } finally {
      catalog.reset()
    }
  }

  private def withEmptyCatalog(f: SessionCatalog => Unit): Unit = {
    val catalog = new SessionCatalog(newEmptyCatalog())
    catalog.createDatabase(newDb("default"), ignoreIfExists = true)
    try {
      f(catalog)
    } finally {
      catalog.reset()
    }
  }
  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  test("basic create and list databases") {
    withEmptyCatalog { catalog =>
      assert(catalog.databaseExists("default"))
      assert(!catalog.databaseExists("testing"))
      assert(!catalog.databaseExists("testing2"))
      catalog.createDatabase(newDb("testing"), ignoreIfExists = false)
      assert(catalog.databaseExists("testing"))
      assert(catalog.listDatabases().toSet == Set("default", "testing"))
      catalog.createDatabase(newDb("testing2"), ignoreIfExists = false)
      assert(catalog.listDatabases().toSet == Set("default", "testing", "testing2"))
      assert(catalog.databaseExists("testing2"))
      assert(!catalog.databaseExists("does_not_exist"))
    }
  }

  def testInvalidName(func: (String) => Unit) {
    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    val name = "砖"
    // scalastyle:on
    val e = intercept[AnalysisException] {
      func(name)
    }.getMessage
    assert(e.contains(s"`$name` is not a valid name for tables/databases."))
  }

  test("create databases using invalid names") {
    withEmptyCatalog { catalog =>
      testInvalidName(
        name => catalog.createDatabase(newDb(name), ignoreIfExists = true))
    }
  }

  test("get database when a database exists") {
    withBasicCatalog { catalog =>
      val db1 = catalog.getDatabaseMetadata("db1")
      assert(db1.name == "db1")
      assert(db1.description.contains("db1"))
    }
  }

  test("get database should throw exception when the database does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.getDatabaseMetadata("db_that_does_not_exist")
    }
  }

  test("list databases without pattern") {
    withBasicCatalog { catalog =>
      assert(catalog.listDatabases().toSet == Set("default", "db1", "db2", "db3"))
    }
  }

  test("list databases with pattern") {
    withBasicCatalog { catalog =>
      assert(catalog.listDatabases("db").toSet == Set.empty)
      assert(catalog.listDatabases("db*").toSet == Set("db1", "db2", "db3"))
      assert(catalog.listDatabases("*1").toSet == Set("db1"))
      assert(catalog.listDatabases("db2").toSet == Set("db2"))
    }
  }

  test("drop database") {
    withBasicCatalog { catalog =>
      catalog.dropDatabase("db1", ignoreIfNotExists = false, cascade = false)
      assert(catalog.listDatabases().toSet == Set("default", "db2", "db3"))
    }
  }

  test("drop database when the database is not empty") {
    // Throw exception if there are functions left
    withBasicCatalog { catalog =>
      catalog.externalCatalog.dropTable("db2", "tbl1", ignoreIfNotExists = false, purge = false)
      catalog.externalCatalog.dropTable("db2", "tbl2", ignoreIfNotExists = false, purge = false)
      intercept[AnalysisException] {
        catalog.dropDatabase("db2", ignoreIfNotExists = false, cascade = false)
      }
    }
    withBasicCatalog { catalog =>
      // Throw exception if there are tables left
      catalog.externalCatalog.dropFunction("db2", "func1")
      intercept[AnalysisException] {
        catalog.dropDatabase("db2", ignoreIfNotExists = false, cascade = false)
      }
    }

    withBasicCatalog { catalog =>
      // When cascade is true, it should drop them
      catalog.externalCatalog.dropDatabase("db2", ignoreIfNotExists = false, cascade = true)
      assert(catalog.listDatabases().toSet == Set("default", "db1", "db3"))
    }
  }

  test("drop database when the database does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.dropDatabase("db_that_does_not_exist", ignoreIfNotExists = false, cascade = false)
    }
  }

  test("alter database") {
    withBasicCatalog { catalog =>
      val db1 = catalog.getDatabaseMetadata("db1")
      // Note: alter properties here because Hive does not support altering other fields
      catalog.alterDatabase(db1.copy(properties = Map("k" -> "v3", "good" -> "true")))
      val newDb1 = catalog.getDatabaseMetadata("db1")
      assert(db1.properties.isEmpty)
      assert(newDb1.properties.size == 2)
      assert(newDb1.properties.get("k") == Some("v3"))
      assert(newDb1.properties.get("good") == Some("true"))
    }
  }

  test("alter database should throw exception when the database does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.alterDatabase(newDb("unknown_db"))
    }
  }

  test("get/set current database") {
    val catalog = new SessionCatalog(newBasicCatalog())
    assert(catalog.getCurrentDatabase == "default")
    catalog.setCurrentDatabase("db2")
    assert(catalog.getCurrentDatabase == "db2")
    intercept[NoSuchDatabaseException] {
      catalog.setCurrentDatabase("deebo")
      assert(catalog.getCurrentDatabase == "deebo")
    }
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  test("create table") {
    withBasicCatalog { catalog =>
      assert(catalog.externalCatalog.listTables("db1").isEmpty)
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      catalog.createTable(newTable("tbl3", "db1"), ignoreIfExists = false)
      catalog.createTable(newTable("tbl3", "db2"), ignoreIfExists = false)
      assert(catalog.externalCatalog.listTables("db1").toSet == Set("tbl3"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2", "tbl3"))
      // Create table without explicitly specifying database
      catalog.setCurrentDatabase("db1")
      catalog.createTable(newTable("tbl4"), ignoreIfExists = false)
      assert(catalog.externalCatalog.listTables("db1").toSet == Set("tbl3", "tbl4"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2", "tbl3"))
    }
  }

  test("create table when database does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    // Creating table in non-existent database should always fail
    intercept[NoSuchDatabaseException] {
      catalog.createTable(newTable("tbl1", "does_not_exist"), ignoreIfExists = false)
    }
    intercept[NoSuchDatabaseException] {
      catalog.createTable(newTable("tbl1", "does_not_exist"), ignoreIfExists = true)
    }
    // Table already exists
    intercept[TableAlreadyExistsException] {
      catalog.createTable(newTable("tbl1", "db2"), ignoreIfExists = false)
    }
  }

  test("create temp table") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempTable1 = Range(1, 10, 1, 10)
    val tempTable2 = Range(1, 20, 2, 10)
    catalog.createTempView("tbl1", tempTable1, overrideIfExists = false)
    catalog.createTempView("tbl2", tempTable2, overrideIfExists = false)
    assert(catalog.getTempView("tbl1") == Option(tempTable1))
    assert(catalog.getTempView("tbl2") == Option(tempTable2))
    assert(catalog.getTempView("tbl3").isEmpty)
    // Temporary table already exists
    intercept[TempTableAlreadyExistsException] {
      catalog.createTempView("tbl1", tempTable1, overrideIfExists = false)
    }
    // Temporary table already exists but we override it
    catalog.createTempView("tbl1", tempTable2, overrideIfExists = true)
    assert(catalog.getTempView("tbl1") == Option(tempTable2))
  }

  test("drop table") {
    withBasicCatalog { catalog =>
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      catalog.dropTable(TableIdentifier("tbl1", Some("db2")), ignoreIfNotExists = false,
        purge = false)
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl2"))
      // Drop table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      catalog.dropTable(TableIdentifier("tbl2"), ignoreIfNotExists = false, purge = false)
      assert(catalog.externalCatalog.listTables("db2").isEmpty)
    }
  }

  test("drop table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    // Should always throw exception when the database does not exist
    intercept[NoSuchDatabaseException] {
      catalog.dropTable(TableIdentifier("tbl1", Some("unknown_db")), ignoreIfNotExists = false)
    }
    intercept[NoSuchDatabaseException] {
      catalog.dropTable(TableIdentifier("tbl1", Some("unknown_db")), ignoreIfNotExists = true)
    }
    intercept[NoSuchTableException] {
      catalog.dropTable(TableIdentifier("unknown_table", Some("db2")), ignoreIfNotExists = false)
    }
    catalog.dropTable(TableIdentifier("unknown_table", Some("db2")), ignoreIfNotExists = true)
  }

  test("drop temp table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tempTable = Range(1, 10, 2, 10)
    sessionCatalog.createTempView("tbl1", tempTable, overrideIfExists = false)
    sessionCatalog.setCurrentDatabase("db2")
    assert(sessionCatalog.getTempView("tbl1") == Some(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If database is not specified, temp table should be dropped first
    sessionCatalog.dropTable(TableIdentifier("tbl1"), ignoreIfNotExists = false)
    assert(sessionCatalog.getTempView("tbl1") == None)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If temp table does not exist, the table in the current database should be dropped
    sessionCatalog.dropTable(TableIdentifier("tbl1"), ignoreIfNotExists = false)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl2"))
    // If database is specified, temp tables are never dropped
    sessionCatalog.createTempView("tbl1", tempTable, overrideIfExists = false)
    sessionCatalog.createTable(newTable("tbl1", "db2"), ignoreIfExists = false)
    sessionCatalog.dropTable(TableIdentifier("tbl1", Some("db2")), ignoreIfNotExists = false)
    assert(sessionCatalog.getTempView("tbl1") == Some(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl2"))
  }

  test("rename table") {
    withBasicCatalog { catalog =>
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      catalog.renameTable(TableIdentifier("tbl1", Some("db2")), TableIdentifier("tblone"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tblone", "tbl2"))
      catalog.renameTable(TableIdentifier("tbl2", Some("db2")), TableIdentifier("tbltwo"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tblone", "tbltwo"))
      // Rename table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      catalog.renameTable(TableIdentifier("tbltwo"), TableIdentifier("table_two"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tblone", "table_two"))
      // Renaming "db2.tblone" to "db1.tblones" should fail because databases don't match
      intercept[AnalysisException] {
        catalog.renameTable(
          TableIdentifier("tblone", Some("db2")), TableIdentifier("tblones", Some("db1")))
      }
      // The new table already exists
      intercept[TableAlreadyExistsException] {
        catalog.renameTable(
          TableIdentifier("tblone", Some("db2")),
          TableIdentifier("table_two"))
      }
    }
    // The new table already exists
    intercept[TableAlreadyExistsException] {
      sessionCatalog.renameTable(
        TableIdentifier("tblone", Some("db2")), TableIdentifier("table_two", Some("db2")))
    }
  }

  test("rename table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.renameTable(
        TableIdentifier("tbl1", Some("unknown_db")), TableIdentifier("tbl2", Some("unknown_db")))
    }
    intercept[NoSuchTableException] {
      catalog.renameTable(
        TableIdentifier("unknown_table", Some("db2")), TableIdentifier("tbl2", Some("db2")))
    }
  }

  test("rename temp table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tempTable = Range(1, 10, 2, 10)
    sessionCatalog.createTempView("tbl1", tempTable, overrideIfExists = false)
    sessionCatalog.setCurrentDatabase("db2")
    assert(sessionCatalog.getTempView("tbl1") == Option(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If database is not specified, temp table should be renamed first
    sessionCatalog.renameTable(TableIdentifier("tbl1"), TableIdentifier("tbl3"))
    assert(sessionCatalog.getTempView("tbl1").isEmpty)
    assert(sessionCatalog.getTempView("tbl3") == Option(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If database is specified, temp tables are never renamed
    sessionCatalog.renameTable(
      TableIdentifier("tbl2", Some("db2")), TableIdentifier("tbl4", Some("db2")))
    assert(sessionCatalog.getTempView("tbl3") == Option(tempTable))
    assert(sessionCatalog.getTempView("tbl4").isEmpty)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl4"))
  }

  test("alter table") {
    withBasicCatalog { catalog =>
      val tbl1 = catalog.externalCatalog.getTable("db2", "tbl1")
      catalog.alterTable(tbl1.copy(properties = Map("toh" -> "frem")))
      val newTbl1 = catalog.externalCatalog.getTable("db2", "tbl1")
      assert(!tbl1.properties.contains("toh"))
      assert(newTbl1.properties.size == tbl1.properties.size + 1)
      assert(newTbl1.properties.get("toh") == Some("frem"))
      // Alter table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      catalog.alterTable(tbl1.copy(identifier = TableIdentifier("tbl1")))
      val newestTbl1 = catalog.externalCatalog.getTable("db2", "tbl1")
      // For hive serde table, hive metastore will set transient_lastDdlTime in table's properties,
      // and its value will be modified, here we ignore it when comparing the two tables.
      assert(newestTbl1.copy(properties = Map.empty) == tbl1.copy(properties = Map.empty))
    }
  }

  test("alter table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.alterTable(newTable("tbl1", "unknown_db"))
    }
    intercept[NoSuchTableException] {
      catalog.alterTable(newTable("unknown_table", "db2"))
    }
  }

  test("get table") {
    withBasicCatalog { catalog =>
      assert(catalog.getTableMetadata(TableIdentifier("tbl1", Some("db2")))
        == catalog.externalCatalog.getTable("db2", "tbl1"))
      // Get table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalog.getTableMetadata(TableIdentifier("tbl1"))
        == catalog.externalCatalog.getTable("db2", "tbl1"))
    }
  }

  test("get table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.getTableMetadata(TableIdentifier("tbl1", Some("unknown_db")))
    }
    intercept[NoSuchTableException] {
      catalog.getTableMetadata(TableIdentifier("unknown_table", Some("db2")))
    }
  }

  test("get option of table metadata") {
    val externalCatalog = newBasicCatalog()
    val catalog = new SessionCatalog(externalCatalog)
    assert(catalog.getTableMetadataOption(TableIdentifier("tbl1", Some("db2")))
      == Option(externalCatalog.getTable("db2", "tbl1")))
    assert(catalog.getTableMetadataOption(TableIdentifier("unknown_table", Some("db2"))).isEmpty)
    intercept[NoSuchDatabaseException] {
      catalog.getTableMetadataOption(TableIdentifier("tbl1", Some("unknown_db")))
    }
  }

  test("lookup table relation") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tempTable1 = Range(1, 10, 1, 10)
    val metastoreTable1 = externalCatalog.getTable("db2", "tbl1")
    sessionCatalog.createTempView("tbl1", tempTable1, overrideIfExists = false)
    sessionCatalog.setCurrentDatabase("db2")
    // If we explicitly specify the database, we'll look up the relation in that database
    assert(sessionCatalog.lookupRelation(TableIdentifier("tbl1", Some("db2")))
      == SubqueryAlias("tbl1", SimpleCatalogRelation("db2", metastoreTable1)))
    // Otherwise, we'll first look up a temporary table with the same name
    assert(sessionCatalog.lookupRelation(TableIdentifier("tbl1"))
      == SubqueryAlias("tbl1", tempTable1))
    // Then, if that does not exist, look up the relation in the current database
    sessionCatalog.dropTable(TableIdentifier("tbl1"), ignoreIfNotExists = false)
    assert(sessionCatalog.lookupRelation(TableIdentifier("tbl1"))
      == SubqueryAlias("tbl1", SimpleCatalogRelation("db2", metastoreTable1)))
  }

  test("lookup table relation with alias") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val alias = "monster"
    val tableMetadata = catalog.getTableMetadata(TableIdentifier("tbl1", Some("db2")))
    val relation = SubqueryAlias("tbl1", SimpleCatalogRelation("db2", tableMetadata))
    val relationWithAlias =
      SubqueryAlias(alias,
        SubqueryAlias("tbl1",
          SimpleCatalogRelation("db2", tableMetadata, Some(alias))))
    assert(catalog.lookupRelation(
      TableIdentifier("tbl1", Some("db2")), alias = None) == relation)
    assert(catalog.lookupRelation(
      TableIdentifier("tbl1", Some("db2")), alias = Some(alias)) == relationWithAlias)
  }

  test("table exists") {
    val catalog = new SessionCatalog(newBasicCatalog())
    assert(catalog.tableExists(TableIdentifier("tbl1", Some("db2"))))
    assert(catalog.tableExists(TableIdentifier("tbl2", Some("db2"))))
    assert(!catalog.tableExists(TableIdentifier("tbl3", Some("db2"))))
    assert(!catalog.tableExists(TableIdentifier("tbl1", Some("db1"))))
    assert(!catalog.tableExists(TableIdentifier("tbl2", Some("db1"))))
    // If database is explicitly specified, do not check temporary tables
    val tempTable = Range(1, 10, 1, 10)
    assert(!catalog.tableExists(TableIdentifier("tbl3", Some("db2"))))
    // If database is not explicitly specified, check the current database
    catalog.setCurrentDatabase("db2")
    assert(catalog.tableExists(TableIdentifier("tbl1")))
    assert(catalog.tableExists(TableIdentifier("tbl2")))

    catalog.createTempView("tbl3", tempTable, overrideIfExists = false)
    // tableExists should not check temp view.
    assert(!catalog.tableExists(TableIdentifier("tbl3")))
  }

  test("getTempViewOrPermanentTableMetadata on temporary views") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempTable = Range(1, 10, 2, 10)
    intercept[NoSuchTableException] {
      catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("view1"))
    }.getMessage

    intercept[NoSuchTableException] {
      catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("view1", Some("default")))
    }.getMessage

    catalog.createTempView("view1", tempTable, overrideIfExists = false)
    assert(catalog.getTempViewOrPermanentTableMetadata(
      TableIdentifier("view1")).identifier.table == "view1")
    assert(catalog.getTempViewOrPermanentTableMetadata(
      TableIdentifier("view1")).schema(0).name == "id")

    intercept[NoSuchTableException] {
      catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("view1", Some("default")))
    }.getMessage
  }

  test("list tables without pattern") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempTable = Range(1, 10, 2, 10)
    catalog.createTempView("tbl1", tempTable, overrideIfExists = false)
    catalog.createTempView("tbl4", tempTable, overrideIfExists = false)
    assert(catalog.listTables("db1").toSet ==
      Set(TableIdentifier("tbl1"), TableIdentifier("tbl4")))
    assert(catalog.listTables("db2").toSet ==
      Set(TableIdentifier("tbl1"),
        TableIdentifier("tbl4"),
        TableIdentifier("tbl1", Some("db2")),
        TableIdentifier("tbl2", Some("db2"))))
    intercept[NoSuchDatabaseException] {
      catalog.listTables("unknown_db")
    }
  }

  test("list tables with pattern") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempTable = Range(1, 10, 2, 10)
    catalog.createTempView("tbl1", tempTable, overrideIfExists = false)
    catalog.createTempView("tbl4", tempTable, overrideIfExists = false)
    assert(catalog.listTables("db1", "*").toSet == catalog.listTables("db1").toSet)
    assert(catalog.listTables("db2", "*").toSet == catalog.listTables("db2").toSet)
    assert(catalog.listTables("db2", "tbl*").toSet ==
      Set(TableIdentifier("tbl1"),
        TableIdentifier("tbl4"),
        TableIdentifier("tbl1", Some("db2")),
        TableIdentifier("tbl2", Some("db2"))))
    assert(catalog.listTables("db2", "*1").toSet ==
      Set(TableIdentifier("tbl1"), TableIdentifier("tbl1", Some("db2"))))
    intercept[NoSuchDatabaseException] {
      catalog.listTables("unknown_db", "*")
    }
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  test("basic create and list partitions") {
    val externalCatalog = newEmptyCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    sessionCatalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
    sessionCatalog.createTable(newTable("tbl", "mydb"), ignoreIfExists = false)
    sessionCatalog.createPartitions(
      TableIdentifier("tbl", Some("mydb")), Seq(part1, part2), ignoreIfExists = false)
    assert(catalogPartitionsEqual(externalCatalog, "mydb", "tbl", Seq(part1, part2)))
    // Create partitions without explicitly specifying database
    sessionCatalog.setCurrentDatabase("mydb")
    sessionCatalog.createPartitions(
      TableIdentifier("tbl"), Seq(partWithMixedOrder), ignoreIfExists = false)
    assert(catalogPartitionsEqual(
      externalCatalog, "mydb", "tbl", Seq(part1, part2, partWithMixedOrder)))
  }

  test("create partitions when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.createPartitions(
        TableIdentifier("tbl1", Some("unknown_db")), Seq(), ignoreIfExists = false)
    }
    intercept[NoSuchTableException] {
      catalog.createPartitions(
        TableIdentifier("does_not_exist", Some("db2")), Seq(), ignoreIfExists = false)
    }
  }

  test("create partitions that already exist") {
    withBasicCatalog { catalog =>
      intercept[AnalysisException] {
        catalog.createPartitions(
          TableIdentifier("tbl2", Some("db2")), Seq(part1), ignoreIfExists = false)
      }
      catalog.createPartitions(
        TableIdentifier("tbl2", Some("db2")), Seq(part1), ignoreIfExists = true)
    }
  }

  test("create partitions with invalid part spec") {
    val catalog = new SessionCatalog(newBasicCatalog())
    var e = intercept[AnalysisException] {
      catalog.createPartitions(
        TableIdentifier("tbl2", Some("db2")),
        Seq(part1, partWithLessColumns), ignoreIfExists = false)
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl2`'"))
    e = intercept[AnalysisException] {
      catalog.createPartitions(
        TableIdentifier("tbl2", Some("db2")),
        Seq(part1, partWithMoreColumns), ignoreIfExists = true)
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a, b, c) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl2`'"))
    e = intercept[AnalysisException] {
      catalog.createPartitions(
        TableIdentifier("tbl2", Some("db2")),
        Seq(partWithUnknownColumns, part1), ignoreIfExists = true)
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a, unknown) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl2`'"))
  }

  test("drop partitions") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    assert(catalogPartitionsEqual(externalCatalog, "db2", "tbl2", Seq(part1, part2)))
    sessionCatalog.dropPartitions(
      TableIdentifier("tbl2", Some("db2")),
      Seq(part1.spec),
      ignoreIfNotExists = false)
    assert(catalogPartitionsEqual(externalCatalog, "db2", "tbl2", Seq(part2)))
    // Drop partitions without explicitly specifying database
    sessionCatalog.setCurrentDatabase("db2")
    sessionCatalog.dropPartitions(
      TableIdentifier("tbl2"),
      Seq(part2.spec),
      ignoreIfNotExists = false)
    assert(externalCatalog.listPartitions("db2", "tbl2").isEmpty)
    // Drop multiple partitions at once
    sessionCatalog.createPartitions(
      TableIdentifier("tbl2", Some("db2")), Seq(part1, part2), ignoreIfExists = false)
    assert(catalogPartitionsEqual(externalCatalog, "db2", "tbl2", Seq(part1, part2)))
    sessionCatalog.dropPartitions(
      TableIdentifier("tbl2", Some("db2")),
      Seq(part1.spec, part2.spec),
      ignoreIfNotExists = false)
    assert(externalCatalog.listPartitions("db2", "tbl2").isEmpty)
  }

  test("drop partitions when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.dropPartitions(
        TableIdentifier("tbl1", Some("unknown_db")),
        Seq(),
        ignoreIfNotExists = false)
    }
    intercept[NoSuchTableException] {
      catalog.dropPartitions(
        TableIdentifier("tbl2", Some("db2")),
        Seq(part1.spec, part2.spec),
        ignoreIfNotExists = false,
        purge = false,
        retainData = false)
      assert(catalog.externalCatalog.listPartitions("db2", "tbl2").isEmpty)
    }
  }

  test("drop partitions when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.dropPartitions(
          TableIdentifier("tbl1", Some("unknown_db")),
          Seq(),
          ignoreIfNotExists = false,
          purge = false,
          retainData = false)
      }
      intercept[NoSuchTableException] {
        catalog.dropPartitions(
          TableIdentifier("does_not_exist", Some("db2")),
          Seq(),
          ignoreIfNotExists = false,
          purge = false,
          retainData = false)
      }
    }
  }

  test("drop partitions that do not exist") {
    withBasicCatalog { catalog =>
      intercept[AnalysisException] {
        catalog.dropPartitions(
          TableIdentifier("tbl2", Some("db2")),
          Seq(part3.spec),
          ignoreIfNotExists = false,
          purge = false,
          retainData = false)
      }
      catalog.dropPartitions(
        TableIdentifier("tbl2", Some("db2")),
        Seq(part3.spec),
        ignoreIfNotExists = true,
        purge = false,
        retainData = false)
    }
  }

  test("drop partitions with invalid partition spec") {
    withBasicCatalog { catalog =>
      var e = intercept[AnalysisException] {
        catalog.dropPartitions(
          TableIdentifier("tbl2", Some("db2")),
          Seq(partWithMoreColumns.spec),
          ignoreIfNotExists = false,
          purge = false,
          retainData = false)
      }
      assert(e.getMessage.contains(
        "Partition spec is invalid. The spec (a, b, c) must be contained within " +
          "the partition spec (a, b) defined in table '`db2`.`tbl2`'"))
      e = intercept[AnalysisException] {
        catalog.dropPartitions(
          TableIdentifier("tbl2", Some("db2")),
          Seq(partWithUnknownColumns.spec),
          ignoreIfNotExists = false,
          purge = false,
          retainData = false)
      }
      assert(e.getMessage.contains(
        "Partition spec is invalid. The spec (a, unknown) must be contained within " +
          "the partition spec (a, b) defined in table '`db2`.`tbl2`'"))
      e = intercept[AnalysisException] {
        catalog.dropPartitions(
          TableIdentifier("tbl2", Some("db2")),
          Seq(partWithEmptyValue.spec, part1.spec),
          ignoreIfNotExists = false,
          purge = false,
          retainData = false)
      }
      assert(e.getMessage.contains("Partition spec is invalid. The spec ([a=3, b=]) contains an " +
        "empty partition column value"))
    }
  }

  test("drop partitions with invalid partition spec") {
    val catalog = new SessionCatalog(newBasicCatalog())
    var e = intercept[AnalysisException] {
      catalog.dropPartitions(
        TableIdentifier("tbl2", Some("db2")),
        Seq(partWithMoreColumns.spec),
        ignoreIfNotExists = false)
    }
    assert(e.getMessage.contains(
      "Partition spec is invalid. The spec (a, b, c) must be contained within " +
        "the partition spec (a, b) defined in table '`db2`.`tbl2`'"))
    e = intercept[AnalysisException] {
      catalog.dropPartitions(
        TableIdentifier("tbl2", Some("db2")),
        Seq(partWithUnknownColumns.spec),
        ignoreIfNotExists = false)
    }
    assert(e.getMessage.contains(
      "Partition spec is invalid. The spec (a, unknown) must be contained within " +
        "the partition spec (a, b) defined in table '`db2`.`tbl2`'"))
  }

  test("get partition") {
    withBasicCatalog { catalog =>
      assert(catalog.getPartition(
        TableIdentifier("tbl2", Some("db2")), part1.spec).spec == part1.spec)
      assert(catalog.getPartition(
        TableIdentifier("tbl2", Some("db2")), part2.spec).spec == part2.spec)
      // Get partition without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalog.getPartition(TableIdentifier("tbl2"), part1.spec).spec == part1.spec)
      assert(catalog.getPartition(TableIdentifier("tbl2"), part2.spec).spec == part2.spec)
      // Get non-existent partition
      intercept[AnalysisException] {
        catalog.getPartition(TableIdentifier("tbl2"), part3.spec)
      }
    }
  }

  test("get partition when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.getPartition(TableIdentifier("tbl1", Some("unknown_db")), part1.spec)
    }
    intercept[NoSuchTableException] {
      catalog.getPartition(TableIdentifier("does_not_exist", Some("db2")), part1.spec)
    }
  }

  test("get partition with invalid partition spec") {
    val catalog = new SessionCatalog(newBasicCatalog())
    var e = intercept[AnalysisException] {
      catalog.getPartition(TableIdentifier("tbl1", Some("db2")), partWithLessColumns.spec)
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl1`'"))
    e = intercept[AnalysisException] {
      catalog.getPartition(TableIdentifier("tbl1", Some("db2")), partWithMoreColumns.spec)
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a, b, c) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl1`'"))
    e = intercept[AnalysisException] {
      catalog.getPartition(TableIdentifier("tbl1", Some("db2")), partWithUnknownColumns.spec)
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a, unknown) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl1`'"))
  }

  test("rename partitions") {
    withBasicCatalog { catalog =>
      val newPart1 = part1.copy(spec = Map("a" -> "100", "b" -> "101"))
      val newPart2 = part2.copy(spec = Map("a" -> "200", "b" -> "201"))
      val newSpecs = Seq(newPart1.spec, newPart2.spec)
      catalog.renamePartitions(
        TableIdentifier("tbl2", Some("db2")), Seq(part1.spec, part2.spec), newSpecs)
      assert(catalog.getPartition(
        TableIdentifier("tbl2", Some("db2")), newPart1.spec).spec === newPart1.spec)
      assert(catalog.getPartition(
        TableIdentifier("tbl2", Some("db2")), newPart2.spec).spec === newPart2.spec)
      intercept[AnalysisException] {
        catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part1.spec)
      }
      intercept[AnalysisException] {
        catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part2.spec)
      }
      // Rename partitions without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      catalog.renamePartitions(TableIdentifier("tbl2"), newSpecs, Seq(part1.spec, part2.spec))
      assert(catalog.getPartition(TableIdentifier("tbl2"), part1.spec).spec === part1.spec)
      assert(catalog.getPartition(TableIdentifier("tbl2"), part2.spec).spec === part2.spec)
      intercept[AnalysisException] {
        catalog.getPartition(TableIdentifier("tbl2"), newPart1.spec)
      }
      intercept[AnalysisException] {
        catalog.getPartition(TableIdentifier("tbl2"), newPart2.spec)
      }
    }
  }

  test("rename partitions when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.renamePartitions(
        TableIdentifier("tbl1", Some("unknown_db")), Seq(part1.spec), Seq(part2.spec))
    }
    intercept[NoSuchTableException] {
      catalog.renamePartitions(
        TableIdentifier("does_not_exist", Some("db2")), Seq(part1.spec), Seq(part2.spec))
    }
  }

  test("rename partition with invalid partition spec") {
    val catalog = new SessionCatalog(newBasicCatalog())
    var e = intercept[AnalysisException] {
      catalog.renamePartitions(
        TableIdentifier("tbl1", Some("db2")),
        Seq(part1.spec), Seq(partWithLessColumns.spec))
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl1`'"))
    e = intercept[AnalysisException] {
      catalog.renamePartitions(
        TableIdentifier("tbl1", Some("db2")),
        Seq(part1.spec), Seq(partWithMoreColumns.spec))
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a, b, c) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl1`'"))
    e = intercept[AnalysisException] {
      catalog.renamePartitions(
        TableIdentifier("tbl1", Some("db2")),
        Seq(part1.spec), Seq(partWithUnknownColumns.spec))
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a, unknown) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl1`'"))
  }

  test("alter partitions") {
    withBasicCatalog { catalog =>
      val newLocation = newUriForDatabase()
      // Alter but keep spec the same
      val oldPart1 = catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part1.spec)
      val oldPart2 = catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part2.spec)
      catalog.alterPartitions(TableIdentifier("tbl2", Some("db2")), Seq(
        oldPart1.copy(storage = storageFormat.copy(locationUri = Some(newLocation))),
        oldPart2.copy(storage = storageFormat.copy(locationUri = Some(newLocation)))))
      val newPart1 = catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part1.spec)
      val newPart2 = catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part2.spec)
      assert(newPart1.storage.locationUri == Some(newLocation))
      assert(newPart2.storage.locationUri == Some(newLocation))
      assert(oldPart1.storage.locationUri != Some(newLocation))
      assert(oldPart2.storage.locationUri != Some(newLocation))
      // Alter partitions without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      catalog.alterPartitions(TableIdentifier("tbl2"), Seq(oldPart1, oldPart2))
      val newerPart1 = catalog.getPartition(TableIdentifier("tbl2"), part1.spec)
      val newerPart2 = catalog.getPartition(TableIdentifier("tbl2"), part2.spec)
      assert(oldPart1.storage.locationUri == newerPart1.storage.locationUri)
      assert(oldPart2.storage.locationUri == newerPart2.storage.locationUri)
      // Alter but change spec, should fail because new partition specs do not exist yet
      val badPart1 = part1.copy(spec = Map("a" -> "v1", "b" -> "v2"))
      val badPart2 = part2.copy(spec = Map("a" -> "v3", "b" -> "v4"))
      intercept[AnalysisException] {
        catalog.alterPartitions(TableIdentifier("tbl2", Some("db2")), Seq(badPart1, badPart2))
      }
    }
  }

  test("alter partitions when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.alterPartitions(TableIdentifier("tbl1", Some("unknown_db")), Seq(part1))
    }
    intercept[NoSuchTableException] {
      catalog.alterPartitions(TableIdentifier("does_not_exist", Some("db2")), Seq(part1))
    }
  }

  test("alter partition with invalid partition spec") {
    val catalog = new SessionCatalog(newBasicCatalog())
    var e = intercept[AnalysisException] {
      catalog.alterPartitions(TableIdentifier("tbl1", Some("db2")), Seq(partWithLessColumns))
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl1`'"))
    e = intercept[AnalysisException] {
      catalog.alterPartitions(TableIdentifier("tbl1", Some("db2")), Seq(partWithMoreColumns))
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a, b, c) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl1`'"))
    e = intercept[AnalysisException] {
      catalog.alterPartitions(TableIdentifier("tbl1", Some("db2")), Seq(partWithUnknownColumns))
    }
    assert(e.getMessage.contains("Partition spec is invalid. The spec (a, unknown) must match " +
      "the partition spec (a, b) defined in table '`db2`.`tbl1`'"))
  }

  test("list partitions") {
    withBasicCatalog { catalog =>
      assert(catalogPartitionsEqual(
        catalog.listPartitions(TableIdentifier("tbl2", Some("db2"))), part1, part2))
      // List partitions without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalogPartitionsEqual(catalog.listPartitions(TableIdentifier("tbl2")), part1, part2))
    }
  }

  test("list partitions with partial partition spec") {
    withBasicCatalog { catalog =>
      assert(catalogPartitionsEqual(
        catalog.listPartitions(TableIdentifier("tbl2", Some("db2")), Some(Map("a" -> "1"))), part1))
    }
  }

  test("list partitions with invalid partial partition spec") {
    withBasicCatalog { catalog =>
      var e = intercept[AnalysisException] {
        catalog.listPartitions(TableIdentifier("tbl2", Some("db2")), Some(partWithMoreColumns.spec))
      }
      assert(e.getMessage.contains("Partition spec is invalid. The spec (a, b, c) must be " +
        "contained within the partition spec (a, b) defined in table '`db2`.`tbl2`'"))
      e = intercept[AnalysisException] {
        catalog.listPartitions(TableIdentifier("tbl2", Some("db2")),
          Some(partWithUnknownColumns.spec))
      }
      assert(e.getMessage.contains("Partition spec is invalid. The spec (a, unknown) must be " +
        "contained within the partition spec (a, b) defined in table '`db2`.`tbl2`'"))
      e = intercept[AnalysisException] {
        catalog.listPartitions(TableIdentifier("tbl2", Some("db2")), Some(partWithEmptyValue.spec))
      }
      assert(e.getMessage.contains("Partition spec is invalid. The spec ([a=3, b=]) contains an " +
        "empty partition column value"))
    }
  }

  test("list partitions when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.listPartitions(TableIdentifier("tbl1", Some("unknown_db")))
      }
      intercept[NoSuchTableException] {
        catalog.listPartitions(TableIdentifier("does_not_exist", Some("db2")))
      }
    }
  }

  private def catalogPartitionsEqual(
      actualParts: Seq[CatalogTablePartition],
      expectedParts: CatalogTablePartition*): Boolean = {
    // ExternalCatalog may set a default location for partitions, here we ignore the partition
    // location when comparing them.
    // And for hive serde table, hive metastore will set some values(e.g.transient_lastDdlTime)
    // in table's parameters and storage's properties, here we also ignore them.
    val actualPartsNormalize = actualParts.map(p =>
      p.copy(parameters = Map.empty, storage = p.storage.copy(
        properties = Map.empty, locationUri = None, serde = None))).toSet

    val expectedPartsNormalize = expectedParts.map(p =>
        p.copy(parameters = Map.empty, storage = p.storage.copy(
          properties = Map.empty, locationUri = None, serde = None))).toSet

    actualPartsNormalize == expectedPartsNormalize
  }

  test("list partitions when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.listPartitions(TableIdentifier("tbl1", Some("unknown_db")))
    }
    intercept[NoSuchTableException] {
      catalog.listPartitions(TableIdentifier("does_not_exist", Some("db2")))
    }
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  test("basic create and list functions") {
    withEmptyCatalog { catalog =>
      catalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
      catalog.createFunction(newFunc("myfunc", Some("mydb")), ignoreIfExists = false)
      assert(catalog.externalCatalog.listFunctions("mydb", "*").toSet == Set("myfunc"))
      // Create function without explicitly specifying database
      catalog.setCurrentDatabase("mydb")
      catalog.createFunction(newFunc("myfunc2"), ignoreIfExists = false)
      assert(catalog.externalCatalog.listFunctions("mydb", "*").toSet == Set("myfunc", "myfunc2"))
    }
  }

  test("create function when database does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.createFunction(
        newFunc("func5", Some("does_not_exist")), ignoreIfExists = false)
    }
  }

  test("create function that already exists") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[FunctionAlreadyExistsException] {
      catalog.createFunction(newFunc("func1", Some("db2")), ignoreIfExists = false)
    }
  }

  test("create temp function") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempFunc1 = (e: Seq[Expression]) => e.head
    val tempFunc2 = (e: Seq[Expression]) => e.last
    val info1 = new ExpressionInfo("tempFunc1", "temp1")
    val info2 = new ExpressionInfo("tempFunc2", "temp2")
    catalog.createTempFunction("temp1", info1, tempFunc1, ignoreIfExists = false)
    catalog.createTempFunction("temp2", info2, tempFunc2, ignoreIfExists = false)
    val arguments = Seq(Literal(1), Literal(2), Literal(3))
    assert(catalog.lookupFunction(FunctionIdentifier("temp1"), arguments) === Literal(1))
    assert(catalog.lookupFunction(FunctionIdentifier("temp2"), arguments) === Literal(3))
    // Temporary function does not exist.
    intercept[NoSuchFunctionException] {
      catalog.lookupFunction(FunctionIdentifier("temp3"), arguments)
    }
    val tempFunc3 = (e: Seq[Expression]) => Literal(e.size)
    val info3 = new ExpressionInfo("tempFunc3", "temp1")
    // Temporary function already exists
    intercept[TempFunctionAlreadyExistsException] {
      catalog.createTempFunction("temp1", info3, tempFunc3, ignoreIfExists = false)
    }
    // Temporary function is overridden
    catalog.createTempFunction("temp1", info3, tempFunc3, ignoreIfExists = true)
    assert(
      catalog.lookupFunction(FunctionIdentifier("temp1"), arguments) === Literal(arguments.length))
  }

  test("isTemporaryFunction") {
    withBasicCatalog { catalog =>
      // Returns false when the function does not exist
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("temp1")))

      val tempFunc1 = (e: Seq[Expression]) => e.head
      catalog.registerFunction(
        newFunc("temp1", None), overrideIfExists = false, functionBuilder = Some(tempFunc1))

      // Returns true when the function is temporary
      assert(catalog.isTemporaryFunction(FunctionIdentifier("temp1")))

      // Returns false when the function is permanent
      assert(catalog.externalCatalog.listFunctions("db2", "*").toSet == Set("func1"))
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("func1", Some("db2"))))
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("db2.func1")))
      catalog.setCurrentDatabase("db2")
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("func1")))

      // Returns false when the function is built-in or hive
      assert(FunctionRegistry.builtin.functionExists(FunctionIdentifier("sum")))
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("sum")))
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("histogram_numeric")))
    }
  }

  test("drop function when database/function does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.dropFunction(
        FunctionIdentifier("something", Some("unknown_db")), ignoreIfNotExists = false)
    }
    intercept[NoSuchFunctionException] {
      catalog.dropFunction(FunctionIdentifier("does_not_exist"), ignoreIfNotExists = false)
    }
  }

  test("drop temp function") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val info = new ExpressionInfo("tempFunc", "func1")
    val tempFunc = (e: Seq[Expression]) => e.head
    catalog.createTempFunction("func1", info, tempFunc, ignoreIfExists = false)
    val arguments = Seq(Literal(1), Literal(2), Literal(3))
    assert(catalog.lookupFunction(FunctionIdentifier("func1"), arguments) === Literal(1))
    catalog.dropTempFunction("func1", ignoreIfNotExists = false)
    intercept[NoSuchFunctionException] {
      catalog.lookupFunction(FunctionIdentifier("func1"), arguments)
    }
    intercept[NoSuchTempFunctionException] {
      catalog.dropTempFunction("func1", ignoreIfNotExists = false)
      intercept[NoSuchFunctionException] {
        catalog.lookupFunction(FunctionIdentifier("func1"), arguments)
      }
      intercept[NoSuchTempFunctionException] {
        catalog.dropTempFunction("func1", ignoreIfNotExists = false)
      }
      catalog.dropTempFunction("func1", ignoreIfNotExists = true)
    }
  }

  test("get function") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val expected =
      CatalogFunction(FunctionIdentifier("func1", Some("db2")), funcClass,
      Seq.empty[FunctionResource])
    assert(catalog.getFunctionMetadata(FunctionIdentifier("func1", Some("db2"))) == expected)
    // Get function without explicitly specifying database
    catalog.setCurrentDatabase("db2")
    assert(catalog.getFunctionMetadata(FunctionIdentifier("func1")) == expected)
  }

  test("get function when database/function does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.getFunctionMetadata(FunctionIdentifier("func1", Some("unknown_db")))
    }
    intercept[NoSuchFunctionException] {
      catalog.getFunctionMetadata(FunctionIdentifier("does_not_exist", Some("db2")))
    }
  }

  test("lookup temp function") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val info1 = new ExpressionInfo("tempFunc1", "func1")
    val tempFunc1 = (e: Seq[Expression]) => e.head
    catalog.createTempFunction("func1", info1, tempFunc1, ignoreIfExists = false)
    assert(catalog.lookupFunction(
      FunctionIdentifier("func1"), Seq(Literal(1), Literal(2), Literal(3))) == Literal(1))
    catalog.dropTempFunction("func1", ignoreIfNotExists = false)
    intercept[NoSuchFunctionException] {
      catalog.lookupFunction(FunctionIdentifier("func1"), Seq(Literal(1), Literal(2), Literal(3)))
    }
  }

  test("list functions") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val info1 = new ExpressionInfo("tempFunc1", "func1")
    val info2 = new ExpressionInfo("tempFunc2", "yes_me")
    val tempFunc1 = (e: Seq[Expression]) => e.head
    val tempFunc2 = (e: Seq[Expression]) => e.last
    catalog.createFunction(newFunc("func2", Some("db2")), ignoreIfExists = false)
    catalog.createFunction(newFunc("not_me", Some("db2")), ignoreIfExists = false)
    catalog.createTempFunction("func1", info1, tempFunc1, ignoreIfExists = false)
    catalog.createTempFunction("yes_me", info2, tempFunc2, ignoreIfExists = false)
    assert(catalog.listFunctions("db1", "*").map(_._1).toSet ==
      Set(FunctionIdentifier("func1"),
        FunctionIdentifier("yes_me")))
    assert(catalog.listFunctions("db2", "*").map(_._1).toSet ==
      Set(FunctionIdentifier("func1"),
        FunctionIdentifier("yes_me"),
        FunctionIdentifier("func1", Some("db2")),
        FunctionIdentifier("func2", Some("db2")),
        FunctionIdentifier("not_me", Some("db2"))))
    assert(catalog.listFunctions("db2", "func*").map(_._1).toSet ==
      Set(FunctionIdentifier("func1"),
        FunctionIdentifier("func1", Some("db2")),
        FunctionIdentifier("func2", Some("db2"))))
  }

  test("list functions when database does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[NoSuchDatabaseException] {
      catalog.listFunctions("unknown_db", "func*")
    }
  }

}
