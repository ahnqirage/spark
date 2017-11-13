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

package org.apache.spark.sql.hive.client

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.net.URI

import org.apache.hadoop.util.VersionInfo
import org.apache.spark.sql.Row
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{Logging, SparkFunSuite}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, EqualTo, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.util.quietly
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveUtils}
import org.apache.spark.sql.hive.test.TestHiveVersion
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.tags.ExtendedHiveTest
<<<<<<< HEAD
import org.apache.spark.util.{MutableURLClassLoader, Utils}
=======
import org.apache.spark.util.Utils
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.hive.test.TestHiveSingleton


/**
 * A simple set of tests that call the methods of a [[HiveClient]], loading different version
 * of hive from maven central.  These tests are simple in that they are mostly just testing to make
 * sure that reflective calls are not throwing NoSuchMethod error, but the actually functionality
 * is not fully tested.
 */
// TODO: Refactor this to `HiveClientSuite` and make it a subclass of `HiveVersionSuite`
@ExtendedHiveTest
class VersionsSuite extends SparkFunSuite  with SQLTestUtils with TestHiveSingleton with Logging {

  // In order to speed up test execution during development or in Jenkins, you can specify the path
  // of an existing Ivy cache:
  private val ivyPath: Option[String] = {
    sys.env.get("SPARK_VERSIONS_SUITE_IVY_PATH").orElse(
      Some(new File(sys.props("java.io.tmpdir"), "hive-ivy-cache").getAbsolutePath))
  }

  private def buildConf() = {
    lazy val warehousePath = Utils.createTempDir()
    lazy val metastorePath = Utils.createTempDir()
    metastorePath.delete()
    Map(
      "javax.jdo.option.ConnectionURL" -> s"jdbc:derby:;databaseName=$metastorePath;create=true",
      "hive.metastore.warehouse.dir" -> warehousePath.toString)
  }

  test("success sanity check") {
    val badClient = IsolatedClientLoader.forVersion(
      hiveMetastoreVersion = HiveContext.hiveExecutionVersion,
      hadoopVersion = VersionInfo.getVersion,
      config = buildConf(),
      ivyPath = ivyPath).createClient()
    val db = new HiveDatabase("default", "")
    badClient.createDatabase(db)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  private def getNestedMessages(e: Throwable): String = {
    var causes = ""
    var lastException = e
    while (lastException != null) {
      causes += lastException.toString + "\n"
      lastException = lastException.getCause
    }
    causes
  }

  private val emptyDir = Utils.createTempDir().getCanonicalPath

  // Its actually pretty easy to mess things up and have all of your tests "pass" by accidentally
  // connecting to an auto-populated, in-process metastore.  Let's make sure we are getting the
  // versions right by forcing a known compatibility failure.
  // TODO: currently only works on mysql where we manually create the schema...
  ignore("failure sanity check") {
    val e = intercept[Throwable] {
<<<<<<< HEAD
      val badClient = quietly { buildClient("13", new Configuration()) }
=======
      val badClient = quietly {
        IsolatedClientLoader.forVersion(
          hiveMetastoreVersion = "13",
          hadoopVersion = VersionInfo.getVersion,
          config = buildConf(),
          ivyPath = ivyPath).createClient()
      }
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    }
    assert(getNestedMessages(e) contains "Unknown column 'A0.OWNER_NAME' in 'field list'")
  }

  private val versions = Seq("0.12", "0.13", "0.14", "1.0", "1.1", "1.2", "2.0", "2.1")

  private var client: HiveClient = null

  private var versionSpark: TestHiveVersion = null

  versions.foreach { version =>
    test(s"$version: create client") {
      client = null
      System.gc() // Hack to avoid SEGV on some JVM versions.
      client =
        IsolatedClientLoader.forVersion(
          hiveMetastoreVersion = version,
          hadoopVersion = VersionInfo.getVersion,
          config = buildConf(),
          ivyPath = ivyPath).createClient()
    }

    ///////////////////////////////////////////////////////////////////////////
    // Database related API
    ///////////////////////////////////////////////////////////////////////////

    val tempDatabasePath = Utils.createTempDir().toURI

    test(s"$version: createDatabase") {
      val defaultDB = CatalogDatabase("default", "desc", new URI("loc"), Map())
      client.createDatabase(defaultDB, ignoreIfExists = true)
      val tempDB = CatalogDatabase(
        "temporary", description = "test create", tempDatabasePath, Map())
      client.createDatabase(tempDB, ignoreIfExists = true)
    }

    test(s"$version: setCurrentDatabase") {
      client.setCurrentDatabase("default")
    }

    test(s"$version: getDatabase") {
      // No exception should be thrown
      client.getDatabase("default")
      intercept[NoSuchDatabaseException](client.getDatabase("nonexist"))
    }

    test(s"$version: databaseExists") {
      assert(client.databaseExists("default") == true)
      assert(client.databaseExists("nonexist") == false)
    }

    test(s"$version: listDatabases") {
      assert(client.listDatabases("defau.*") == Seq("default"))
    }

    test(s"$version: alterDatabase") {
      val database = client.getDatabase("temporary").copy(properties = Map("flag" -> "true"))
      client.alterDatabase(database)
      assert(client.getDatabase("temporary").properties.contains("flag"))
    }

    test(s"$version: dropDatabase") {
      assert(client.databaseExists("temporary") == true)
      client.dropDatabase("temporary", ignoreIfNotExists = false, cascade = true)
      assert(client.databaseExists("temporary") == false)
    }

    ///////////////////////////////////////////////////////////////////////////
    // Table related API
    ///////////////////////////////////////////////////////////////////////////

    test(s"$version: createTable") {
      client.createTable(table("default", tableName = "src"), ignoreIfExists = false)
      client.createTable(table("default", "temporary"), ignoreIfExists = false)
    }

    test(s"$version: loadTable") {
      client.loadTable(
        emptyDir,
        tableName = "src",
        replace = false,
        isSrcLocal = false)
    }

    test(s"$version: tableExists") {
      // No exception should be thrown
      assert(client.tableExists("default", "src"))
      assert(!client.tableExists("default", "nonexistent"))
    }

    test(s"$version: getTable") {
      // No exception should be thrown
      client.getTable("default", "src")
    }

    test(s"$version: getTableOption") {
      assert(client.getTableOption("default", "src").isDefined)
    }

    test(s"$version: alterTable(table: CatalogTable)") {
      val newTable = client.getTable("default", "src").copy(properties = Map("changed" -> ""))
      client.alterTable(newTable)
      assert(client.getTable("default", "src").properties.contains("changed"))
    }

    test(s"$version: alterTable(dbName: String, tableName: String, table: CatalogTable)") {
      val newTable = client.getTable("default", "src").copy(properties = Map("changedAgain" -> ""))
      client.alterTable("default", "src", newTable)
      assert(client.getTable("default", "src").properties.contains("changedAgain"))
    }

    test(s"$version: alterTable - rename") {
      val newTable = client.getTable("default", "src")
        .copy(identifier = TableIdentifier("tgt", database = Some("default")))
      assert(!client.tableExists("default", "tgt"))

      client.alterTable("default", "src", newTable)

      assert(client.tableExists("default", "tgt"))
      assert(!client.tableExists("default", "src"))
    }

    test(s"$version: alterTable - change database") {
      val tempDB = CatalogDatabase(
        "temporary", description = "test create", tempDatabasePath, Map())
      client.createDatabase(tempDB, ignoreIfExists = true)

      val newTable = client.getTable("default", "tgt")
        .copy(identifier = TableIdentifier("tgt", database = Some("temporary")))
      assert(!client.tableExists("temporary", "tgt"))

      client.alterTable("default", "tgt", newTable)

      assert(client.tableExists("temporary", "tgt"))
      assert(!client.tableExists("default", "tgt"))
    }

    test(s"$version: alterTable - change database and table names") {
      val newTable = client.getTable("temporary", "tgt")
        .copy(identifier = TableIdentifier("src", database = Some("default")))
      assert(!client.tableExists("default", "src"))

      client.alterTable("temporary", "tgt", newTable)

      assert(client.tableExists("default", "src"))
      assert(!client.tableExists("temporary", "tgt"))
    }

    test(s"$version: listTables(database)") {
      assert(client.listTables("default") === Seq("src", "temporary"))
    }

    test(s"$version: listTables(database, pattern)") {
      assert(client.listTables("default", pattern = "src") === Seq("src"))
      assert(client.listTables("default", pattern = "nonexist").isEmpty)
    }

    test(s"$version: dropTable") {
      val versionsWithoutPurge = versions.takeWhile(_ != "0.14")
      // First try with the purge option set. This should fail if the version is < 0.14, in which
      // case we check the version and try without it.
      try {
        client.dropTable("default", tableName = "temporary", ignoreIfNotExists = false,
          purge = true)
        assert(!versionsWithoutPurge.contains(version))
      } catch {
        case _: UnsupportedOperationException =>
          assert(versionsWithoutPurge.contains(version))
          client.dropTable("default", tableName = "temporary", ignoreIfNotExists = false,
            purge = false)
      }
      assert(client.listTables("default") === Seq("src"))
    }

    ///////////////////////////////////////////////////////////////////////////
    // Partition related API
    ///////////////////////////////////////////////////////////////////////////

    val storageFormat = CatalogStorageFormat(
      locationUri = None,
      inputFormat = None,
      outputFormat = None,
      serde = None,
      compressed = false,
      properties = Map.empty)

    test(s"$version: sql create partitioned table") {
      client.runSqlHive("CREATE TABLE src_part (value INT) PARTITIONED BY (key1 INT, key2 INT)")
    }

    val testPartitionCount = 2

    test(s"$version: createPartitions") {
      val partitions = (1 to testPartitionCount).map { key2 =>
        CatalogTablePartition(Map("key1" -> "1", "key2" -> key2.toString), storageFormat)
      }
      client.createPartitions(
        "default", "src_part", partitions, ignoreIfExists = true)
    }

    test(s"$version: getPartitionNames(catalogTable)") {
      val partitionNames = (1 to testPartitionCount).map(key2 => s"key1=1/key2=$key2")
      assert(partitionNames == client.getPartitionNames(client.getTable("default", "src_part")))
    }

    test(s"$version: getPartitions(catalogTable)") {
      assert(testPartitionCount ==
        client.getPartitions(client.getTable("default", "src_part")).size)
    }

    test(s"$version: getPartitionsByFilter") {
      // Only one partition [1, 1] for key2 == 1
      val result = client.getPartitionsByFilter(client.getTable("default", "src_part"),
        Seq(EqualTo(AttributeReference("key2", IntegerType)(), Literal(1))))

      // Hive 0.12 doesn't support getPartitionsByFilter, it ignores the filter condition.
      if (version != "0.12") {
        assert(result.size == 1)
      } else {
        assert(result.size == testPartitionCount)
      }
    }

    test(s"$version: getPartition") {
      // No exception should be thrown
      client.getPartition("default", "src_part", Map("key1" -> "1", "key2" -> "2"))
    }

    test(s"$version: getPartitionOption(db: String, table: String, spec: TablePartitionSpec)") {
      val partition = client.getPartitionOption(
        "default", "src_part", Map("key1" -> "1", "key2" -> "2"))
      assert(partition.isDefined)
    }

    test(s"$version: getPartitionOption(table: CatalogTable, spec: TablePartitionSpec)") {
      val partition = client.getPartitionOption(
        client.getTable("default", "src_part"), Map("key1" -> "1", "key2" -> "2"))
      assert(partition.isDefined)
    }

    test(s"$version: getPartitions(db: String, table: String)") {
      assert(testPartitionCount == client.getPartitions("default", "src_part", None).size)
    }

    test(s"$version: loadPartition") {
      val partSpec = new java.util.LinkedHashMap[String, String]
      partSpec.put("key1", "1")
      partSpec.put("key2", "2")

      client.loadPartition(
        emptyDir,
        "default",
        "src_part",
        partSpec,
        replace = false,
        inheritTableSpecs = false,
        isSrcLocal = false)
    }

    test(s"$version: loadDynamicPartitions") {
      val partSpec = new java.util.LinkedHashMap[String, String]
      partSpec.put("key1", "1")
      partSpec.put("key2", "") // Dynamic partition

      client.loadDynamicPartitions(
        emptyDir,
        "default",
        "src_part",
        partSpec,
        replace = false,
        numDP = 1)
    }

    test(s"$version: renamePartitions") {
      val oldSpec = Map("key1" -> "1", "key2" -> "1")
      val newSpec = Map("key1" -> "1", "key2" -> "3")
      client.renamePartitions("default", "src_part", Seq(oldSpec), Seq(newSpec))

      // Checks the existence of the new partition (key1 = 1, key2 = 3)
      assert(client.getPartitionOption("default", "src_part", newSpec).isDefined)
    }

    test(s"$version: alterPartitions") {
      val spec = Map("key1" -> "1", "key2" -> "2")
      val newLocation = new URI(Utils.createTempDir().toURI.toString.stripSuffix("/"))
      val storage = storageFormat.copy(
        locationUri = Some(newLocation),
        // needed for 0.12 alter partitions
        serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
      val partition = CatalogTablePartition(spec, storage)
      client.alterPartitions("default", "src_part", Seq(partition))
      assert(client.getPartition("default", "src_part", spec)
        .storage.locationUri == Some(newLocation))
    }

    test(s"$version: dropPartitions") {
      val spec = Map("key1" -> "1", "key2" -> "3")
      val versionsWithoutPurge = versions.takeWhile(_ != "1.2")
      // Similar to dropTable; try with purge set, and if it fails, make sure we're running
      // with a version that is older than the minimum (1.2 in this case).
      try {
        client.dropPartitions("default", "src_part", Seq(spec), ignoreIfNotExists = true,
          purge = true, retainData = false)
        assert(!versionsWithoutPurge.contains(version))
      } catch {
        case _: UnsupportedOperationException =>
          assert(versionsWithoutPurge.contains(version))
          client.dropPartitions("default", "src_part", Seq(spec), ignoreIfNotExists = true,
            purge = false, retainData = false)
      }

      assert(client.getPartitionOption("default", "src_part", spec).isEmpty)
    }

    ///////////////////////////////////////////////////////////////////////////
    // Function related API
    ///////////////////////////////////////////////////////////////////////////

    def function(name: String, className: String): CatalogFunction = {
      CatalogFunction(
        FunctionIdentifier(name, Some("default")), className, Seq.empty[FunctionResource])
    }

    test(s"$version: createFunction") {
      val functionClass = "org.apache.spark.MyFunc1"
      if (version == "0.12") {
        // Hive 0.12 doesn't support creating permanent functions
        intercept[AnalysisException] {
          client.createFunction("default", function("func1", functionClass))
        }
      } else {
        client.createFunction("default", function("func1", functionClass))
      }
    }

    test(s"$version: functionExists") {
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        assert(client.functionExists("default", "func1") == false)
      } else {
        assert(client.functionExists("default", "func1") == true)
      }
    }

    test(s"$version: renameFunction") {
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        intercept[NoSuchPermanentFunctionException] {
          client.renameFunction("default", "func1", "func2")
        }
      } else {
        client.renameFunction("default", "func1", "func2")
        assert(client.functionExists("default", "func2") == true)
      }
    }

    test(s"$version: alterFunction") {
      val functionClass = "org.apache.spark.MyFunc2"
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        intercept[NoSuchPermanentFunctionException] {
          client.alterFunction("default", function("func2", functionClass))
        }
      } else {
        client.alterFunction("default", function("func2", functionClass))
      }
    }

    test(s"$version: getFunction") {
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        intercept[NoSuchPermanentFunctionException] {
          client.getFunction("default", "func2")
        }
      } else {
        // No exception should be thrown
        val func = client.getFunction("default", "func2")
        assert(func.className == "org.apache.spark.MyFunc2")
      }
    }

    test(s"$version: getFunctionOption") {
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        assert(client.getFunctionOption("default", "func2").isEmpty)
      } else {
        assert(client.getFunctionOption("default", "func2").isDefined)
        assert(client.getFunctionOption("default", "the_func_not_exists").isEmpty)
      }
    }

    test(s"$version: listFunctions") {
      if (version == "0.12") {
        // Hive 0.12 doesn't allow customized permanent functions
        assert(client.listFunctions("default", "fun.*").isEmpty)
      } else {
        assert(client.listFunctions("default", "fun.*").size == 1)
      }
    }

    test(s"$version: dropFunction") {
      if (version == "0.12") {
        // Hive 0.12 doesn't support creating permanent functions
        intercept[NoSuchPermanentFunctionException] {
          client.dropFunction("default", "func2")
        }
      } else {
        // No exception should be thrown
        client.dropFunction("default", "func2")
        assert(client.listFunctions("default", "fun.*").size == 0)
      }
    }

    ///////////////////////////////////////////////////////////////////////////
    // SQL related API
    ///////////////////////////////////////////////////////////////////////////

    test(s"$version: sql set command") {
      client.runSqlHive("SET spark.sql.test.key=1")
    }

    test(s"$version: sql create index and reset") {
      client.runSqlHive("CREATE TABLE indexed_table (key INT)")
      client.runSqlHive("CREATE INDEX index_1 ON TABLE indexed_table(key) " +
        "as 'COMPACT' WITH DEFERRED REBUILD")
    }

    ///////////////////////////////////////////////////////////////////////////
    // Miscellaneous API
    ///////////////////////////////////////////////////////////////////////////

    test(s"$version: version") {
      assert(client.version.fullVersion.startsWith(version))
    }

    test(s"$version: getConf") {
      assert("success" === client.getConf("test", null))
    }

    test(s"$version: setOut") {
      client.setOut(new PrintStream(new ByteArrayOutputStream()))
    }

    test(s"$version: setInfo") {
      client.setInfo(new PrintStream(new ByteArrayOutputStream()))
    }

    test(s"$version: setError") {
      client.setError(new PrintStream(new ByteArrayOutputStream()))
    }

    test(s"$version: newSession") {
      val newClient = client.newSession()
      assert(newClient != null)
    }

    test(s"$version: withHiveState and addJar") {
      val newClassPath = "."
      client.addJar(newClassPath)
      client.withHiveState {
        // No exception should be thrown.
        // withHiveState changes the classloader to MutableURLClassLoader
        val classLoader = Thread.currentThread().getContextClassLoader
          .asInstanceOf[MutableURLClassLoader]

        val urls = classLoader.getURLs()
        urls.contains(new File(newClassPath).toURI.toURL)
      }
    }

    test(s"$version: reset") {
      // Clears all database, tables, functions...
      client.reset()
      assert(client.listTables("default").isEmpty)
    }

    ///////////////////////////////////////////////////////////////////////////
    // End-To-End tests
    ///////////////////////////////////////////////////////////////////////////

    test(s"$version: CREATE TABLE AS SELECT") {
      withTable("tbl") {
        versionSpark.sql("CREATE TABLE tbl AS SELECT 1 AS a")
        assert(versionSpark.table("tbl").collect().toSeq == Seq(Row(1)))
        val tableMeta = versionSpark.sessionState.catalog.getTableMetadata(TableIdentifier("tbl"))
        val totalSize = tableMeta.stats.map(_.sizeInBytes)
        // Except 0.12, all the following versions will fill the Hive-generated statistics
        if (version == "0.12") {
          assert(totalSize.isEmpty)
        } else {
          assert(totalSize.nonEmpty && totalSize.get > 0)
        }
      }
    }

    test(s"$version: Delete the temporary staging directory and files after each insert") {
      withTempDir { tmpDir =>
        withTable("tab") {
          versionSpark.sql(
            s"""
               |CREATE TABLE tab(c1 string)
               |location '${tmpDir.toURI.toString}'
             """.stripMargin)

          (1 to 3).map { i =>
            versionSpark.sql(s"INSERT OVERWRITE TABLE tab SELECT '$i'")
          }
          def listFiles(path: File): List[String] = {
            val dir = path.listFiles()
            val folders = dir.filter(_.isDirectory).toList
            val filePaths = dir.map(_.getName).toList
            folders.flatMap(listFiles) ++: filePaths
          }
          // expect 2 files left: `.part-00000-random-uuid.crc` and `part-00000-random-uuid`
          // 0.12, 0.13, 1.0 and 1.1 also has another two more files ._SUCCESS.crc and _SUCCESS
          val metadataFiles = Seq("._SUCCESS.crc", "_SUCCESS")
          assert(listFiles(tmpDir).filterNot(metadataFiles.contains).length == 2)
        }
      }
    }

    test(s"$version: SPARK-13709: reading partitioned Avro table with nested schema") {
      withTempDir { dir =>
        val path = dir.toURI.toString
        val tableName = "spark_13709"
        val tempTableName = "spark_13709_temp"

        new File(dir.getAbsolutePath, tableName).mkdir()
        new File(dir.getAbsolutePath, tempTableName).mkdir()

        val avroSchema =
          """{
            |  "name": "test_record",
            |  "type": "record",
            |  "fields": [ {
            |    "name": "f0",
            |    "type": "int"
            |  }, {
            |    "name": "f1",
            |    "type": {
            |      "type": "record",
            |      "name": "inner",
            |      "fields": [ {
            |        "name": "f10",
            |        "type": "int"
            |      }, {
            |        "name": "f11",
            |        "type": "double"
            |      } ]
            |    }
            |  } ]
            |}
          """.stripMargin

        withTable(tableName, tempTableName) {
          // Creates the external partitioned Avro table to be tested.
          versionSpark.sql(
            s"""CREATE EXTERNAL TABLE $tableName
               |PARTITIONED BY (ds STRING)
               |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
               |STORED AS
               |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
               |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
               |LOCATION '$path/$tableName'
               |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
          )

          // Creates an temporary Avro table used to prepare testing Avro file.
          versionSpark.sql(
            s"""CREATE EXTERNAL TABLE $tempTableName
               |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
               |STORED AS
               |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
               |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
               |LOCATION '$path/$tempTableName'
               |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
          )

          // Generates Avro data.
          versionSpark.sql(s"INSERT OVERWRITE TABLE $tempTableName SELECT 1, STRUCT(2, 2.5)")

          // Adds generated Avro data as a new partition to the testing table.
          versionSpark.sql(
            s"ALTER TABLE $tableName ADD PARTITION (ds = 'foo') LOCATION '$path/$tempTableName'")

          // The following query fails before SPARK-13709 is fixed. This is because when reading
          // data from table partitions, Avro deserializer needs the Avro schema, which is defined
          // in table property "avro.schema.literal". However, we only initializes the deserializer
          // using partition properties, which doesn't include the wanted property entry. Merging
          // two sets of properties solves the problem.
          assert(versionSpark.sql(s"SELECT * FROM $tableName").collect() ===
            Array(Row(1, Row(2, 2.5D), "foo")))
        }
      }
    }

    test(s"$version: CTAS for managed data source tables") {
      withTable("t", "t1") {
        versionSpark.range(1).write.saveAsTable("t")
        assert(versionSpark.table("t").collect() === Array(Row(0)))
        versionSpark.sql("create table t1 using parquet as select 2 as a")
        assert(versionSpark.table("t1").collect() === Array(Row(2)))
      }
    }

<<<<<<< HEAD
    test(s"$version: Decimal support of Avro Hive serde") {
      val tableName = "tab1"
      // TODO: add the other logical types. For details, see the link:
      // https://avro.apache.org/docs/1.8.1/spec.html#Logical+Types
      val avroSchema =
        """{
          |  "name": "test_record",
          |  "type": "record",
          |  "fields": [ {
          |    "name": "f0",
          |    "type": [
          |      "null",
          |      {
          |        "precision": 38,
          |        "scale": 2,
          |        "type": "bytes",
          |        "logicalType": "decimal"
          |      }
          |    ]
          |  } ]
          |}
        """.stripMargin

      Seq(true, false).foreach { isPartitioned =>
        withTable(tableName) {
          val partitionClause = if (isPartitioned) "PARTITIONED BY (ds STRING)" else ""
          // Creates the (non-)partitioned Avro table
          versionSpark.sql(
            s"""
               |CREATE TABLE $tableName
               |$partitionClause
               |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
               |STORED AS
               |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
               |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
               |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
          )

          val errorMsg = "data type mismatch: cannot cast DecimalType(2,1) to BinaryType"

          if (isPartitioned) {
            val insertStmt = s"INSERT OVERWRITE TABLE $tableName partition (ds='a') SELECT 1.3"
            if (version == "0.12" || version == "0.13") {
              val e = intercept[AnalysisException](versionSpark.sql(insertStmt)).getMessage
              assert(e.contains(errorMsg))
            } else {
              versionSpark.sql(insertStmt)
              assert(versionSpark.table(tableName).collect() ===
                versionSpark.sql("SELECT 1.30, 'a'").collect())
            }
          } else {
            val insertStmt = s"INSERT OVERWRITE TABLE $tableName SELECT 1.3"
            if (version == "0.12" || version == "0.13") {
              val e = intercept[AnalysisException](versionSpark.sql(insertStmt)).getMessage
              assert(e.contains(errorMsg))
            } else {
              versionSpark.sql(insertStmt)
              assert(versionSpark.table(tableName).collect() ===
                versionSpark.sql("SELECT 1.30").collect())
            }
          }
        }
      }
    }

    test(s"$version: read avro file containing decimal") {
      val url = Thread.currentThread().getContextClassLoader.getResource("avroDecimal")
      val location = new File(url.getFile)

      val tableName = "tab1"
      val avroSchema =
        """{
          |  "name": "test_record",
          |  "type": "record",
          |  "fields": [ {
          |    "name": "f0",
          |    "type": [
          |      "null",
          |      {
          |        "precision": 38,
          |        "scale": 2,
          |        "type": "bytes",
          |        "logicalType": "decimal"
          |      }
          |    ]
          |  } ]
          |}
        """.stripMargin
      withTable(tableName) {
        versionSpark.sql(
          s"""
             |CREATE TABLE $tableName
             |ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
             |WITH SERDEPROPERTIES ('respectSparkSchema' = 'true')
             |STORED AS
             |  INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
             |  OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
             |LOCATION '$location'
             |TBLPROPERTIES ('avro.schema.literal' = '$avroSchema')
           """.stripMargin
        )
        assert(versionSpark.table(tableName).collect() ===
          versionSpark.sql("SELECT 1.30").collect())
      }
    }

    // TODO: add more tests.
=======
    test(s"$version: CREATE TABLE AS SELECT") {
      withTable("tbl") {
        sqlContext.sql("CREATE TABLE tbl AS SELECT 1 AS a")
        assert(sqlContext.table("tbl").collect().toSeq == Seq(Row(1)))
      }
    }

    test(s"$version: Delete the temporary staging directory and files after each insert") {
      withTempDir { tmpDir =>
        withTable("tab", "tbl") {
          sqlContext.sql(
            s"""
               |CREATE TABLE tab(c1 string)
               |location '${tmpDir.toURI.toString}'
             """.stripMargin)

          sqlContext.sql("CREATE TABLE tbl AS SELECT 1 AS a")
          sqlContext.sql(s"INSERT OVERWRITE TABLE tab SELECT * from tbl ")

          def listFiles(path: File): List[String] = {
            val dir = path.listFiles()
            val folders = dir.filter(_.isDirectory).toList
            val filePaths = dir.map(_.getName).toList
            folders.flatMap(listFiles) ++: filePaths
          }
          val expectedFiles = ".part-00000.crc" :: "part-00000" :: Nil
          assert(listFiles(tmpDir).sorted == expectedFiles)
        }
      }
    }

    test(s"$version: CREATE TABLE AS SELECT") {
      withTable("tbl") {
        sqlContext.sql("CREATE TABLE tbl AS SELECT 1 AS a")
        assert(sqlContext.table("tbl").collect().toSeq == Seq(Row(1)))
      }
    }

    test(s"$version: Delete the temporary staging directory and files after each insert") {
      withTempDir { tmpDir =>
        withTable("tab", "tbl") {
          sqlContext.sql(
            s"""
               |CREATE TABLE tab(c1 string)
               |location '${tmpDir.toURI.toString}'
             """.stripMargin)

          sqlContext.sql("CREATE TABLE tbl AS SELECT 1 AS a")
          sqlContext.sql(s"INSERT OVERWRITE TABLE tab SELECT * from tbl ")

          def listFiles(path: File): List[String] = {
            val dir = path.listFiles()
            val folders = dir.filter(_.isDirectory).toList
            val filePaths = dir.map(_.getName).toList
            folders.flatMap(listFiles) ++: filePaths
          }
          val expectedFiles = ".part-00000.crc" :: "part-00000" :: Nil
          assert(listFiles(tmpDir).sorted == expectedFiles)
        }
      }
    }
  }
}
