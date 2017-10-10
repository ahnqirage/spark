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

package org.apache.spark.repl

import java.io._
import java.net.URLClassLoader

import scala.collection.mutable.ArrayBuffer

import org.apache.log4j.{Level, LogManager}

import org.apache.spark.{SparkContext, SparkFunSuite}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION

/**
 * A special test suite for REPL that all test cases share one REPL instance.
 */
class SingletonReplSuite extends SparkFunSuite {

  private val out = new StringWriter()
  private val in = new PipedOutputStream()
  private var thread: Thread = _

  private val CONF_EXECUTOR_CLASSPATH = "spark.executor.extraClassPath"
  private val oldExecutorClasspath = System.getProperty(CONF_EXECUTOR_CLASSPATH)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val cl = getClass.getClassLoader
    var paths = new ArrayBuffer[String]
    if (cl.isInstanceOf[URLClassLoader]) {
      val urlLoader = cl.asInstanceOf[URLClassLoader]
      for (url <- urlLoader.getURLs) {
        if (url.getProtocol == "file") {
          paths += url.getFile
        }
      }
    }
    val classpath = paths.map(new File(_).getAbsolutePath).mkString(File.pathSeparator)

    System.setProperty(CONF_EXECUTOR_CLASSPATH, classpath)
    Main.conf.set("spark.master", "local-cluster[2,1,1024]")
    val interp = new SparkILoop(
      new BufferedReader(new InputStreamReader(new PipedInputStream(in))),
      new PrintWriter(out))

    // Forces to create new SparkContext
    Main.sparkContext = null
    Main.sparkSession = null

    // Starts a new thread to run the REPL interpreter, so that we won't block.
    thread = new Thread(new Runnable {
      override def run(): Unit = Main.doMain(Array("-classpath", classpath), interp)
    })
    thread.setDaemon(true)
    thread.start()

    waitUntil(() => out.toString.contains("Type :help for more information"))
  }

  override def afterAll(): Unit = {
    in.close()
    thread.join()
    if (oldExecutorClasspath != null) {
      System.setProperty(CONF_EXECUTOR_CLASSPATH, oldExecutorClasspath)
    } else {
      System.clearProperty(CONF_EXECUTOR_CLASSPATH)
    }
    super.afterAll()
  }

  private def waitUntil(cond: () => Boolean): Unit = {
    import scala.concurrent.duration._
    import org.scalatest.concurrent.Eventually._

    eventually(timeout(50.seconds), interval(500.millis)) {
      assert(cond(), "current output: " + out.toString)
    }
  }

  /**
   * Run the given commands string in a globally shared interpreter instance. Note that the given
   * commands should not crash the interpreter, to not affect other test cases.
   */
  def runInterpreter(input: String): String = {
    val currentOffset = out.getBuffer.length()
    // append a special statement to the end of the given code, so that we can know what's
    // the final output of this code snippet and rely on it to wait until the output is ready.
    val timestamp = System.currentTimeMillis()
    in.write((input + s"\nval _result_$timestamp = 1\n").getBytes)
    in.flush()
    val stopMessage = s"_result_$timestamp: Int = 1"
    waitUntil(() => out.getBuffer.substring(currentOffset).contains(stopMessage))
    out.getBuffer.substring(currentOffset)
  }

  def assertContains(message: String, output: String) {
    val isContain = output.contains(message)
    assert(isContain,
      "Interpreter output did not contain '" + message + "':\n" + output)
  }

  def assertDoesNotContain(message: String, output: String) {
    val isContain = output.contains(message)
    assert(!isContain,
      "Interpreter output contained '" + message + "':\n" + output)
  }

  test("propagation of local properties") {
    // A mock ILoop that doesn't install the SIGINT handler.
    class ILoop(out: PrintWriter) extends SparkILoop(None, out) {
      settings = new scala.tools.nsc.Settings
      settings.usejavacp.value = true
      org.apache.spark.repl.Main.interp = this
    }

    val out = new StringWriter()
    Main.interp = new ILoop(new PrintWriter(out))
    Main.sparkContext = new SparkContext("local", "repl-test")
    Main.interp.createInterpreter()

    Main.sparkContext.setLocalProperty("someKey", "someValue")

    // Make sure the value we set in the caller to interpret is propagated in the thread that
    // interprets the command.
    Main.interp.interpret("org.apache.spark.repl.Main.sparkContext.getLocalProperty(\"someKey\")")
    assert(out.toString.contains("someValue"))

    Main.sparkContext.stop()
    System.clearProperty("spark.driver.port")
  }

  test("SPARK-15236: use Hive catalog") {
    // turn on the INFO log so that it is possible the code will dump INFO
    // entry for using "HiveMetastore"
    val rootLogger = LogManager.getRootLogger()
    val logLevel = rootLogger.getLevel
    rootLogger.setLevel(Level.INFO)
    try {
      Main.conf.set(CATALOG_IMPLEMENTATION.key, "hive")
      val output = runInterpreter("local",
        """
      |spark.sql("drop table if exists t_15236")
    """.stripMargin)
      assertDoesNotContain("error:", output)
      assertDoesNotContain("Exception", output)
      // only when the config is set to hive and
      // hive classes are built, we will use hive catalog.
      // Then log INFO entry will show things using HiveMetastore
      if (SparkSession.hiveClassesArePresent) {
        assertContains("HiveMetaStore", output)
      } else {
        // If hive classes are not built, in-memory catalog will be used
        assertDoesNotContain("HiveMetaStore", output)
      }
    } finally {
      rootLogger.setLevel(logLevel)
    }
  }

  test("SPARK-15236: use in-memory catalog") {
    val rootLogger = LogManager.getRootLogger()
    val logLevel = rootLogger.getLevel
    rootLogger.setLevel(Level.INFO)
    try {
      Main.conf.set(CATALOG_IMPLEMENTATION.key, "in-memory")
      val output = runInterpreter("local",
        """
          |spark.sql("drop table if exists t_16236")
        """.stripMargin)
      assertDoesNotContain("error:", output)
      assertDoesNotContain("Exception", output)
      assertDoesNotContain("HiveMetaStore", output)
    } finally {
      rootLogger.setLevel(logLevel)
    }
  }

  test("broadcast vars") {
    // Test that the value that a broadcast var had when it was created is used,
    // even if that variable is then modified in the driver program
    val output = runInterpreter(
      """
        |var array = new Array[Int](5)
        |val broadcastArray = sc.broadcast(array)
        |val res1 = sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
        |array(0) = 5
        |val res2 = sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res1: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    assertContains("res2: Array[Int] = Array(0, 0, 0, 0, 0)", output)
  }

  if (System.getenv("MESOS_NATIVE_JAVA_LIBRARY") != null) {
    test("running on Mesos") {
      val output = runInterpreter("localquiet",
        """
          |var v = 7
          |def getV() = v
          |sc.parallelize(1 to 10).map(x => getV()).collect().reduceLeft(_+_)
          |v = 10
          |sc.parallelize(1 to 10).map(x => getV()).collect().reduceLeft(_+_)
          |var array = new Array[Int](5)
          |val broadcastArray = sc.broadcast(array)
          |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
          |array(0) = 5
          |sc.parallelize(0 to 4).map(x => broadcastArray.value(x)).collect()
        """.stripMargin)
      assertDoesNotContain("error:", output)
      assertDoesNotContain("Exception", output)
      assertContains("res0: Int = 70", output)
      assertContains("res1: Int = 100", output)
      assertContains("res2: Array[Int] = Array(0, 0, 0, 0, 0)", output)
      assertContains("res4: Array[Int] = Array(0, 0, 0, 0, 0)", output)
    }
  }

  test("line wrapper only initialized once when used as encoder outer scope") {
    val output = runInterpreter("local",
      """
        |val fileName = "repl-test-" + System.currentTimeMillis
        |val tmpDir = System.getProperty("java.io.tmpdir")
        |val file = new java.io.File(tmpDir, fileName)
        |def createFile(): Unit = file.createNewFile()
        |
        |createFile();case class TestCaseClass(value: Int)
        |sc.parallelize(1 to 10).map(x => TestCaseClass(x)).collect()
        |
        |file.delete()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
  }

  test("define case class and create Dataset together with paste mode") {
    val output = runInterpreterInPasteMode("local-cluster[1,1,1024]",
      """
        |import spark.implicits._
        |case class TestClass(value: Int)
        |Seq(TestClass(1)).toDS()
      """.stripMargin)
    assertDoesNotContain("error:", output)
    assertDoesNotContain("Exception", output)
    assertContains("res: Int = 20", output)
  }
}
