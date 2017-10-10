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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{Analyzer, FunctionRegistry}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.optimizer.Optimizer
import org.apache.spark.sql.catalyst.parser.ParserInterface
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.AnalyzeTableCommand
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryManager}
import org.apache.spark.sql.util.ExecutionListenerManager


/**
 * A class that holds all session-specific state in a given [[SparkSession]].
 *
 * @param sharedState The state shared across sessions, e.g. global view manager, external catalog.
 * @param conf SQL-specific key-value configurations.
 * @param experimentalMethods Interface to add custom planning strategies and optimizers.
 * @param functionRegistry Internal catalog for managing functions registered by the user.
 * @param udfRegistration Interface exposed to the user for registering user-defined functions.
 * @param catalogBuilder a function to create an internal catalog for managing table and database
 *                       states.
 * @param sqlParser Parser that extracts expressions, plans, table identifiers etc. from SQL texts.
 * @param analyzerBuilder A function to create the logical query plan analyzer for resolving
 *                        unresolved attributes and relations.
 * @param optimizerBuilder a function to create the logical query plan optimizer.
 * @param planner Planner that converts optimized logical plans to physical plans.
 * @param streamingQueryManager Interface to start and stop streaming queries.
 * @param listenerManager Interface to register custom [[QueryExecutionListener]]s.
 * @param resourceLoaderBuilder a function to create a session shared resource loader to load JARs,
 *                              files, etc.
 * @param createQueryExecution Function used to create QueryExecution objects.
 * @param createClone Function used to create clones of the session state.
 */
private[sql] class SessionState(
    sharedState: SharedState,
    val conf: SQLConf,
    val experimentalMethods: ExperimentalMethods,
    val functionRegistry: FunctionRegistry,
    val udfRegistration: UDFRegistration,
    catalogBuilder: () => SessionCatalog,
    val sqlParser: ParserInterface,
    analyzerBuilder: () => Analyzer,
    optimizerBuilder: () => Optimizer,
    val planner: SparkPlanner,
    val streamingQueryManager: StreamingQueryManager,
    val listenerManager: ExecutionListenerManager,
    resourceLoaderBuilder: () => SessionResourceLoader,
    createQueryExecution: LogicalPlan => QueryExecution,
    createClone: (SparkSession, SessionState) => SessionState) {

  // The following fields are lazy to avoid creating the Hive client when creating SessionState.
  lazy val catalog: SessionCatalog = catalogBuilder()

  lazy val analyzer: Analyzer = analyzerBuilder()

  lazy val optimizer: Optimizer = optimizerBuilder()

  lazy val resourceLoader: SessionResourceLoader = resourceLoaderBuilder()

  def newHadoopConf(): Configuration = SessionState.newHadoopConf(
    sharedState.sparkContext.hadoopConfiguration,
    conf)

  def newHadoopConfWithOptions(options: Map[String, String]): Configuration = {
    val hadoopConf = newHadoopConf()
    options.foreach { case (k, v) =>
      if ((v ne null) && k != "path" && k != "paths") {
        hadoopConf.set(k, v)
      }
    }
    hadoopConf
  }

  lazy val experimentalMethods = new ExperimentalMethods

  /**
   * Internal catalog for managing functions registered by the user.
   */
  lazy val functionRegistry: FunctionRegistry = FunctionRegistry.builtin.copy()

  /**
   * A class for loading resources specified by a function.
   */
  lazy val functionResourceLoader: FunctionResourceLoader = {
    new FunctionResourceLoader {
      override def loadResource(resource: FunctionResource): Unit = {
        resource.resourceType match {
          case JarResource => addJar(resource.uri)
          case FileResource => sparkSession.sparkContext.addFile(resource.uri)
          case ArchiveResource =>
            throw new AnalysisException(
              "Archive is not allowed to be loaded. If YARN mode is used, " +
                "please use --archives options while calling spark-submit.")
        }
      }
    }
  }

  /**
   * Internal catalog for managing table and database states.
   */
  lazy val catalog = new SessionCatalog(
    sparkSession.sharedState.externalCatalog,
    functionResourceLoader,
    functionRegistry,
    conf,
    newHadoopConf())

  /**
   * Interface exposed to the user for registering user-defined functions.
   * Note that the user-defined functions must be deterministic.
   */
  lazy val udf: UDFRegistration = new UDFRegistration(functionRegistry)

  /**
   * Logical query plan analyzer for resolving unresolved attributes and relations.
   */
  lazy val analyzer: Analyzer = {
    new Analyzer(catalog, conf) {
      override val extendedResolutionRules =
        AnalyzeCreateTableAsSelect(sparkSession) ::
        PreprocessTableInsertion(conf) ::
        new FindDataSourceTable(sparkSession) ::
        DataSourceAnalysis(conf) ::
        (if (conf.runSQLonFile) new ResolveDataSource(sparkSession) :: Nil else Nil)

      override val extendedCheckRules = Seq(datasources.PreWriteCheck(conf, catalog))
    }
  }

  /**
   * Logical query plan optimizer.
   */
  lazy val optimizer: Optimizer = new SparkOptimizer(catalog, conf, experimentalMethods)

  /**
   * Get an identical copy of the `SessionState` and associate it with the given `SparkSession`
   */
  lazy val sqlParser: ParserInterface = new SparkSqlParser(conf)

  /**
   * Planner that converts optimized logical plans to physical plans.
   */
  def planner: SparkPlanner =
    new SparkPlanner(sparkSession.sparkContext, conf, experimentalMethods.extraStrategies)

  /**
   * An interface to register custom [[org.apache.spark.sql.util.QueryExecutionListener]]s
   * that listen for execution metrics.
   */
  lazy val listenerManager: ExecutionListenerManager = new ExecutionListenerManager

  /**
   * Interface to start and stop [[StreamingQuery]]s.
   */
  lazy val streamingQueryManager: StreamingQueryManager = {
    new StreamingQueryManager(sparkSession)
  }

  private val jarClassLoader: NonClosableMutableURLClassLoader =
    sparkSession.sharedState.jarClassLoader

  // Automatically extract all entries and put it in our SQLConf
  // We need to call it after all of vals have been initialized.
  sparkSession.sparkContext.getConf.getAll.foreach { case (k, v) =>
    conf.setConfString(k, v)
  }

  // ------------------------------------------------------
  //  Helper methods, partially leftover from pre-2.0 days
  // ------------------------------------------------------

  def executeSql(sql: String): QueryExecution = executePlan(sqlParser.parsePlan(sql))

  def executePlan(plan: LogicalPlan): QueryExecution = new QueryExecution(sparkSession, plan)

  def refreshTable(tableName: String): Unit = {
    catalog.refreshTable(sqlParser.parseTableIdentifier(tableName))
  }
}

  def addJar(path: String): Unit = {
    sparkSession.sparkContext.addJar(path)

  /**
   * Add a jar path to [[SparkContext]] and the classloader.
   *
   * Note: this method seems not access any session state, but a Hive based `SessionState` needs
   * to add the jar to its hive client for the current session. Hence, it still needs to be in
   * [[SessionState]].
   */
  def addJar(path: String): Unit = {
    session.sparkContext.addJar(path)
    val uri = new Path(path).toUri
    val jarURL = if (uri.getScheme == null) {
      // `path` is a local file path without a URL scheme
      new File(path).toURI.toURL
    } else {
      // `path` is a URL with a scheme
      uri.toURL
    }
    jarClassLoader.addURL(jarURL)
    Thread.currentThread().setContextClassLoader(jarClassLoader)
  }

  /**
   * Analyzes the given table in the current database to generate statistics, which will be
   * used in query optimizations.
   *
   * Right now, it only supports catalog tables and it only updates the size of a catalog table
   * in the external catalog.
   */
  def analyze(tableName: String): Unit = {
    AnalyzeTableCommand(tableName).run(sparkSession)
  }
}
