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

package org.apache.spark.ml.clustering

<<<<<<< HEAD
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.ml.util.TestingUtils._
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.sql._
=======
import org.apache.spark.SparkFunSuite
import org.apache.spark.ml.util.{DefaultReadWriteTest, MLTestingUtils}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.util.MLlibTestSparkContext
import org.apache.spark.mllib.util.TestingUtils._
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284


object LDASuite {
  def generateLDAData(
<<<<<<< HEAD
      spark: SparkSession,
=======
      sql: SQLContext,
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      rows: Int,
      k: Int,
      vocabSize: Int): DataFrame = {
    val avgWC = 1  // average instances of each word in a doc
<<<<<<< HEAD
    val sc = spark.sparkContext
=======
    val sc = sql.sparkContext
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    val rng = new java.util.Random()
    rng.setSeed(1)
    val rdd = sc.parallelize(1 to rows).map { i =>
      Vectors.dense(Array.fill(vocabSize)(rng.nextInt(2 * avgWC).toDouble))
    }.map(v => new TestRow(v))
<<<<<<< HEAD
    spark.createDataFrame(rdd)
=======
    sql.createDataFrame(rdd)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  /**
   * Mapping from all Params to valid settings which differ from the defaults.
   * This is useful for tests which need to exercise all Params, such as save/load.
   * This excludes input columns to simplify some tests.
   */
  val allParamSettings: Map[String, Any] = Map(
    "k" -> 3,
    "maxIter" -> 2,
    "checkpointInterval" -> 30,
    "learningOffset" -> 1023.0,
    "learningDecay" -> 0.52,
<<<<<<< HEAD
    "subsamplingRate" -> 0.051,
    "docConcentration" -> Array(2.0)
=======
    "subsamplingRate" -> 0.051
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  )
}


class LDASuite extends SparkFunSuite with MLlibTestSparkContext with DefaultReadWriteTest {

<<<<<<< HEAD
  import testImplicits._

  val k: Int = 5
  val vocabSize: Int = 30
  @transient var dataset: Dataset[_] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = LDASuite.generateLDAData(spark, 50, k, vocabSize)
=======
  val k: Int = 5
  val vocabSize: Int = 30
  @transient var dataset: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    dataset = LDASuite.generateLDAData(sqlContext, 50, k, vocabSize)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  test("default parameters") {
    val lda = new LDA()

    assert(lda.getFeaturesCol === "features")
    assert(lda.getMaxIter === 20)
    assert(lda.isDefined(lda.seed))
    assert(lda.getCheckpointInterval === 10)
    assert(lda.getK === 10)
    assert(!lda.isSet(lda.docConcentration))
    assert(!lda.isSet(lda.topicConcentration))
    assert(lda.getOptimizer === "online")
    assert(lda.getLearningDecay === 0.51)
    assert(lda.getLearningOffset === 1024)
    assert(lda.getSubsamplingRate === 0.05)
    assert(lda.getOptimizeDocConcentration)
    assert(lda.getTopicDistributionCol === "topicDistribution")
  }

  test("set parameters") {
    val lda = new LDA()
      .setFeaturesCol("test_feature")
      .setMaxIter(33)
      .setSeed(123)
      .setCheckpointInterval(7)
      .setK(9)
      .setTopicConcentration(0.56)
      .setTopicDistributionCol("myOutput")

    assert(lda.getFeaturesCol === "test_feature")
    assert(lda.getMaxIter === 33)
    assert(lda.getSeed === 123)
    assert(lda.getCheckpointInterval === 7)
    assert(lda.getK === 9)
    assert(lda.getTopicConcentration === 0.56)
    assert(lda.getTopicDistributionCol === "myOutput")


    // setOptimizer
    lda.setOptimizer("em")
    assert(lda.getOptimizer === "em")
    lda.setOptimizer("online")
    assert(lda.getOptimizer === "online")
    lda.setLearningDecay(0.53)
    assert(lda.getLearningDecay === 0.53)
    lda.setLearningOffset(1027)
    assert(lda.getLearningOffset === 1027)
    lda.setSubsamplingRate(0.06)
    assert(lda.getSubsamplingRate === 0.06)
    lda.setOptimizeDocConcentration(false)
    assert(!lda.getOptimizeDocConcentration)
  }

  test("parameters validation") {
    val lda = new LDA()

    // misc Params
    intercept[IllegalArgumentException] {
      new LDA().setK(1)
    }
    intercept[IllegalArgumentException] {
      new LDA().setOptimizer("no_such_optimizer")
    }
    intercept[IllegalArgumentException] {
      new LDA().setDocConcentration(-1.1)
    }
    intercept[IllegalArgumentException] {
      new LDA().setTopicConcentration(-1.1)
    }

<<<<<<< HEAD
    val dummyDF = Seq((1, Vectors.dense(1.0, 2.0))).toDF("id", "features")

    // validate parameters
    lda.transformSchema(dummyDF.schema)
    lda.setDocConcentration(1.1)
    lda.transformSchema(dummyDF.schema)
    lda.setDocConcentration(Range(0, lda.getK).map(_ + 2.0).toArray)
    lda.transformSchema(dummyDF.schema)
    lda.setDocConcentration(Range(0, lda.getK - 1).map(_ + 2.0).toArray)
    withClue("LDA docConcentration validity check failed for bad array length") {
      intercept[IllegalArgumentException] {
        lda.transformSchema(dummyDF.schema)
=======
    // validateParams()
    lda.validateParams()
    lda.setDocConcentration(1.1)
    lda.validateParams()
    lda.setDocConcentration(Range(0, lda.getK).map(_ + 2.0).toArray)
    lda.validateParams()
    lda.setDocConcentration(Range(0, lda.getK - 1).map(_ + 2.0).toArray)
    withClue("LDA docConcentration validity check failed for bad array length") {
      intercept[IllegalArgumentException] {
        lda.validateParams()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      }
    }

    // Online LDA
    intercept[IllegalArgumentException] {
      new LDA().setLearningOffset(0)
    }
    intercept[IllegalArgumentException] {
      new LDA().setLearningDecay(0)
    }
    intercept[IllegalArgumentException] {
      new LDA().setSubsamplingRate(0)
    }
    intercept[IllegalArgumentException] {
      new LDA().setSubsamplingRate(1.1)
    }
  }

  test("fit & transform with Online LDA") {
    val lda = new LDA().setK(k).setSeed(1).setOptimizer("online").setMaxIter(2)
    val model = lda.fit(dataset)

<<<<<<< HEAD
    MLTestingUtils.checkCopyAndUids(lda, model)
=======
    MLTestingUtils.checkCopy(model)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    assert(model.isInstanceOf[LocalLDAModel])
    assert(model.vocabSize === vocabSize)
    assert(model.estimatedDocConcentration.size === k)
    assert(model.topicsMatrix.numRows === vocabSize)
    assert(model.topicsMatrix.numCols === k)
    assert(!model.isDistributed)

    // transform()
    val transformed = model.transform(dataset)
    val expectedColumns = Array("features", lda.getTopicDistributionCol)
    expectedColumns.foreach { column =>
      assert(transformed.columns.contains(column))
    }
    transformed.select(lda.getTopicDistributionCol).collect().foreach { r =>
      val topicDistribution = r.getAs[Vector](0)
      assert(topicDistribution.size === k)
      assert(topicDistribution.toArray.forall(w => w >= 0.0 && w <= 1.0))
    }

    // logLikelihood, logPerplexity
    val ll = model.logLikelihood(dataset)
    assert(ll <= 0.0 && ll != Double.NegativeInfinity)
    val lp = model.logPerplexity(dataset)
    assert(lp >= 0.0 && lp != Double.PositiveInfinity)

    // describeTopics
    val topics = model.describeTopics(3)
    assert(topics.count() === k)
<<<<<<< HEAD
    assert(topics.select("topic").rdd.map(_.getInt(0)).collect().toSet === Range(0, k).toSet)
=======
    assert(topics.select("topic").map(_.getInt(0)).collect().toSet === Range(0, k).toSet)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    topics.select("termIndices").collect().foreach { case r: Row =>
      val termIndices = r.getAs[Seq[Int]](0)
      assert(termIndices.length === 3 && termIndices.toSet.size === 3)
    }
    topics.select("termWeights").collect().foreach { case r: Row =>
      val termWeights = r.getAs[Seq[Double]](0)
      assert(termWeights.length === 3 && termWeights.forall(w => w >= 0.0 && w <= 1.0))
    }
  }

  test("fit & transform with EM LDA") {
    val lda = new LDA().setK(k).setSeed(1).setOptimizer("em").setMaxIter(2)
    val model_ = lda.fit(dataset)

<<<<<<< HEAD
    MLTestingUtils.checkCopyAndUids(lda, model_)
=======
    MLTestingUtils.checkCopy(model_)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    assert(model_.isInstanceOf[DistributedLDAModel])
    val model = model_.asInstanceOf[DistributedLDAModel]
    assert(model.vocabSize === vocabSize)
    assert(model.estimatedDocConcentration.size === k)
    assert(model.topicsMatrix.numRows === vocabSize)
    assert(model.topicsMatrix.numCols === k)
    assert(model.isDistributed)

    val localModel = model.toLocal
    assert(localModel.isInstanceOf[LocalLDAModel])

    // training logLikelihood, logPrior
    val ll = model.trainingLogLikelihood
    assert(ll <= 0.0 && ll != Double.NegativeInfinity)
    val lp = model.logPrior
    assert(lp <= 0.0 && lp != Double.NegativeInfinity)
  }

  test("read/write LocalLDAModel") {
    def checkModelData(model: LDAModel, model2: LDAModel): Unit = {
      assert(model.vocabSize === model2.vocabSize)
      assert(Vectors.dense(model.topicsMatrix.toArray) ~==
        Vectors.dense(model2.topicsMatrix.toArray) absTol 1e-6)
      assert(Vectors.dense(model.getDocConcentration) ~==
        Vectors.dense(model2.getDocConcentration) absTol 1e-6)
    }
    val lda = new LDA()
<<<<<<< HEAD
    testEstimatorAndModelReadWrite(lda, dataset, LDASuite.allParamSettings,
      LDASuite.allParamSettings, checkModelData)
=======
    testEstimatorAndModelReadWrite(lda, dataset, LDASuite.allParamSettings, checkModelData)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  test("read/write DistributedLDAModel") {
    def checkModelData(model: LDAModel, model2: LDAModel): Unit = {
      assert(model.vocabSize === model2.vocabSize)
      assert(Vectors.dense(model.topicsMatrix.toArray) ~==
        Vectors.dense(model2.topicsMatrix.toArray) absTol 1e-6)
      assert(Vectors.dense(model.getDocConcentration) ~==
        Vectors.dense(model2.getDocConcentration) absTol 1e-6)
<<<<<<< HEAD
      val logPrior = model.asInstanceOf[DistributedLDAModel].logPrior
      val logPrior2 = model2.asInstanceOf[DistributedLDAModel].logPrior
      val trainingLogLikelihood =
        model.asInstanceOf[DistributedLDAModel].trainingLogLikelihood
      val trainingLogLikelihood2 =
        model2.asInstanceOf[DistributedLDAModel].trainingLogLikelihood
      assert(logPrior ~== logPrior2 absTol 1e-6)
      assert(trainingLogLikelihood ~== trainingLogLikelihood2 absTol 1e-6)
    }
    val lda = new LDA()
    testEstimatorAndModelReadWrite(lda, dataset,
      LDASuite.allParamSettings ++ Map("optimizer" -> "em"),
      LDASuite.allParamSettings ++ Map("optimizer" -> "em"), checkModelData)
  }

  test("EM LDA checkpointing: save last checkpoint") {
    // Checkpoint dir is set by MLlibTestSparkContext
    val lda = new LDA().setK(2).setSeed(1).setOptimizer("em").setMaxIter(3).setCheckpointInterval(1)
    val model_ = lda.fit(dataset)
    assert(model_.isInstanceOf[DistributedLDAModel])
    val model = model_.asInstanceOf[DistributedLDAModel]

    // There should be 1 checkpoint remaining.
    assert(model.getCheckpointFiles.length === 1)
    val checkpointFile = new Path(model.getCheckpointFiles.head)
    val fs = checkpointFile.getFileSystem(spark.sparkContext.hadoopConfiguration)
    assert(fs.exists(checkpointFile))
    model.deleteCheckpointFiles()
    assert(model.getCheckpointFiles.isEmpty)
  }

  test("EM LDA checkpointing: remove last checkpoint") {
    // Checkpoint dir is set by MLlibTestSparkContext
    val lda = new LDA().setK(2).setSeed(1).setOptimizer("em").setMaxIter(3).setCheckpointInterval(1)
      .setKeepLastCheckpoint(false)
    val model_ = lda.fit(dataset)
    assert(model_.isInstanceOf[DistributedLDAModel])
    val model = model_.asInstanceOf[DistributedLDAModel]

    assert(model.getCheckpointFiles.isEmpty)
  }

  test("EM LDA disable checkpointing") {
    // Checkpoint dir is set by MLlibTestSparkContext
    val lda = new LDA().setK(2).setSeed(1).setOptimizer("em").setMaxIter(3)
      .setCheckpointInterval(-1)
    val model_ = lda.fit(dataset)
    assert(model_.isInstanceOf[DistributedLDAModel])
    val model = model_.asInstanceOf[DistributedLDAModel]

    assert(model.getCheckpointFiles.isEmpty)
  }

  test("string params should be case-insensitive") {
    val lda = new LDA()
    Seq("eM", "oNLinE").foreach { optimizer =>
      lda.setOptimizer(optimizer)
      assert(lda.getOptimizer === optimizer)
      val model = lda.fit(dataset)
      assert(model.getOptimizer === optimizer)
    }
  }
=======
    }
    val lda = new LDA()
    testEstimatorAndModelReadWrite(lda, dataset,
      LDASuite.allParamSettings ++ Map("optimizer" -> "em"), checkModelData)
  }
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
}
