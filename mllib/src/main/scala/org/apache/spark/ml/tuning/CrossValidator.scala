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

package org.apache.spark.ml.tuning

<<<<<<< HEAD
import java.util.{List => JList}

import scala.collection.JavaConverters._
import scala.concurrent.Future
import scala.concurrent.duration.Duration

import org.apache.hadoop.fs.Path
import org.json4s.DefaultFormats

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.param.{IntParam, ParamMap, ParamValidators}
import org.apache.spark.ml.param.shared.HasParallelism
=======
import com.github.fommil.netlib.F2jBLAS
import org.apache.hadoop.fs.Path
import org.json4s.jackson.JsonMethods._
import org.json4s.{DefaultFormats, JObject}

import org.apache.spark.{Logging, SparkContext}
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml._
import org.apache.spark.ml.classification.OneVsRestParams
import org.apache.spark.ml.evaluation.Evaluator
import org.apache.spark.ml.feature.RFormulaModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.ml.util._
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils


/**
 * Params for [[CrossValidator]] and [[CrossValidatorModel]].
 */
private[ml] trait CrossValidatorParams extends ValidatorParams {
  /**
   * Param for number of folds for cross validation.  Must be &gt;= 2.
   * Default: 3
   *
   * @group param
   */
  val numFolds: IntParam = new IntParam(this, "numFolds",
    "number of folds for cross validation (>= 2)", ParamValidators.gtEq(2))

  /** @group getParam */
  def getNumFolds: Int = $(numFolds)

  setDefault(numFolds -> 3)
}

/**
 * K-fold cross validation performs model selection by splitting the dataset into a set of
 * non-overlapping randomly partitioned folds which are used as separate training and test datasets
 * e.g., with k=3 folds, K-fold cross validation will generate 3 (training, test) dataset pairs,
 * each of which uses 2/3 of the data for training and 1/3 for testing. Each fold is used as the
 * test set exactly once.
 */
@Since("1.2.0")
<<<<<<< HEAD
class CrossValidator @Since("1.2.0") (@Since("1.4.0") override val uid: String)
  extends Estimator[CrossValidatorModel]
  with CrossValidatorParams with HasParallelism with MLWritable with Logging {
=======
@Experimental
class CrossValidator @Since("1.2.0") (@Since("1.4.0") override val uid: String)
  extends Estimator[CrossValidatorModel]
  with CrossValidatorParams with MLWritable with Logging {
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

  @Since("1.2.0")
  def this() = this(Identifiable.randomUID("cv"))

  /** @group setParam */
  @Since("1.2.0")
  def setEstimator(value: Estimator[_]): this.type = set(estimator, value)

  /** @group setParam */
  @Since("1.2.0")
  def setEstimatorParamMaps(value: Array[ParamMap]): this.type = set(estimatorParamMaps, value)

  /** @group setParam */
  @Since("1.2.0")
  def setEvaluator(value: Evaluator): this.type = set(evaluator, value)

  /** @group setParam */
  @Since("1.2.0")
  def setNumFolds(value: Int): this.type = set(numFolds, value)

<<<<<<< HEAD
  /** @group setParam */
  @Since("2.0.0")
  def setSeed(value: Long): this.type = set(seed, value)

  /**
   * Set the mamixum level of parallelism to evaluate models in parallel.
   * Default is 1 for serial evaluation
   *
   * @group expertSetParam
   */
  @Since("2.3.0")
  def setParallelism(value: Int): this.type = set(parallelism, value)

  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): CrossValidatorModel = {
=======
  @Since("1.4.0")
  override def fit(dataset: DataFrame): CrossValidatorModel = {
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    val schema = dataset.schema
    transformSchema(schema, logging = true)
    val sparkSession = dataset.sparkSession
    val est = $(estimator)
    val eval = $(evaluator)
    val epm = $(estimatorParamMaps)

    // Create execution context based on $(parallelism)
    val executionContext = getExecutionContext

    val instr = Instrumentation.create(this, dataset)
    instr.logParams(numFolds, seed, parallelism)
    logTuningParams(instr)

    // Compute metrics for each model over each split
    val splits = MLUtils.kFold(dataset.toDF.rdd, $(numFolds), $(seed))
    val metrics = splits.zipWithIndex.map { case ((training, validation), splitIndex) =>
      val trainingDataset = sparkSession.createDataFrame(training, schema).cache()
      val validationDataset = sparkSession.createDataFrame(validation, schema).cache()
      logDebug(s"Train split $splitIndex with multiple sets of parameters.")

      // Fit models in a Future for training in parallel
      val modelFutures = epm.map { paramMap =>
        Future[Model[_]] {
          val model = est.fit(trainingDataset, paramMap)
          model.asInstanceOf[Model[_]]
        } (executionContext)
      }

      // Unpersist training data only when all models have trained
      Future.sequence[Model[_], Iterable](modelFutures)(implicitly, executionContext)
        .onComplete { _ => trainingDataset.unpersist() } (executionContext)

      // Evaluate models in a Future that will calulate a metric and allow model to be cleaned up
      val foldMetricFutures = modelFutures.zip(epm).map { case (modelFuture, paramMap) =>
        modelFuture.map { model =>
          // TODO: duplicate evaluator to take extra params from input
          val metric = eval.evaluate(model.transform(validationDataset, paramMap))
          logDebug(s"Got metric $metric for model trained with $paramMap.")
          metric
        } (executionContext)
      }

      // Wait for metrics to be calculated before unpersisting validation dataset
      val foldMetrics = foldMetricFutures.map(ThreadUtils.awaitResult(_, Duration.Inf))
      validationDataset.unpersist()
      foldMetrics
    }.transpose.map(_.sum / $(numFolds)) // Calculate average metric over all splits

    logInfo(s"Average cross-validation metrics: ${metrics.toSeq}")
    val (bestMetric, bestIndex) =
      if (eval.isLargerBetter) metrics.zipWithIndex.maxBy(_._1)
      else metrics.zipWithIndex.minBy(_._1)
    logInfo(s"Best set of parameters:\n${epm(bestIndex)}")
    logInfo(s"Best cross-validation metric: $bestMetric.")
    val bestModel = est.fit(dataset, epm(bestIndex)).asInstanceOf[Model[_]]
    instr.logSuccess(bestModel)
    copyValues(new CrossValidatorModel(uid, bestModel, metrics).setParent(this))
  }

  @Since("1.4.0")
<<<<<<< HEAD
  override def transformSchema(schema: StructType): StructType = transformSchemaImpl(schema)
=======
  override def transformSchema(schema: StructType): StructType = {
    $(estimator).transformSchema(schema)
  }

  @Since("1.4.0")
  override def validateParams(): Unit = {
    super.validateParams()
    val est = $(estimator)
    for (paramMap <- $(estimatorParamMaps)) {
      est.copy(paramMap).validateParams()
    }
  }
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

  @Since("1.4.0")
  override def copy(extra: ParamMap): CrossValidator = {
    val copied = defaultCopy(extra).asInstanceOf[CrossValidator]
    if (copied.isDefined(estimator)) {
      copied.setEstimator(copied.getEstimator.copy(extra))
    }
    if (copied.isDefined(evaluator)) {
      copied.setEvaluator(copied.getEvaluator.copy(extra))
    }
    copied
  }

  // Currently, this only works if all [[Param]]s in [[estimatorParamMaps]] are simple types.
  // E.g., this may fail if a [[Param]] is an instance of an [[Estimator]].
  // However, this case should be unusual.
  @Since("1.6.0")
  override def write: MLWriter = new CrossValidator.CrossValidatorWriter(this)
}

@Since("1.6.0")
object CrossValidator extends MLReadable[CrossValidator] {

  @Since("1.6.0")
  override def read: MLReader[CrossValidator] = new CrossValidatorReader

  @Since("1.6.0")
  override def load(path: String): CrossValidator = super.load(path)

  private[CrossValidator] class CrossValidatorWriter(instance: CrossValidator) extends MLWriter {

<<<<<<< HEAD
    ValidatorParams.validateParams(instance)

    override protected def saveImpl(path: String): Unit =
      ValidatorParams.saveImpl(path, instance, sc)
=======
    SharedReadWrite.validateParams(instance)

    override protected def saveImpl(path: String): Unit =
      SharedReadWrite.saveImpl(path, instance, sc)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  private class CrossValidatorReader extends MLReader[CrossValidator] {

    /** Checked against metadata when loading model */
    private val className = classOf[CrossValidator].getName

    override def load(path: String): CrossValidator = {
<<<<<<< HEAD
      implicit val format = DefaultFormats

      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val cv = new CrossValidator(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
      DefaultParamsReader.getAndSetParams(cv, metadata,
        skipParams = Option(List("estimatorParamMaps")))
      cv
=======
      val (metadata, estimator, evaluator, estimatorParamMaps, numFolds) =
        SharedReadWrite.load(path, sc, className)
      new CrossValidator(metadata.uid)
        .setEstimator(estimator)
        .setEvaluator(evaluator)
        .setEstimatorParamMaps(estimatorParamMaps)
        .setNumFolds(numFolds)
    }
  }

  private object CrossValidatorReader {
    /**
     * Examine the given estimator (which may be a compound estimator) and extract a mapping
     * from UIDs to corresponding [[Params]] instances.
     */
    def getUidMap(instance: Params): Map[String, Params] = {
      val uidList = getUidMapImpl(instance)
      val uidMap = uidList.toMap
      if (uidList.size != uidMap.size) {
        throw new RuntimeException("CrossValidator.load found a compound estimator with stages" +
          s" with duplicate UIDs.  List of UIDs: ${uidList.map(_._1).mkString(", ")}")
      }
      uidMap
    }

    def getUidMapImpl(instance: Params): List[(String, Params)] = {
      val subStages: Array[Params] = instance match {
        case p: Pipeline => p.getStages.asInstanceOf[Array[Params]]
        case pm: PipelineModel => pm.stages.asInstanceOf[Array[Params]]
        case v: ValidatorParams => Array(v.getEstimator, v.getEvaluator)
        case ovr: OneVsRestParams =>
          // TODO: SPARK-11892: This case may require special handling.
          throw new UnsupportedOperationException("CrossValidator write will fail because it" +
            " cannot yet handle an estimator containing type: ${ovr.getClass.getName}")
        case rform: RFormulaModel =>
          // TODO: SPARK-11891: This case may require special handling.
          throw new UnsupportedOperationException("CrossValidator write will fail because it" +
            " cannot yet handle an estimator containing an RFormulaModel")
        case _: Params => Array()
      }
      val subStageMaps = subStages.map(getUidMapImpl).foldLeft(List.empty[(String, Params)])(_ ++ _)
      List((instance.uid, instance)) ++ subStageMaps
    }
  }

  private[tuning] object SharedReadWrite {

    /**
     * Check that [[CrossValidator.evaluator]] and [[CrossValidator.estimator]] are Writable.
     * This does not check [[CrossValidator.estimatorParamMaps]].
     */
    def validateParams(instance: ValidatorParams): Unit = {
      def checkElement(elem: Params, name: String): Unit = elem match {
        case stage: MLWritable => // good
        case other =>
          throw new UnsupportedOperationException("CrossValidator write will fail " +
            s" because it contains $name which does not implement Writable." +
            s" Non-Writable $name: ${other.uid} of type ${other.getClass}")
      }
      checkElement(instance.getEvaluator, "evaluator")
      checkElement(instance.getEstimator, "estimator")
      // Check to make sure all Params apply to this estimator.  Throw an error if any do not.
      // Extraneous Params would cause problems when loading the estimatorParamMaps.
      val uidToInstance: Map[String, Params] = CrossValidatorReader.getUidMap(instance)
      instance.getEstimatorParamMaps.foreach { case pMap: ParamMap =>
        pMap.toSeq.foreach { case ParamPair(p, v) =>
          require(uidToInstance.contains(p.parent), s"CrossValidator save requires all Params in" +
            s" estimatorParamMaps to apply to this CrossValidator, its Estimator, or its" +
            s" Evaluator.  An extraneous Param was found: $p")
        }
      }
    }

    private[tuning] def saveImpl(
        path: String,
        instance: CrossValidatorParams,
        sc: SparkContext,
        extraMetadata: Option[JObject] = None): Unit = {
      import org.json4s.JsonDSL._

      val estimatorParamMapsJson = compact(render(
        instance.getEstimatorParamMaps.map { case paramMap =>
          paramMap.toSeq.map { case ParamPair(p, v) =>
            Map("parent" -> p.parent, "name" -> p.name, "value" -> p.jsonEncode(v))
          }
        }.toSeq
      ))
      val jsonParams = List(
        "numFolds" -> parse(instance.numFolds.jsonEncode(instance.getNumFolds)),
        "estimatorParamMaps" -> parse(estimatorParamMapsJson)
      )
      DefaultParamsWriter.saveMetadata(instance, path, sc, extraMetadata, Some(jsonParams))

      val evaluatorPath = new Path(path, "evaluator").toString
      instance.getEvaluator.asInstanceOf[MLWritable].save(evaluatorPath)
      val estimatorPath = new Path(path, "estimator").toString
      instance.getEstimator.asInstanceOf[MLWritable].save(estimatorPath)
    }

    private[tuning] def load[M <: Model[M]](
        path: String,
        sc: SparkContext,
        expectedClassName: String): (Metadata, Estimator[M], Evaluator, Array[ParamMap], Int) = {

      val metadata = DefaultParamsReader.loadMetadata(path, sc, expectedClassName)

      implicit val format = DefaultFormats
      val evaluatorPath = new Path(path, "evaluator").toString
      val evaluator = DefaultParamsReader.loadParamsInstance[Evaluator](evaluatorPath, sc)
      val estimatorPath = new Path(path, "estimator").toString
      val estimator = DefaultParamsReader.loadParamsInstance[Estimator[M]](estimatorPath, sc)

      val uidToParams = Map(evaluator.uid -> evaluator) ++ CrossValidatorReader.getUidMap(estimator)

      val numFolds = (metadata.params \ "numFolds").extract[Int]
      val estimatorParamMaps: Array[ParamMap] =
        (metadata.params \ "estimatorParamMaps").extract[Seq[Seq[Map[String, String]]]].map {
          pMap =>
            val paramPairs = pMap.map { case pInfo: Map[String, String] =>
              val est = uidToParams(pInfo("parent"))
              val param = est.getParam(pInfo("name"))
              val value = param.jsonDecode(pInfo("value"))
              param -> value
            }
            ParamMap(paramPairs: _*)
        }.toArray
      (metadata, estimator, evaluator, estimatorParamMaps, numFolds)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    }
  }
}

/**
 * CrossValidatorModel contains the model with the highest average cross-validation
 * metric across folds and uses this model to transform input data. CrossValidatorModel
 * also tracks the metrics for each param map evaluated.
 *
 * @param bestModel The best model selected from k-fold cross validation.
 * @param avgMetrics Average cross-validation metrics for each paramMap in
<<<<<<< HEAD
 *                   `CrossValidator.estimatorParamMaps`, in the corresponding order.
 */
@Since("1.2.0")
=======
 *                   [[CrossValidator.estimatorParamMaps]], in the corresponding order.
 */
@Since("1.2.0")
@Experimental
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
class CrossValidatorModel private[ml] (
    @Since("1.4.0") override val uid: String,
    @Since("1.2.0") val bestModel: Model[_],
    @Since("1.5.0") val avgMetrics: Array[Double])
  extends Model[CrossValidatorModel] with CrossValidatorParams with MLWritable {

<<<<<<< HEAD
  /** A Python-friendly auxiliary constructor. */
  private[ml] def this(uid: String, bestModel: Model[_], avgMetrics: JList[Double]) = {
    this(uid, bestModel, avgMetrics.asScala.toArray)
  }

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
=======
  @Since("1.4.0")
  override def validateParams(): Unit = {
    bestModel.validateParams()
  }

  @Since("1.4.0")
  override def transform(dataset: DataFrame): DataFrame = {
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    transformSchema(dataset.schema, logging = true)
    bestModel.transform(dataset)
  }

  @Since("1.4.0")
  override def transformSchema(schema: StructType): StructType = {
    bestModel.transformSchema(schema)
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): CrossValidatorModel = {
    val copied = new CrossValidatorModel(
      uid,
      bestModel.copy(extra).asInstanceOf[Model[_]],
      avgMetrics.clone())
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new CrossValidatorModel.CrossValidatorModelWriter(this)
}

@Since("1.6.0")
object CrossValidatorModel extends MLReadable[CrossValidatorModel] {

<<<<<<< HEAD
=======
  import CrossValidator.SharedReadWrite

>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  @Since("1.6.0")
  override def read: MLReader[CrossValidatorModel] = new CrossValidatorModelReader

  @Since("1.6.0")
  override def load(path: String): CrossValidatorModel = super.load(path)

  private[CrossValidatorModel]
  class CrossValidatorModelWriter(instance: CrossValidatorModel) extends MLWriter {

<<<<<<< HEAD
    ValidatorParams.validateParams(instance)
=======
    SharedReadWrite.validateParams(instance)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

    override protected def saveImpl(path: String): Unit = {
      import org.json4s.JsonDSL._
      val extraMetadata = "avgMetrics" -> instance.avgMetrics.toSeq
<<<<<<< HEAD
      ValidatorParams.saveImpl(path, instance, sc, Some(extraMetadata))
=======
      SharedReadWrite.saveImpl(path, instance, sc, Some(extraMetadata))
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      val bestModelPath = new Path(path, "bestModel").toString
      instance.bestModel.asInstanceOf[MLWritable].save(bestModelPath)
    }
  }

  private class CrossValidatorModelReader extends MLReader[CrossValidatorModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[CrossValidatorModel].getName

    override def load(path: String): CrossValidatorModel = {
      implicit val format = DefaultFormats

<<<<<<< HEAD
      val (metadata, estimator, evaluator, estimatorParamMaps) =
        ValidatorParams.loadImpl(path, sc, className)
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val avgMetrics = (metadata.metadata \ "avgMetrics").extract[Seq[Double]].toArray

      val model = new CrossValidatorModel(metadata.uid, bestModel, avgMetrics)
      model.set(model.estimator, estimator)
        .set(model.evaluator, evaluator)
        .set(model.estimatorParamMaps, estimatorParamMaps)
      DefaultParamsReader.getAndSetParams(model, metadata,
        skipParams = Option(List("estimatorParamMaps")))
      model
=======
      val (metadata, estimator, evaluator, estimatorParamMaps, numFolds) =
        SharedReadWrite.load(path, sc, className)
      val bestModelPath = new Path(path, "bestModel").toString
      val bestModel = DefaultParamsReader.loadParamsInstance[Model[_]](bestModelPath, sc)
      val avgMetrics = (metadata.metadata \ "avgMetrics").extract[Seq[Double]].toArray
      val cv = new CrossValidatorModel(metadata.uid, bestModel, avgMetrics)
      cv.set(cv.estimator, estimator)
        .set(cv.evaluator, evaluator)
        .set(cv.estimatorParamMaps, estimatorParamMaps)
        .set(cv.numFolds, numFolds)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    }
  }
}
