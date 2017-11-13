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

package org.apache.spark.ml.classification

import com.github.fommil.netlib.BLAS.{getInstance => blas}
<<<<<<< HEAD
import org.json4s.{DefaultFormats, JObject}
import org.json4s.JsonDSL._

import org.apache.spark.annotation.Since
import org.apache.spark.internal.Logging
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}
import org.apache.spark.ml.param.ParamMap
=======

import org.apache.spark.Logging
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml.{PredictionModel, Predictor}
import org.apache.spark.ml.param.{Param, ParamMap}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.ml.regression.DecisionTreeRegressionModel
import org.apache.spark.ml.tree._
import org.apache.spark.ml.tree.impl.GradientBoostedTrees
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.DefaultParamsReader.Metadata
import org.apache.spark.mllib.tree.configuration.{Algo => OldAlgo}
import org.apache.spark.mllib.tree.model.{GradientBoostedTreesModel => OldGBTModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._

/**
 * Gradient-Boosted Trees (GBTs) (http://en.wikipedia.org/wiki/Gradient_boosting)
 * learning algorithm for classification.
 * It supports binary labels, as well as both continuous and categorical features.
 *
 * The implementation is based upon: J.H. Friedman. "Stochastic Gradient Boosting." 1999.
 *
 * Notes on Gradient Boosting vs. TreeBoost:
 *  - This implementation is for Stochastic Gradient Boosting, not for TreeBoost.
 *  - Both algorithms learn tree ensembles by minimizing loss functions.
 *  - TreeBoost (Friedman, 1999) additionally modifies the outputs at tree leaf nodes
 *    based on the loss function, whereas the original gradient boosting method does not.
 *  - We expect to implement TreeBoost in the future:
 *    [https://issues.apache.org/jira/browse/SPARK-4240]
 *
 * @note Multiclass labels are not currently supported.
 */
@Since("1.4.0")
@Experimental
final class GBTClassifier @Since("1.4.0") (
    @Since("1.4.0") override val uid: String)
  extends Predictor[Vector, GBTClassifier, GBTClassificationModel]
  with GBTParams with TreeClassifierParams with Logging {
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

  @Since("1.4.0")
  def this() = this(Identifiable.randomUID("gbtc"))

  // Override parameter setters from parent trait for Java API compatibility.

  // Parameters from TreeClassifierParams:

  @Since("1.4.0")
  override def setMaxDepth(value: Int): this.type = super.setMaxDepth(value)

  @Since("1.4.0")
  override def setMaxBins(value: Int): this.type = super.setMaxBins(value)

  @Since("1.4.0")
  override def setMinInstancesPerNode(value: Int): this.type =
    super.setMinInstancesPerNode(value)

  @Since("1.4.0")
  override def setMinInfoGain(value: Double): this.type = super.setMinInfoGain(value)

  @Since("1.4.0")
  override def setMaxMemoryInMB(value: Int): this.type = super.setMaxMemoryInMB(value)

  @Since("1.4.0")
  override def setCacheNodeIds(value: Boolean): this.type = super.setCacheNodeIds(value)

  @Since("1.4.0")
  override def setCheckpointInterval(value: Int): this.type = super.setCheckpointInterval(value)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

  /**
   * The impurity setting is ignored for GBT models.
   * Individual trees are built using impurity "Variance."
   *
   * @group setParam
   */
  @Since("1.4.0")
  override def setImpurity(value: String): this.type = {
    logWarning("GBTClassifier.setImpurity should NOT be used")
    this
  }

  // Parameters from TreeEnsembleParams:

  @Since("1.4.0")
  override def setSubsamplingRate(value: Double): this.type = super.setSubsamplingRate(value)

  @Since("1.4.0")
  override def setSeed(value: Long): this.type = {
    logWarning("The 'seed' parameter is currently ignored by Gradient Boosting.")
    super.setSeed(value)
  }

  // Parameters from GBTParams:

  @Since("1.4.0")
  override def setMaxIter(value: Int): this.type = super.setMaxIter(value)

  @Since("1.4.0")
  override def setStepSize(value: Double): this.type = super.setStepSize(value)

  // Parameters for GBTClassifier:

  /**
   * Loss function which GBT tries to minimize. (case-insensitive)
   * Supported: "logistic"
   * (default = logistic)
   * @group param
   */
  @Since("1.4.0")
  val lossType: Param[String] = new Param[String](this, "lossType", "Loss function which GBT" +
    " tries to minimize (case-insensitive). Supported options:" +
    s" ${GBTClassifier.supportedLossTypes.mkString(", ")}",
    (value: String) => GBTClassifier.supportedLossTypes.contains(value.toLowerCase))
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

  // Parameters from GBTClassifierParams:

  /** @group setParam */
  @Since("1.4.0")
  def setLossType(value: String): this.type = set(lossType, value)

<<<<<<< HEAD
  override protected def train(dataset: Dataset[_]): GBTClassificationModel = {
=======
  /** @group getParam */
  @Since("1.4.0")
  def getLossType: String = $(lossType).toLowerCase

  /** (private[ml]) Convert new loss to old loss. */
  override private[ml] def getOldLossType: OldLoss = {
    getLossType match {
      case "logistic" => OldLogLoss
      case _ =>
        // Should never happen because of check in setter method.
        throw new RuntimeException(s"GBTClassifier was given bad loss type: $getLossType")
    }
  }

  override protected def train(dataset: DataFrame): GBTClassificationModel = {
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    val categoricalFeatures: Map[Int, Int] =
      MetadataUtils.getCategoricalFeatures(dataset.schema($(featuresCol)))
    // We copy and modify this from Classifier.extractLabeledPoints since GBT only supports
    // 2 classes now.  This lets us provide a more precise error message.
    val oldDataset: RDD[LabeledPoint] =
      dataset.select(col($(labelCol)), col($(featuresCol))).rdd.map {
        case Row(label: Double, features: Vector) =>
          require(label == 0 || label == 1, s"GBTClassifier was given" +
            s" dataset with invalid label $label.  Labels must be in {0,1}; note that" +
            s" GBTClassifier currently only supports binary classification.")
          LabeledPoint(label, features)
      }
    val numFeatures = oldDataset.first().features.size
    val boostingStrategy = super.getOldBoostingStrategy(categoricalFeatures, OldAlgo.Classification)

    val numClasses = 2
    if (isDefined(thresholds)) {
      require($(thresholds).length == numClasses, this.getClass.getSimpleName +
        ".train() called with non-matching numClasses and thresholds.length." +
        s" numClasses=$numClasses, but thresholds has length ${$(thresholds).length}")
    }

    val instr = Instrumentation.create(this, oldDataset)
    instr.logParams(labelCol, featuresCol, predictionCol, impurity, lossType,
      maxDepth, maxBins, maxIter, maxMemoryInMB, minInfoGain, minInstancesPerNode,
      seed, stepSize, subsamplingRate, cacheNodeIds, checkpointInterval)
    instr.logNumFeatures(numFeatures)
    instr.logNumClasses(numClasses)

    val (baseLearners, learnerWeights) = GradientBoostedTrees.run(oldDataset, boostingStrategy,
      $(seed))
    val m = new GBTClassificationModel(uid, baseLearners, learnerWeights, numFeatures)
    instr.logSuccess(m)
    m
  }

  @Since("1.4.1")
  override def copy(extra: ParamMap): GBTClassifier = defaultCopy(extra)
}

@Since("1.4.0")
@Experimental
object GBTClassifier {
  // The losses below should be lowercase.
  /** Accessor for supported loss settings: logistic */
  @Since("1.4.0")
  final val supportedLossTypes: Array[String] = Array("logistic").map(_.toLowerCase)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
}

/**
 * Gradient-Boosted Trees (GBTs) (http://en.wikipedia.org/wiki/Gradient_boosting)
 * model for classification.
 * It supports binary labels, as well as both continuous and categorical features.
 *
 * @param _trees  Decision trees in the ensemble.
 * @param _treeWeights  Weights for the decision trees in the ensemble.
 *
 * @note Multiclass labels are not currently supported.
 */
@Since("1.6.0")
@Experimental
final class GBTClassificationModel private[ml](
    @Since("1.6.0") override val uid: String,
    private val _trees: Array[DecisionTreeRegressionModel],
    private val _treeWeights: Array[Double],
    @Since("1.6.0") override val numFeatures: Int)
  extends PredictionModel[Vector, GBTClassificationModel]
  with TreeEnsembleModel with Serializable {
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

  require(_trees.nonEmpty, "GBTClassificationModel requires at least 1 tree.")
  require(_trees.length == _treeWeights.length, "GBTClassificationModel given trees, treeWeights" +
    s" of non-matching lengths (${_trees.length}, ${_treeWeights.length}, respectively).")

  /**
   * Construct a GBTClassificationModel
   *
   * @param _trees  Decision trees in the ensemble.
   * @param _treeWeights  Weights for the decision trees in the ensemble.
   * @param numFeatures  The number of features.
   */
  @Since("1.6.0")
  def this(uid: String, _trees: Array[DecisionTreeRegressionModel], _treeWeights: Array[Double]) =
    this(uid, _trees, _treeWeights, -1, 2)

  @Since("1.4.0")
<<<<<<< HEAD
  override def trees: Array[DecisionTreeRegressionModel] = _trees

  /**
   * Number of trees in ensemble
   */
  @Since("2.0.0")
  val getNumTrees: Int = trees.length

  @Since("1.4.0")
  override def trees: Array[DecisionTreeModel] = _trees.asInstanceOf[Array[DecisionTreeModel]]

  @Since("1.4.0")
  override def treeWeights: Array[Double] = _treeWeights

  override protected def transformImpl(dataset: Dataset[_]): DataFrame = {
    val bcastModel = dataset.sparkSession.sparkContext.broadcast(this)
    val predictUDF = udf { (features: Any) =>
      bcastModel.value.predict(features.asInstanceOf[Vector])
    }
    dataset.withColumn($(predictionCol), predictUDF(col($(featuresCol))))
  }

  override protected def predict(features: Vector): Double = {
    // If thresholds defined, use predictRaw to get probabilities, otherwise use optimization
    if (isDefined(thresholds)) {
      super.predict(features)
    } else {
      if (margin(features) > 0.0) 1.0 else 0.0
    }
  }

  @Since("1.4.0")
  override def copy(extra: ParamMap): GBTClassificationModel = {
    copyValues(new GBTClassificationModel(uid, _trees, _treeWeights, numFeatures, numClasses),
      extra).setParent(parent)
  }

  @Since("1.4.0")
  override def toString: String = {
    s"GBTClassificationModel (uid=$uid) with $numTrees trees"
  }

  /**
   * Estimate of the importance of each feature.
   *
   * Each feature's importance is the average of its importance across all trees in the ensemble
   * The importance vector is normalized to sum to 1. This method is suggested by Hastie et al.
   * (Hastie, Tibshirani, Friedman. "The Elements of Statistical Learning, 2nd Edition." 2001.)
   * and follows the implementation from scikit-learn.

   * See `DecisionTreeClassificationModel.featureImportances`
   */
  @Since("2.0.0")
  lazy val featureImportances: Vector = TreeEnsembleModel.featureImportances(trees, numFeatures)

  /** Raw prediction for the positive class. */
  private def margin(features: Vector): Double = {
    val treePredictions = _trees.map(_.rootNode.predictImpl(features).prediction)
    blas.ddot(numTrees, treePredictions, 1, _treeWeights, 1)
  }

  /** (private[ml]) Convert to a model in the old API */
  private[ml] def toOld: OldGBTModel = {
    new OldGBTModel(OldAlgo.Classification, _trees.map(_.toOld), _treeWeights)
  }

  // hard coded loss, which is not meant to be changed in the model
  private val loss = getOldLossType

  @Since("2.0.0")
  override def write: MLWriter = new GBTClassificationModel.GBTClassificationModelWriter(this)
}

@Since("2.0.0")
object GBTClassificationModel extends MLReadable[GBTClassificationModel] {

  private val numFeaturesKey: String = "numFeatures"
  private val numTreesKey: String = "numTrees"

  @Since("2.0.0")
  override def read: MLReader[GBTClassificationModel] = new GBTClassificationModelReader

  @Since("2.0.0")
  override def load(path: String): GBTClassificationModel = super.load(path)

  private[GBTClassificationModel]
  class GBTClassificationModelWriter(instance: GBTClassificationModel) extends MLWriter {

    override protected def saveImpl(path: String): Unit = {

      val extraMetadata: JObject = Map(
        numFeaturesKey -> instance.numFeatures,
        numTreesKey -> instance.getNumTrees)
      EnsembleModelReadWrite.saveImpl(instance, path, sparkSession, extraMetadata)
    }
  }

  private class GBTClassificationModelReader extends MLReader[GBTClassificationModel] {

    /** Checked against metadata when loading model */
    private val className = classOf[GBTClassificationModel].getName
    private val treeClassName = classOf[DecisionTreeRegressionModel].getName

    override def load(path: String): GBTClassificationModel = {
      implicit val format = DefaultFormats
      val (metadata: Metadata, treesData: Array[(Metadata, Node)], treeWeights: Array[Double]) =
        EnsembleModelReadWrite.loadImpl(path, sparkSession, className, treeClassName)
      val numFeatures = (metadata.metadata \ numFeaturesKey).extract[Int]
      val numTrees = (metadata.metadata \ numTreesKey).extract[Int]

      val trees: Array[DecisionTreeRegressionModel] = treesData.map {
        case (treeMetadata, root) =>
          val tree =
            new DecisionTreeRegressionModel(treeMetadata.uid, root, numFeatures)
          DefaultParamsReader.getAndSetParams(tree, treeMetadata)
          tree
      }
      require(numTrees == trees.length, s"GBTClassificationModel.load expected $numTrees" +
        s" trees based on metadata but found ${trees.length} trees.")
      val model = new GBTClassificationModel(metadata.uid,
        trees, treeWeights, numFeatures)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  /** Convert a model from the old API */
  private[ml] def fromOld(
      oldModel: OldGBTModel,
      parent: GBTClassifier,
      categoricalFeatures: Map[Int, Int],
      numFeatures: Int = -1,
      numClasses: Int = 2): GBTClassificationModel = {
    require(oldModel.algo == OldAlgo.Classification, "Cannot convert GradientBoostedTreesModel" +
      s" with algo=${oldModel.algo} (old API) to GBTClassificationModel (new API).")
    val newTrees = oldModel.trees.map { tree =>
      // parent for each tree is null since there is no good way to set this.
      DecisionTreeRegressionModel.fromOld(tree, null, categoricalFeatures)
    }
    val uid = if (parent != null) parent.uid else Identifiable.randomUID("gbtc")
    new GBTClassificationModel(uid, newTrees, oldModel.treeWeights, numFeatures, numClasses)
  }
}
