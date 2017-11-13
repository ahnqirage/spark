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

package org.apache.spark.ml.feature

import org.apache.hadoop.fs.Path

import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.ml._
import org.apache.spark.ml.linalg._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util._
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.VersionUtils.majorVersion

/**
 * Params for [[PCA]] and [[PCAModel]].
 */
private[feature] trait PCAParams extends Params with HasInputCol with HasOutputCol {

  /**
   * The number of principal components.
   * @group param
   */
  final val k: IntParam = new IntParam(this, "k", "the number of principal components (> 0)",
    ParamValidators.gt(0))

  /** @group getParam */
  def getK: Int = $(k)

  /** Validates and transforms the input schema. */
  protected def validateAndTransformSchema(schema: StructType): StructType = {
    SchemaUtils.checkColumnType(schema, $(inputCol), new VectorUDT)
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
    StructType(outputFields)
  }

}

/**
 * PCA trains a model to project vectors to a lower dimensional space of the top `PCA!.k`
 * principal components.
 */
<<<<<<< HEAD
@Since("1.5.0")
class PCA @Since("1.5.0") (
    @Since("1.5.0") override val uid: String)
  extends Estimator[PCAModel] with PCAParams with DefaultParamsWritable {
=======
@Experimental
class PCA (override val uid: String) extends Estimator[PCAModel] with PCAParams
  with DefaultParamsWritable {

  @Since("1.5.0")
  def this() = this(Identifiable.randomUID("pca"))

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setK(value: Int): this.type = set(k, value)

  /**
   * Computes a [[PCAModel]] that contains the principal components of the input vectors.
   */
  @Since("2.0.0")
  override def fit(dataset: Dataset[_]): PCAModel = {
    transformSchema(dataset.schema, logging = true)
    val input: RDD[OldVector] = dataset.select($(inputCol)).rdd.map {
      case Row(v: Vector) => OldVectors.fromML(v)
    }
    val pca = new feature.PCA(k = $(k))
    val pcaModel = pca.fit(input)
    copyValues(new PCAModel(uid, pcaModel.pc).setParent(this))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): PCA = defaultCopy(extra)
}

@Since("1.6.0")
object PCA extends DefaultParamsReadable[PCA] {

  @Since("1.6.0")
  override def load(path: String): PCA = super.load(path)
}

/**
<<<<<<< HEAD
 * Model fitted by [[PCA]]. Transforms vectors to a lower dimensional space.
 *
 * @param pc A principal components Matrix. Each column is one principal component.
 * @param explainedVariance A vector of proportions of variance explained by
 *                          each principal component.
=======
 * :: Experimental ::
 * Model fitted by [[PCA]].
 *
 * @param pc A principal components Matrix. Each column is one principal component.
 */
@Since("1.5.0")
class PCAModel private[ml] (
<<<<<<< HEAD
    @Since("1.5.0") override val uid: String,
    @Since("2.0.0") val pc: DenseMatrix,
    @Since("2.0.0") val explainedVariance: DenseVector)
=======
    override val uid: String,
    val pc: DenseMatrix)
  extends Model[PCAModel] with PCAParams with MLWritable {

  import PCAModel._

  /** @group setParam */
  @Since("1.5.0")
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  @Since("1.5.0")
  def setOutputCol(value: String): this.type = set(outputCol, value)

  /**
   * Transform a vector by computed Principal Components.
   *
   * @note Vectors to be transformed must be the same length as the source vectors given
   * to `PCA.fit()`.
   */
  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val pcaModel = new feature.PCAModel($(k), pc)
    val pcaOp = udf { pcaModel.transform _ }
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    dataset.withColumn($(outputCol), pcaOp(col($(inputCol))))
  }

  @Since("1.5.0")
  override def transformSchema(schema: StructType): StructType = {
    validateAndTransformSchema(schema)
  }

  @Since("1.5.0")
  override def copy(extra: ParamMap): PCAModel = {
    val copied = new PCAModel(uid, pc)
    copyValues(copied, extra).setParent(parent)
  }

  @Since("1.6.0")
  override def write: MLWriter = new PCAModelWriter(this)
}

@Since("1.6.0")
object PCAModel extends MLReadable[PCAModel] {

  private[PCAModel] class PCAModelWriter(instance: PCAModel) extends MLWriter {

    private case class Data(pc: DenseMatrix)

    override protected def saveImpl(path: String): Unit = {
      DefaultParamsWriter.saveMetadata(instance, path, sc)
      val data = Data(instance.pc)
      val dataPath = new Path(path, "data").toString
      sqlContext.createDataFrame(Seq(data)).repartition(1).write.parquet(dataPath)
    }
  }

  private class PCAModelReader extends MLReader[PCAModel] {

    private val className = classOf[PCAModel].getName

    override def load(path: String): PCAModel = {
      val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
      val dataPath = new Path(path, "data").toString
      val Row(pc: DenseMatrix) = sqlContext.read.parquet(dataPath)
        .select("pc")
        .head()
      val model = new PCAModel(metadata.uid, pc)
      DefaultParamsReader.getAndSetParams(model, metadata)
      model
    }
  }

  @Since("1.6.0")
  override def read: MLReader[PCAModel] = new PCAModelReader

  @Since("1.6.0")
  override def load(path: String): PCAModel = super.load(path)
}
