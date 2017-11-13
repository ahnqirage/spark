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
package org.apache.spark.sql.execution.datasources

<<<<<<< HEAD
import org.apache.spark.sql.catalyst.analysis.MultiInstanceRelation
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{AttributeMap, AttributeReference}
import org.apache.spark.sql.catalyst.plans.QueryPlan
=======
import org.apache.spark.sql.catalyst.analysis.{EliminateSubQueries, MultiInstanceRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeMap, AttributeReference}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.util.Utils

/**
 * Used to link a [[BaseRelation]] in to a logical query plan.
 */
case class LogicalRelation(
    relation: BaseRelation,
    output: Seq[AttributeReference],
    catalogTable: Option[CatalogTable],
    override val isStreaming: Boolean)
  extends LeafNode with MultiInstanceRelation {

  // Logical Relations are distinct if they have different output for the sake of transformations.
  override def equals(other: Any): Boolean = other match {
    case l @ LogicalRelation(otherRelation, _, _, isStreaming) =>
      relation == otherRelation && output == l.output && isStreaming == l.isStreaming
    case _ => false
  }

  override def hashCode: Int = {
    com.google.common.base.Objects.hashCode(relation, output)
  }

<<<<<<< HEAD
  // Only care about relation when canonicalizing.
  override lazy val canonicalized: LogicalPlan = copy(
    output = output.map(QueryPlan.normalizeExprId(_, output)),
    catalogTable = None)
=======
  override def sameResult(otherPlan: LogicalPlan): Boolean = {
    EliminateSubQueries(otherPlan) match {
      case LogicalRelation(otherRelation, _) => relation == otherRelation
      case _ => false
    }
  }

  // When comparing two LogicalRelations from within LogicalPlan.sameResult, we only need
  // LogicalRelation.cleanArgs to return Seq(relation), since expectedOutputAttribute's
  // expId can be different but the relation is still the same.
  override lazy val cleanArgs: Seq[Any] = Seq(relation)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

  override def computeStats(): Statistics = {
    catalogTable
      .flatMap(_.stats.map(_.toPlanStats(output)))
      .getOrElse(Statistics(sizeInBytes = relation.sizeInBytes))
  }

  /** Used to lookup original attribute capitalization */
  val attributeMap: AttributeMap[AttributeReference] = AttributeMap(output.map(o => (o, o)))

  /**
   * Returns a new instance of this LogicalRelation. According to the semantics of
   * MultiInstanceRelation, this method returns a copy of this object with
   * unique expression ids. We respect the `expectedOutputAttributes` and create
   * new instances of attributes in it.
   */
  override def newInstance(): LogicalRelation = {
    this.copy(output = output.map(_.newInstance()))
  }

  override def refresh(): Unit = relation match {
    case fs: HadoopFsRelation => fs.location.refresh()
    case _ =>  // Do nothing.
  }

  override def simpleString: String = s"Relation[${Utils.truncatedString(output, ",")}] $relation"
}

object LogicalRelation {
  def apply(relation: BaseRelation, isStreaming: Boolean = false): LogicalRelation =
    LogicalRelation(relation, relation.schema.toAttributes, None, isStreaming)

  def apply(relation: BaseRelation, table: CatalogTable): LogicalRelation =
    LogicalRelation(relation, relation.schema.toAttributes, Some(table), false)
}
