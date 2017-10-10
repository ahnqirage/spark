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

package org.apache.spark.sql.catalyst.expressions

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.types._

/**
 * An interface for expressions that contain a [[QueryPlan]].
 */
abstract class PlanExpression[T <: QueryPlan[_]] extends Expression {
  /**  The id of the subquery expression. */
  def exprId: ExprId

  /** The plan being wrapped in the query. */
  def plan: T

  /** Updates the expression with a new plan. */
  def withNewPlan(plan: T): PlanExpression[T]

  protected def conditionString: String = children.mkString("[", " && ", "]")
}

object SubqueryExpression {
  def hasCorrelatedSubquery(e: Expression): Boolean = {
    e.find {
      case e: SubqueryExpression if e.children.nonEmpty => true
      case _ => false
    }.isDefined
  }
}

/**
 * A base interface for expressions that contain a [[LogicalPlan]].
 */
case class ScalarSubquery(
    query: LogicalPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression with Unevaluable {
  override lazy val resolved: Boolean = childrenResolved && query.resolved
  override lazy val references: AttributeSet = {
    if (query.resolved) super.references -- query.outputSet
    else super.references
  }
  override def dataType: DataType = query.schema.fields.head.dataType
  override def foldable: Boolean = false
  override def nullable: Boolean = true
  override def plan: LogicalPlan = SubqueryAlias(toString, query)
  override def withNewPlan(plan: LogicalPlan): ScalarSubquery = copy(query = plan)
  override def toString: String = s"scalar-subquery#${exprId.id} $conditionString"
}

object ScalarSubquery {
  def hasCorrelatedScalarSubquery(e: Expression): Boolean = {
    e.find {
      case e: ScalarSubquery if e.children.nonEmpty => true
      case _ => false
    }.isDefined
  }
}

/**
 * A subquery that will return only one row and one column. This will be converted into a physical
 * scalar subquery during planning.
 *
 * Note: `exprId` is used to have a unique name in explain string output.
 */
case class ScalarSubquery(
    plan: LogicalPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression with Predicate with Unevaluable {
  override lazy val resolved = childrenResolved && query.resolved
  override lazy val references: AttributeSet = super.references -- query.outputSet
  override def nullable: Boolean = nullAware
  override def plan: LogicalPlan = SubqueryAlias(toString, query)
  override def withNewPlan(plan: LogicalPlan): PredicateSubquery = copy(query = plan)
  override def toString: String = s"predicate-subquery#${exprId.id} $conditionString"
}

object ScalarSubquery {
  def hasCorrelatedScalarSubquery(e: Expression): Boolean = {
    e.find {
      case s: ScalarSubquery => s.children.nonEmpty
      case _ => false
    }.isDefined
  }

  /**
   * Returns whether there are any null-aware predicate subqueries inside Not. If not, we could
   * turn the null-aware predicate into not-null-aware predicate.
   */
  def hasNullAwarePredicateWithinNot(e: Expression): Boolean = {
    e.find{ x =>
      x.isInstanceOf[Not] && e.find {
        case p: PredicateSubquery => p.nullAware
        case _ => false
      }.isDefined
    }.isDefined
  }
}

/**
 * A [[ListQuery]] expression defines the query which we want to search in an IN subquery
 * expression. It should and can only be used in conjunction with an IN expression.
 *
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   a.id IN (SELECT  id
 *                    FROM    b)
 * }}}
 */
case class ListQuery(
    plan: LogicalPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId,
    childOutputs: Seq[Attribute] = Seq.empty)
  extends SubqueryExpression(plan, children, exprId) with Unevaluable {
  override def dataType: DataType = if (childOutputs.length > 1) {
    childOutputs.toStructType
  } else {
    childOutputs.head.dataType
  }
  override lazy val resolved: Boolean = childrenResolved && plan.resolved && childOutputs.nonEmpty
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): ListQuery = copy(plan = plan)
  override def toString: String = s"list#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    ListQuery(
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0),
      childOutputs.map(_.canonicalized.asInstanceOf[Attribute]))
  }
}

/**
 * The [[Exists]] expression checks if a row exists in a subquery given some correlated condition.
 *
 * For example (SQL):
 * {{{
 *   SELECT  *
 *   FROM    a
 *   WHERE   EXISTS (SELECT  *
 *                   FROM    b
 *                   WHERE   b.id = a.id)
 * }}}
 */
case class Exists(
    plan: LogicalPlan,
    children: Seq[Expression] = Seq.empty,
    exprId: ExprId = NamedExpression.newExprId)
  extends SubqueryExpression(plan, children, exprId) with Predicate with Unevaluable {
  override def nullable: Boolean = false
  override def withNewPlan(plan: LogicalPlan): Exists = copy(plan = plan)
  override def toString: String = s"exists#${exprId.id} $conditionString"
  override lazy val canonicalized: Expression = {
    Exists(
      plan.canonicalized,
      children.map(_.canonicalized),
      ExprId(0))
  }
}
