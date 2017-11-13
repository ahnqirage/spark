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

import scala.collection.mutable

<<<<<<< HEAD
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.catalyst.expressions.objects.LambdaVariable

=======
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
/**
 * This class is used to compute equality of (sub)expression trees. Expressions can be added
 * to this class and they subsequently query for expression equality. Expression trees are
 * considered equal if for the same input(s), the same result is produced.
 */
class EquivalentExpressions {
  /**
   * Wrapper around an Expression that provides semantic equality.
   */
  case class Expr(e: Expression) {
    override def equals(o: Any): Boolean = o match {
      case other: Expr => e.semanticEquals(other.e)
      case _ => false
    }
<<<<<<< HEAD

    override def hashCode: Int = e.semanticHash()
=======
    override val hashCode: Int = e.semanticHash()
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  // For each expression, the set of equivalent expressions.
  private val equivalenceMap = mutable.HashMap.empty[Expr, mutable.MutableList[Expression]]

  /**
   * Adds each expression to this data structure, grouping them with existing equivalent
   * expressions. Non-recursive.
   * Returns true if there was already a matching expression.
   */
  def addExpr(expr: Expression): Boolean = {
    if (expr.deterministic) {
      val e: Expr = Expr(expr)
      val f = equivalenceMap.get(e)
      if (f.isDefined) {
        f.get += expr
        true
      } else {
        equivalenceMap.put(e, mutable.MutableList(expr))
        false
      }
    } else {
      false
    }
  }

  /**
   * Adds the expression to this data structure recursively. Stops if a matching expression
   * is found. That is, if `expr` has already been added, its children are not added.
<<<<<<< HEAD
   */
  def addExprTree(expr: Expression): Unit = {
    val skip = expr.isInstanceOf[LeafExpression] ||
      // `LambdaVariable` is usually used as a loop variable, which can't be evaluated ahead of the
      // loop. So we can't evaluate sub-expressions containing `LambdaVariable` at the beginning.
      expr.find(_.isInstanceOf[LambdaVariable]).isDefined

    // There are some special expressions that we should not recurse into all of its children.
    //   1. CodegenFallback: it's children will not be used to generate code (call eval() instead)
    //   2. If: common subexpressions will always be evaluated at the beginning, but the true and
    //          false expressions in `If` may not get accessed, according to the predicate
    //          expression. We should only recurse into the predicate expression.
    //   3. CaseWhen: like `If`, the children of `CaseWhen` only get accessed in a certain
    //                condition. We should only recurse into the first condition expression as it
    //                will always get accessed.
    //   4. Coalesce: it's also a conditional expression, we should only recurse into the first
    //                children, because others may not get accessed.
    def childrenToRecurse: Seq[Expression] = expr match {
      case _: CodegenFallback => Nil
      case i: If => i.predicate :: Nil
      // `CaseWhen` implements `CodegenFallback`, we only need to handle `CaseWhenCodegen` here.
      case c: CaseWhenCodegen => c.children.head :: Nil
      case c: Coalesce => c.children.head :: Nil
      case other => other.children
    }

    if (!skip && !addExpr(expr)) {
      childrenToRecurse.foreach(addExprTree)
=======
   * If ignoreLeaf is true, leaf nodes are ignored.
   */
  def addExprTree(root: Expression, ignoreLeaf: Boolean = true): Unit = {
    val skip = root.isInstanceOf[LeafExpression] && ignoreLeaf
    if (!skip && !addExpr(root)) {
      root.children.foreach(addExprTree(_, ignoreLeaf))
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    }
  }

  /**
   * Returns all of the expression trees that are equivalent to `e`. Returns
   * an empty collection if there are none.
   */
  def getEquivalentExprs(e: Expression): Seq[Expression] = {
    equivalenceMap.getOrElse(Expr(e), mutable.MutableList())
  }

  /**
   * Returns all the equivalent sets of expressions.
   */
  def getAllEquivalentExprs: Seq[Seq[Expression]] = {
    equivalenceMap.values.map(_.toSeq).toSeq
  }

  /**
   * Returns the state of the data structure as a string. If `all` is false, skips sets of
   * equivalent expressions with cardinality 1.
   */
  def debugString(all: Boolean = false): String = {
    val sb: mutable.StringBuilder = new StringBuilder()
    sb.append("Equivalent expressions:\n")
<<<<<<< HEAD
    equivalenceMap.foreach { case (k, v) =>
      if (all || v.length > 1) {
        sb.append("  " + v.mkString(", ")).append("\n")
      }
    }
=======
    equivalenceMap.foreach { case (k, v) => {
      if (all || v.length > 1) {
        sb.append("  " + v.mkString(", ")).append("\n")
      }
    }}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    sb.toString()
  }
}
