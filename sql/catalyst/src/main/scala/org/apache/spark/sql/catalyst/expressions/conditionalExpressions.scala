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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.codegen._
<<<<<<< HEAD
import org.apache.spark.sql.types._

// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "_FUNC_(expr1, expr2, expr3) - If `expr1` evaluates to true, then returns `expr2`; otherwise returns `expr3`.",
  examples = """
    Examples:
      > SELECT _FUNC_(1 < 2, 'a', 'b');
       a
  """)
// scalastyle:on line.size.limit
=======
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types._


>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
case class If(predicate: Expression, trueValue: Expression, falseValue: Expression)
  extends Expression {

  override def children: Seq[Expression] = predicate :: trueValue :: falseValue :: Nil
  override def nullable: Boolean = trueValue.nullable || falseValue.nullable

  override def checkInputDataTypes(): TypeCheckResult = {
    if (predicate.dataType != BooleanType) {
      TypeCheckResult.TypeCheckFailure(
        s"type of predicate expression in If should be boolean, not ${predicate.dataType}")
    } else if (!trueValue.dataType.sameType(falseValue.dataType)) {
      TypeCheckResult.TypeCheckFailure(s"differing types in '$sql' " +
        s"(${trueValue.dataType.simpleString} and ${falseValue.dataType.simpleString}).")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  override def dataType: DataType = trueValue.dataType

  override def eval(input: InternalRow): Any = {
    if (java.lang.Boolean.TRUE.equals(predicate.eval(input))) {
      trueValue.eval(input)
    } else {
      falseValue.eval(input)
    }
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val condEval = predicate.genCode(ctx)
    val trueEval = trueValue.genCode(ctx)
    val falseEval = falseValue.genCode(ctx)

    // place generated code of condition, true value and false value in separate methods if
    // their code combined is large
    val combinedLength = condEval.code.length + trueEval.code.length + falseEval.code.length
    val generatedCode = if (combinedLength > 1024 &&
      // Split these expressions only if they are created from a row object
      (ctx.INPUT_ROW != null && ctx.currentVars == null)) {

      val (condFuncName, condGlobalIsNull, condGlobalValue) =
        createAndAddFunction(ctx, condEval, predicate.dataType, "evalIfCondExpr")
      val (trueFuncName, trueGlobalIsNull, trueGlobalValue) =
        createAndAddFunction(ctx, trueEval, trueValue.dataType, "evalIfTrueExpr")
      val (falseFuncName, falseGlobalIsNull, falseGlobalValue) =
        createAndAddFunction(ctx, falseEval, falseValue.dataType, "evalIfFalseExpr")
      s"""
        $condFuncName(${ctx.INPUT_ROW});
        boolean ${ev.isNull} = false;
        ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        if (!$condGlobalIsNull && $condGlobalValue) {
          $trueFuncName(${ctx.INPUT_ROW});
          ${ev.isNull} = $trueGlobalIsNull;
          ${ev.value} = $trueGlobalValue;
        } else {
          $falseFuncName(${ctx.INPUT_ROW});
          ${ev.isNull} = $falseGlobalIsNull;
          ${ev.value} = $falseGlobalValue;
        }
      """
    }
    else {
      s"""
        ${condEval.code}
        boolean ${ev.isNull} = false;
        ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
        if (!${condEval.isNull} && ${condEval.value}) {
          ${trueEval.code}
          ${ev.isNull} = ${trueEval.isNull};
          ${ev.value} = ${trueEval.value};
        } else {
          ${falseEval.code}
          ${ev.isNull} = ${falseEval.isNull};
          ${ev.value} = ${falseEval.value};
        }
      """
    }

    ev.copy(code = generatedCode)
  }

  private def createAndAddFunction(
      ctx: CodegenContext,
      ev: ExprCode,
      dataType: DataType,
      baseFuncName: String): (String, String, String) = {
    val globalIsNull = ctx.freshName("isNull")
    ctx.addMutableState("boolean", globalIsNull, s"$globalIsNull = false;")
    val globalValue = ctx.freshName("value")
    ctx.addMutableState(ctx.javaType(dataType), globalValue,
      s"$globalValue = ${ctx.defaultValue(dataType)};")
    val funcName = ctx.freshName(baseFuncName)
    val funcBody =
      s"""
         |private void $funcName(InternalRow ${ctx.INPUT_ROW}) {
         |  ${ev.code.trim}
         |  $globalIsNull = ${ev.isNull};
         |  $globalValue = ${ev.value};
         |}
         """.stripMargin
    val fullFuncName = ctx.addNewFunction(funcName, funcBody)
    (fullFuncName, globalIsNull, globalValue)
  }

  override def toString: String = s"if ($predicate) $trueValue else $falseValue"

  override def sql: String = s"(IF(${predicate.sql}, ${trueValue.sql}, ${falseValue.sql}))"
}

/**
 * Abstract parent class for common logic in CaseWhen and CaseWhenCodegen.
 *
 * @param branches seq of (branch condition, branch value)
 * @param elseValue optional value for the else branch
 */
abstract class CaseWhenBase(
    branches: Seq[(Expression, Expression)],
    elseValue: Option[Expression])
  extends Expression with Serializable {

  override def children: Seq[Expression] = branches.flatMap(b => b._1 :: b._2 :: Nil) ++ elseValue

  // both then and else expressions should be considered.
  def valueTypes: Seq[DataType] = branches.map(_._2.dataType) ++ elseValue.map(_.dataType)

  def valueTypesEqual: Boolean = valueTypes.size <= 1 || valueTypes.sliding(2, 1).forall {
    case Seq(dt1, dt2) => dt1.sameType(dt2)
  }

  override def dataType: DataType = branches.head._2.dataType

  override def nullable: Boolean = {
    // Result is nullable if any of the branch is nullable, or if the else value is nullable
    branches.exists(_._2.nullable) || elseValue.map(_.nullable).getOrElse(true)
  }
<<<<<<< HEAD
=======

  /**
   * Whether should it fallback to interpret mode or not.
   * @return
   */
  protected def shouldFallback: Boolean = {
    branches.length > 20
  }

  /**
   * Whether should it fallback to interpret mode or not.
   * @return
   */
  protected def shouldFallback: Boolean = {
    branches.length > 20
  }
}

// scalastyle:off
/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * Refer to this link for the corresponding semantics:
 * https://cwiki.apache.org/confluence/display/Hive/LanguageManual+UDF#LanguageManualUDF-ConditionalFunctions
 */
// scalastyle:on
case class CaseWhen(branches: Seq[Expression]) extends CaseWhenLike with CodegenFallback {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray

  override def children: Seq[Expression] = branches
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

  override def checkInputDataTypes(): TypeCheckResult = {
    // Make sure all branch conditions are boolean types.
    if (valueTypesEqual) {
      if (branches.forall(_._1.dataType == BooleanType)) {
        TypeCheckResult.TypeCheckSuccess
      } else {
        val index = branches.indexWhere(_._1.dataType != BooleanType)
        TypeCheckResult.TypeCheckFailure(
          s"WHEN expressions in CaseWhen should all be boolean type, " +
            s"but the ${index + 1}th when expression's type is ${branches(index)._1}")
      }
    } else {
      TypeCheckResult.TypeCheckFailure(
        "THEN and ELSE expressions should all be same type or coercible to a common type")
    }
  }

  override def eval(input: InternalRow): Any = {
    var i = 0
    val size = branches.size
    while (i < size) {
      if (java.lang.Boolean.TRUE.equals(branches(i)._1.eval(input))) {
        return branches(i)._2.eval(input)
      }
      i += 1
    }
<<<<<<< HEAD
    if (elseValue.isDefined) {
      return elseValue.get.eval(input)
=======
    return res
  }

  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (shouldFallback) {
      // Fallback to interpreted mode if there are too many branches, as it may reach the
      // 64K limit (limit on bytecode size for a single function).
      return super[CodegenFallback].genCode(ctx, ev)
    }
    val len = branchesArr.length
    val got = ctx.freshName("got")

    val cases = (0 until len/2).map { i =>
      val cond = branchesArr(i * 2).gen(ctx)
      val res = branchesArr(i * 2 + 1).gen(ctx)
      s"""
        if (!$got) {
          ${cond.code}
          if (!${cond.isNull} && ${cond.value}) {
            $got = true;
            ${res.code}
            ${ev.isNull} = ${res.isNull};
            ${ev.value} = ${res.value};
          }
        }
      """
    }.mkString("\n")

    val other = if (len % 2 == 1) {
      val res = branchesArr(len - 1).gen(ctx)
      s"""
        if (!$got) {
          ${res.code}
          ${ev.isNull} = ${res.isNull};
          ${ev.value} = ${res.value};
        }
      """
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    } else {
      return null
    }
  }

  override def toString: String = {
    val cases = branches.map { case (c, v) => s" WHEN $c THEN $v" }.mkString
    val elseCase = elseValue.map(" ELSE " + _).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }

  override def sql: String = {
    val cases = branches.map { case (c, v) => s" WHEN ${c.sql} THEN ${v.sql}" }.mkString
    val elseCase = elseValue.map(" ELSE " + _.sql).getOrElse("")
    "CASE" + cases + elseCase + " END"
  }
}


/**
 * Case statements of the form "CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END".
 * When a = true, returns b; when c = true, returns d; else returns e.
 *
 * @param branches seq of (branch condition, branch value)
 * @param elseValue optional value for the else branch
 */
<<<<<<< HEAD
// scalastyle:off line.size.limit
@ExpressionDescription(
  usage = "CASE WHEN expr1 THEN expr2 [WHEN expr3 THEN expr4]* [ELSE expr5] END - When `expr1` = true, returns `expr2`; else when `expr3` = true, returns `expr4`; else returns `expr5`.",
  arguments = """
    Arguments:
      * expr1, expr3 - the branch condition expressions should all be boolean type.
      * expr2, expr4, expr5 - the branch value expressions and else value expression should all be
          same type or coercible to a common type.
  """,
  examples = """
    Examples:
      > SELECT CASE WHEN 1 > 0 THEN 1 WHEN 2 > 0 THEN 2.0 ELSE 1.2 END;
       1
      > SELECT CASE WHEN 1 < 0 THEN 1 WHEN 2 > 0 THEN 2.0 ELSE 1.2 END;
       2
      > SELECT CASE WHEN 1 < 0 THEN 1 WHEN 2 < 0 THEN 2.0 ELSE null END;
       NULL
  """)
// scalastyle:on line.size.limit
case class CaseWhen(
    val branches: Seq[(Expression, Expression)],
    val elseValue: Option[Expression] = None)
  extends CaseWhenBase(branches, elseValue) with CodegenFallback with Serializable {

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    super[CodegenFallback].doGenCode(ctx, ev)
=======
// scalastyle:on
case class CaseKeyWhen(key: Expression, branches: Seq[Expression])
  extends CaseWhenLike with CodegenFallback {

  // Use private[this] Array to speed up evaluation.
  @transient private[this] lazy val branchesArr = branches.toArray

  override def children: Seq[Expression] = key +: branches

  override protected def checkTypesInternal(): TypeCheckResult = {
    if ((key +: whenList).map(_.dataType).distinct.size > 1) {
      TypeCheckResult.TypeCheckFailure(
        "key and WHEN expressions should all be same type or coercible to a common type")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  private def evalElse(input: InternalRow): Any = {
    if (branchesArr.length % 2 == 0) {
      null
    } else {
      branchesArr(branchesArr.length - 1).eval(input)
    }
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }

  def toCodegen(): CaseWhenCodegen = {
    CaseWhenCodegen(branches, elseValue)
  }
}

<<<<<<< HEAD
/**
 * CaseWhen expression used when code generation condition is satisfied.
 * OptimizeCodegen optimizer replaces CaseWhen into CaseWhenCodegen.
 *
 * @param branches seq of (branch condition, branch value)
 * @param elseValue optional value for the else branch
 */
case class CaseWhenCodegen(
    val branches: Seq[(Expression, Expression)],
    val elseValue: Option[Expression] = None)
  extends CaseWhenBase(branches, elseValue) with Serializable {

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Generate code that looks like:
    //
    // condA = ...
    // if (condA) {
    //   valueA
    // } else {
    //   condB = ...
    //   if (condB) {
    //     valueB
    //   } else {
    //     condC = ...
    //     if (condC) {
    //       valueC
    //     } else {
    //       elseValue
    //     }
    //   }
    // }
    val cases = branches.map { case (condExpr, valueExpr) =>
      val cond = condExpr.genCode(ctx)
      val res = valueExpr.genCode(ctx)
=======
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    if (shouldFallback) {
      // Fallback to interpreted mode if there are too many branches, as it may reach the
      // 64K limit (limit on bytecode size for a single function).
      return super[CodegenFallback].genCode(ctx, ev)
    }
    val keyEval = key.gen(ctx)
    val len = branchesArr.length
    val got = ctx.freshName("got")

    val cases = (0 until len/2).map { i =>
      val cond = branchesArr(i * 2).gen(ctx)
      val res = branchesArr(i * 2 + 1).gen(ctx)
      s"""
        if (!$got) {
          ${cond.code}
          if (!${cond.isNull} && ${ctx.genEqual(key.dataType, keyEval.value, cond.value)}) {
            $got = true;
            ${res.code}
            ${ev.isNull} = ${res.isNull};
            ${ev.value} = ${res.value};
          }
        }
      """
    }.mkString("\n")

    val other = if (len % 2 == 1) {
      val res = branchesArr(len - 1).gen(ctx)
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      s"""
        ${cond.code}
        if (!${cond.isNull} && ${cond.value}) {
          ${res.code}
          ${ev.isNull} = ${res.isNull};
          ${ev.value} = ${res.value};
        }
      """
    }

    var generatedCode = cases.mkString("", "\nelse {\n", "\nelse {\n")

    elseValue.foreach { elseExpr =>
      val res = elseExpr.genCode(ctx)
      generatedCode +=
        s"""
          ${res.code}
          ${ev.isNull} = ${res.isNull};
          ${ev.value} = ${res.value};
        """
    }

    generatedCode += "}\n" * cases.size

<<<<<<< HEAD
    ev.copy(code = s"""
      boolean ${ev.isNull} = true;
      ${ctx.javaType(dataType)} ${ev.value} = ${ctx.defaultValue(dataType)};
      $generatedCode""")
=======
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val evalChildren = children.map(_.gen(ctx))
    val first = evalChildren(0)
    val rest = evalChildren.drop(1)
    def updateEval(eval: GeneratedExpressionCode): String =
      s"""
        ${eval.code}
        if (!${eval.isNull} && (${ev.isNull} ||
          ${ctx.genGreater(dataType, ev.value, eval.value)})) {
          ${ev.isNull} = false;
          ${ev.value} = ${eval.value};
        }
      """
    s"""
      ${first.code}
      boolean ${ev.isNull} = ${first.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${first.value};
      ${rest.map(updateEval).mkString("\n")}
    """
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}

/** Factory methods for CaseWhen. */
object CaseWhen {
  def apply(branches: Seq[(Expression, Expression)], elseValue: Expression): CaseWhen = {
    CaseWhen(branches, Option(elseValue))
  }

  /**
   * A factory method to facilitate the creation of this expression when used in parsers.
   *
   * @param branches Expressions at even position are the branch conditions, and expressions at odd
   *                 position are branch values.
   */
  def createFromParser(branches: Seq[Expression]): CaseWhen = {
    val cases = branches.grouped(2).flatMap {
      case cond :: value :: Nil => Some((cond, value))
      case value :: Nil => None
    }.toArray.toSeq  // force materialization to make the seq serializable
    val elseValue = if (branches.size % 2 == 1) Some(branches.last) else None
    CaseWhen(cases, elseValue)
  }
}

<<<<<<< HEAD
/**
 * Case statements of the form "CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END".
 * When a = b, returns c; when a = d, returns e; else returns f.
 */
object CaseKeyWhen {
  def apply(key: Expression, branches: Seq[Expression]): CaseWhen = {
    val cases = branches.grouped(2).flatMap {
      case Seq(cond, value) => Some((EqualTo(key, cond), value))
      case Seq(value) => None
    }.toArray.toSeq  // force materialization to make the seq serializable
    val elseValue = if (branches.size % 2 == 1) Some(branches.last) else None
    CaseWhen(cases, elseValue)
=======
  override def genCode(ctx: CodeGenContext, ev: GeneratedExpressionCode): String = {
    val evalChildren = children.map(_.gen(ctx))
    val first = evalChildren(0)
    val rest = evalChildren.drop(1)
    def updateEval(eval: GeneratedExpressionCode): String =
      s"""
        ${eval.code}
        if (!${eval.isNull} && (${ev.isNull} ||
          ${ctx.genGreater(dataType, eval.value, ev.value)})) {
          ${ev.isNull} = false;
          ${ev.value} = ${eval.value};
        }
      """
    s"""
      ${first.code}
      boolean ${ev.isNull} = ${first.isNull};
      ${ctx.javaType(dataType)} ${ev.value} = ${first.value};
      ${rest.map(updateEval).mkString("\n")}
    """
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  }
}

