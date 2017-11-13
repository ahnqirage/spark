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

package org.apache.spark.sql.execution

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{execution, AnalysisException, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical._
import org.apache.spark.sql.execution.columnar.{InMemoryColumnarTableScan, InMemoryRelation}
import org.apache.spark.sql.execution.datasources.{CreateTableUsing, CreateTempTableUsing, DescribeCommand => LogicalDescribeCommand, _}
import org.apache.spark.sql.execution.{DescribeCommand => RunnableDescribeCommand}
import org.apache.spark.sql.{Strategy, execution}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

  override protected def planLater(plan: LogicalPlan): SparkPlan = PlanLater(plan)
}

case class PlanLater(plan: LogicalPlan) extends LeafExecNode {

  override def output: Seq[Attribute] = plan.output

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
}

abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SparkPlanner =>

  /**
   * Plans special cases of limit operators.
   */
  object SpecialLimits extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ReturnAnswer(rootPlan) => rootPlan match {
        case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
          TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
        case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
          TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
        case Limit(IntegerLiteral(limit), child) =>
          // With whole stage codegen, Spark releases resources only when all the output data of the
          // query plan are consumed. It's possible that `CollectLimitExec` only consumes a little
          // data from child plan and finishes the query without releasing resources. Here we wrap
          // the child plan with `LocalLimitExec`, to stop the processing of whole stage codegen and
          // trigger the resource releasing work, after we consume `limit` rows.
          CollectLimitExec(limit, LocalLimitExec(limit, planLater(child))) :: Nil
        case other => planLater(other) :: Nil
      }
      case Limit(IntegerLiteral(limit), Sort(order, true, child)) =>
        TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
      case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child))) =>
        TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
      case _ => Nil
    }
  }

  /**
   * Select the proper physical plan for join based on joining keys and size of logical plan.
   *
   * At first, uses the [[ExtractEquiJoinKeys]] pattern to find joins where at least some of the
   * predicates can be evaluated by matching join keys. If found,  Join implementations are chosen
   * with the following precedence:
   *
   * - Broadcast: if one side of the join has an estimated physical size that is smaller than the
   *     user-configurable [[SQLConf.AUTO_BROADCASTJOIN_THRESHOLD]] threshold
   *     or if that side has an explicit broadcast hint (e.g. the user applied the
   *     [[org.apache.spark.sql.functions.broadcast()]] function to a DataFrame), then that side
   *     of the join will be broadcasted and the other side will be streamed, with no shuffling
   *     performed. If both sides of the join are eligible to be broadcasted then the
   * - Sort merge: if the matching join keys are sortable.
   */
  object JoinSelection extends Strategy with PredicateHelper {

    /**
     * Matches a plan whose output should be small enough to be used in broadcast join.
     */
    private def canBroadcast(plan: LogicalPlan): Boolean = {
      plan.stats.hints.broadcast ||
        (plan.stats.sizeInBytes >= 0 &&
          plan.stats.sizeInBytes <= conf.autoBroadcastJoinThreshold)
    }

    /**
     * Matches a plan whose single partition should be small enough to build a hash table.
     *
     * Note: this assume that the number of partition is fixed, requires additional work if it's
     * dynamic.
     */
    private def canBuildLocalHashMap(plan: LogicalPlan): Boolean = {
      plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
    }

    /**
     * Returns whether plan a is much smaller (3X) than plan b.
     *
     * The cost to build hash map is higher than sorting, we should only build hash map on a table
     * that is much smaller than other one. Since we does not have the statistic for number of rows,
     * use the size of bytes here as estimation.
     */
    private def muchSmaller(a: LogicalPlan, b: LogicalPlan): Boolean = {
      a.stats.sizeInBytes * 3 <= b.stats.sizeInBytes
    }

    private def canBuildRight(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti => true
      case j: ExistenceJoin => true
      case _ => false
    }

    private def canBuildLeft(joinType: JoinType): Boolean = joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }

<<<<<<< HEAD
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      // --- BroadcastHashJoin --------------------------------------------------------------------

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildRight(joinType) && canBroadcast(right) =>
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if canBuildLeft(joinType) && canBroadcast(left) =>
        Seq(joins.BroadcastHashJoinExec(
          leftKeys, rightKeys, joinType, BuildLeft, condition, planLater(left), planLater(right)))
=======
      case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right)
        if RowOrdering.isOrderable(leftKeys) =>
        val mergeJoin =
          joins.SortMergeJoin(leftKeys, rightKeys, planLater(left), planLater(right))
        condition.map(Filter(_, mergeJoin)).getOrElse(mergeJoin) :: Nil

      // --- Outer joins --------------------------------------------------------------------------
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284

      // --- ShuffledHashJoin ---------------------------------------------------------------------

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
         if !conf.preferSortMergeJoin && canBuildRight(joinType) && canBuildLocalHashMap(right)
           && muchSmaller(right, left) ||
           !RowOrdering.isOrderable(leftKeys) =>
        Seq(joins.ShuffledHashJoinExec(
          leftKeys, rightKeys, joinType, BuildRight, condition, planLater(left), planLater(right)))

      case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
        if RowOrdering.isOrderable(leftKeys) =>
        joins.SortMergeOuterJoin(
          leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

      // --- Cases where this strategy does not apply ---------------------------------------------

      case _ => Nil
    }
  }

  /**
   * Used to plan the streaming deduplicate operator.
   */
<<<<<<< HEAD
  object StreamingDeduplicationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case Deduplicate(keys, child) if child.isStreaming =>
        StreamingDeduplicateExec(keys, planLater(child)) :: Nil
=======
  object Aggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.Aggregate(groupingExpressions, resultExpressions, child) =>
        // A single aggregate expression might appear multiple times in resultExpressions.
        // In order to avoid evaluating an individual aggregate function multiple times, we'll
        // build a set of the distinct aggregate expressions and build a function which can
        // be used to re-write expressions so that they reference the single copy of the
        // aggregate function which actually gets computed.
        val aggregateExpressions = resultExpressions.flatMap { expr =>
          expr.collect {
            case agg: AggregateExpression => agg
          }
        }.distinct
        // For those distinct aggregate expressions, we create a map from the
        // aggregate function to the corresponding attribute of the function.
        val aggregateFunctionToAttribute = aggregateExpressions.map { agg =>
          val aggregateFunction = agg.aggregateFunction
          val attribute = Alias(aggregateFunction, aggregateFunction.toString)().toAttribute
          (aggregateFunction, agg.isDistinct) -> attribute
        }.toMap

        val (functionsWithDistinct, functionsWithoutDistinct) =
          aggregateExpressions.partition(_.isDistinct)
        if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our MultipleDistinctRewriter should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
            "Spark user mailing list.")
        }

        val namedGroupingExpressions = groupingExpressions.map {
          case ne: NamedExpression => ne -> ne
          // If the expression is not a NamedExpressions, we add an alias.
          // So, when we generate the result of the operator, the Aggregate Operator
          // can directly get the Seq of attributes representing the grouping expressions.
          case other =>
            val withAlias = Alias(other, other.toString)()
            other -> withAlias
        }
        val groupExpressionMap = namedGroupingExpressions.toMap

        // The original `resultExpressions` are a set of expressions which may reference
        // aggregate expressions, grouping column values, and constants. When aggregate operator
        // emits output rows, we will use `resultExpressions` to generate an output projection
        // which takes the grouping columns and final aggregate result buffer as input.
        // Thus, we must re-write the result expressions so that their attributes match up with
        // the attributes of the final result projection's input row:
        val rewrittenResultExpressions = resultExpressions.map { expr =>
          expr.transformDown {
            case AggregateExpression(aggregateFunction, _, isDistinct) =>
              // The final aggregation buffer's attributes will be `finalAggregationAttributes`,
              // so replace each aggregate expression by its corresponding attribute in the set:
              aggregateFunctionToAttribute(aggregateFunction, isDistinct)
            case expression =>
              // Since we're using `namedGroupingAttributes` to extract the grouping key
              // columns, we need to replace grouping key expressions with their corresponding
              // attributes. We do not rely on the equality check at here since attributes may
              // differ cosmetically. Instead, we use semanticEquals.
              groupExpressionMap.collectFirst {
                case (expr, ne) if expr semanticEquals expression => ne.toAttribute
              }.getOrElse(expression)
          }.asInstanceOf[NamedExpression]
        }

        val aggregateOperator =
          if (aggregateExpressions.map(_.aggregateFunction).exists(!_.supportsPartial)) {
            if (functionsWithDistinct.nonEmpty) {
              sys.error("Distinct columns cannot exist in Aggregate operator containing " +
                "aggregate functions which don't support partial aggregation.")
            } else {
              aggregate.Utils.planAggregateWithoutPartial(
                namedGroupingExpressions.map(_._2),
                aggregateExpressions,
                aggregateFunctionToAttribute,
                rewrittenResultExpressions,
                planLater(child))
            }
          } else if (functionsWithDistinct.isEmpty) {
            aggregate.Utils.planAggregateWithoutDistinct(
              namedGroupingExpressions.map(_._2),
              aggregateExpressions,
              aggregateFunctionToAttribute,
              rewrittenResultExpressions,
              planLater(child))
          } else {
            aggregate.Utils.planAggregateWithOneDistinct(
              namedGroupingExpressions.map(_._2),
              functionsWithDistinct,
              functionsWithoutDistinct,
              aggregateFunctionToAttribute,
              rewrittenResultExpressions,
              planLater(child))
          }

        aggregateOperator

      case _ => Nil
    }
  }

  object StreamingJoinStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, condition, left, right)
          if left.isStreaming && right.isStreaming =>

          new StreamingSymmetricHashJoinExec(
            leftKeys, rightKeys, joinType, condition, planLater(left), planLater(right)) :: Nil

        case Join(left, right, _, _) if left.isStreaming && right.isStreaming =>
          throw new AnalysisException(
            "Stream stream joins without equality predicate is not supported", plan = Some(plan))

        case _ => Nil
      }
    }
  }

  /**
   * Used to plan the aggregate operator for expressions based on the AggregateFunction2 interface.
   */
  object Aggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalAggregation(
          groupingExpressions, aggregateExpressions, resultExpressions, child) =>

        val (functionsWithDistinct, functionsWithoutDistinct) =
          aggregateExpressions.partition(_.isDistinct)
        if (functionsWithDistinct.map(_.aggregateFunction.children).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our MultipleDistinctRewriter should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
              "Spark user mailing list.")
        }

        val aggregateOperator =
          if (functionsWithDistinct.isEmpty) {
            aggregate.AggUtils.planAggregateWithoutDistinct(
              groupingExpressions,
              aggregateExpressions,
              resultExpressions,
              planLater(child))
          } else {
            aggregate.AggUtils.planAggregateWithOneDistinct(
              groupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              resultExpressions,
              planLater(child))
          }

        aggregateOperator

      case _ => Nil
    }
  }

  protected lazy val singleRowRdd = sparkContext.parallelize(Seq(InternalRow()), 1)

  object InMemoryScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, mem: InMemoryRelation) =>
        pruneFilterProject(
          projectList,
          filters,
          identity[Seq[Expression]], // All filters still need to be evaluated.
          InMemoryTableScanExec(_, filters, mem)) :: Nil
      case _ => Nil
    }
  }

  /**
   * This strategy is just for explaining `Dataset/DataFrame` created by `spark.readStream`.
   * It won't affect the execution, because `StreamingRelation` will be replaced with
   * `StreamingExecutionRelation` in `StreamingQueryManager` and `StreamingExecutionRelation` will
   * be replaced with the real relation using the `Source` in `StreamExecution`.
   */
  object StreamingRelationStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case s: StreamingRelation =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case s: StreamingExecutionRelation =>
        StreamingRelationExec(s.toString, s.output) :: Nil
      case _ => Nil
    }
  }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case r: RunnableCommand => ExecutedCommandExec(r, r.children.map(planLater)) :: Nil

      case MemoryPlan(sink, output) =>
        val encoder = RowEncoder(sink.schema)
        LocalTableScanExec(output, sink.allData.map(r => encoder.toRow(r).copy())) :: Nil

      case logical.Distinct(child) =>
        throw new IllegalStateException(
          "logical distinct operator should have been replaced by aggregate in the optimizer")
<<<<<<< HEAD
      case logical.Intersect(left, right) =>
        throw new IllegalStateException(
          "logical intersect operator should have been replaced by semi-join in the optimizer")
      case logical.Except(left, right) =>
        throw new IllegalStateException(
          "logical except operator should have been replaced by anti-join in the optimizer")

      case logical.DeserializeToObject(deserializer, objAttr, child) =>
        execution.DeserializeToObjectExec(deserializer, objAttr, planLater(child)) :: Nil
      case logical.SerializeFromObject(serializer, child) =>
        execution.SerializeFromObjectExec(serializer, planLater(child)) :: Nil
      case logical.MapPartitions(f, objAttr, child) =>
        execution.MapPartitionsExec(f, objAttr, planLater(child)) :: Nil
      case logical.MapPartitionsInR(f, p, b, is, os, objAttr, child) =>
        execution.MapPartitionsExec(
          execution.r.MapPartitionsRWrapper(f, p, b, is, os), objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsInR(f, p, b, is, os, key, value, grouping, data, objAttr, child) =>
        execution.FlatMapGroupsInRExec(f, p, b, is, os, key, value, grouping,
          data, objAttr, planLater(child)) :: Nil
      case logical.MapElements(f, _, _, objAttr, child) =>
        execution.MapElementsExec(f, objAttr, planLater(child)) :: Nil
      case logical.AppendColumns(f, _, _, in, out, child) =>
        execution.AppendColumnsExec(f, in, out, planLater(child)) :: Nil
      case logical.AppendColumnsWithObject(f, childSer, newSer, child) =>
        execution.AppendColumnsWithObjectExec(f, childSer, newSer, planLater(child)) :: Nil
      case logical.MapGroups(f, key, value, grouping, data, objAttr, child) =>
        execution.MapGroupsExec(f, key, value, grouping, data, objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsWithState(
          f, key, value, grouping, data, output, _, _, _, timeout, child) =>
        execution.MapGroupsExec(
          f, key, value, grouping, data, output, timeout, planLater(child)) :: Nil
      case logical.CoGroup(f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr, left, right) =>
        execution.CoGroupExec(
          f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr,
=======

      case logical.MapPartitions(f, tEnc, uEnc, output, child) =>
        execution.MapPartitions(f, tEnc, uEnc, output, planLater(child)) :: Nil
      case logical.AppendColumns(f, tEnc, uEnc, newCol, child) =>
        execution.AppendColumns(f, tEnc, uEnc, newCol, planLater(child)) :: Nil
      case logical.MapGroups(f, kEnc, tEnc, uEnc, grouping, output, child) =>
        execution.MapGroups(f, kEnc, tEnc, uEnc, grouping, output, planLater(child)) :: Nil
      case logical.CoGroup(f, kEnc, leftEnc, rightEnc, rEnc, output,
        leftGroup, rightGroup, left, right) =>
        execution.CoGroup(f, kEnc, leftEnc, rightEnc, rEnc, output, leftGroup, rightGroup,
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
          planLater(left), planLater(right)) :: Nil

      case logical.Repartition(numPartitions, shuffle, child) =>
        if (shuffle) {
          ShuffleExchangeExec(RoundRobinPartitioning(numPartitions), planLater(child)) :: Nil
        } else {
          execution.CoalesceExec(numPartitions, planLater(child)) :: Nil
        }
<<<<<<< HEAD
      case logical.Sort(sortExprs, global, child) =>
        execution.SortExec(sortExprs, global, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.ProjectExec(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.FilterExec(condition, planLater(child)) :: Nil
      case f: logical.TypedFilter =>
        execution.FilterExec(f.typedCondition(f.deserializer), planLater(f.child)) :: Nil
      case e @ logical.Expand(_, _, child) =>
        execution.ExpandExec(e.projections, e.output, planLater(child)) :: Nil
      case logical.Window(windowExprs, partitionSpec, orderSpec, child) =>
        execution.window.WindowExec(windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil
=======
      case logical.SortPartitions(sortExprs, child) =>
        // This sort only sorts tuples within a partition. Its requiredDistribution will be
        // an UnspecifiedDistribution.
        execution.Sort(sortExprs, global = false, child = planLater(child)) :: Nil
      case logical.Sort(sortExprs, global, child) =>
        execution.Sort(sortExprs, global, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.Project(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.Filter(condition, planLater(child)) :: Nil
      case e @ logical.Expand(_, _, child) =>
        execution.Expand(e.projections, e.output, planLater(child)) :: Nil
      case logical.Window(projectList, windowExprs, partitionSpec, orderSpec, child) =>
        execution.Window(
          projectList, windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      case logical.Sample(lb, ub, withReplacement, seed, child) =>
        execution.SampleExec(lb, ub, withReplacement, seed, planLater(child)) :: Nil
      case logical.LocalRelation(output, data, _) =>
        LocalTableScanExec(output, data) :: Nil
      case logical.LocalLimit(IntegerLiteral(limit), child) =>
        execution.LocalLimitExec(limit, planLater(child)) :: Nil
      case logical.GlobalLimit(IntegerLiteral(limit), child) =>
        execution.GlobalLimitExec(limit, planLater(child)) :: Nil
      case logical.Union(unionChildren) =>
        execution.UnionExec(unionChildren.map(planLater)) :: Nil
      case g @ logical.Generate(generator, join, outer, _, _, child) =>
<<<<<<< HEAD
        execution.GenerateExec(
          generator, join = join, outer = outer, g.qualifiedGeneratorOutput,
          planLater(child)) :: Nil
      case _: logical.OneRowRelation =>
        execution.RDDScanExec(Nil, singleRowRdd, "OneRowRelation") :: Nil
      case r: logical.Range =>
        execution.RangeExec(r) :: Nil
      case logical.RepartitionByExpression(expressions, child, numPartitions) =>
        exchange.ShuffleExchangeExec(HashPartitioning(
          expressions, numPartitions), planLater(child)) :: Nil
      case ExternalRDD(outputObjAttr, rdd) => ExternalRDDScanExec(outputObjAttr, rdd) :: Nil
      case r: LogicalRDD =>
        RDDScanExec(r.output, r.rdd, "ExistingRDD", r.outputPartitioning, r.outputOrdering) :: Nil
      case h: ResolvedHint => planLater(h.child) :: Nil
=======
        execution.Generate(
          generator, join = join, outer = outer, g.output, planLater(child)) :: Nil
      case logical.OneRowRelation =>
        execution.PhysicalRDD(Nil, singleRowRdd, "OneRowRelation") :: Nil
      case logical.RepartitionByExpression(expressions, child, nPartitions) =>
        execution.Exchange(HashPartitioning(
          expressions, nPartitions.getOrElse(numPartitions)), planLater(child)) :: Nil
      case e @ EvaluatePython(udf, child, _) =>
        BatchPythonEvaluation(udf, e.output, planLater(child)) :: Nil
      case LogicalRDD(output, rdd) => PhysicalRDD(output, rdd, "ExistingRDD") :: Nil
      case BroadcastHint(child) => planLater(child) :: Nil
      case _ => Nil
    }
  }

  object DDLStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case CreateTableUsing(tableIdent, userSpecifiedSchema, provider, true, opts, false, _) =>
        ExecutedCommand(
          CreateTempTableUsing(
            tableIdent, userSpecifiedSchema, provider, opts)) :: Nil
      case c: CreateTableUsing if !c.temporary =>
        sys.error("Tables created with SQLContext must be TEMPORARY. Use a HiveContext instead.")
      case c: CreateTableUsing if c.temporary && c.allowExisting =>
        sys.error("allowExisting should be set to false when creating a temporary table.")

      case CreateTableUsingAsSelect(tableIdent, provider, true, partitionsCols, mode, opts, query)
          if partitionsCols.nonEmpty =>
        sys.error("Cannot create temporary partitioned table.")

      case CreateTableUsingAsSelect(tableIdent, provider, true, _, mode, opts, query) =>
        val cmd = CreateTempTableUsingAsSelect(
          tableIdent, provider, Array.empty[String], mode, opts, query)
        ExecutedCommand(cmd) :: Nil
      case c: CreateTableUsingAsSelect if !c.temporary =>
        sys.error("Tables created with SQLContext must be TEMPORARY. Use a HiveContext instead.")

      case describe @ LogicalDescribeCommand(table, isExtended) =>
        val resultPlan = self.sqlContext.executePlan(table).executedPlan
        ExecutedCommand(
          RunnableDescribeCommand(resultPlan, describe.output, isExtended)) :: Nil

      case logical.ShowFunctions(db, pattern) => ExecutedCommand(ShowFunctions(db, pattern)) :: Nil

      case logical.DescribeFunction(function, extended) =>
        ExecutedCommand(DescribeFunction(function, extended)) :: Nil

>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
      case _ => Nil
    }
  }
}
