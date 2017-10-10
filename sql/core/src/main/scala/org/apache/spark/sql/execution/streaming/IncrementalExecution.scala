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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.{InternalOutputModes, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{AllTuples, ClusteredDistribution, HashPartitioning, SinglePartition}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan, SparkPlanner, UnaryExecNode}
import org.apache.spark.sql.streaming.OutputMode

/**
 * A variant of [[QueryExecution]] that allows the execution of the given [[LogicalPlan]]
 * plan incrementally. Possibly preserving state in between each execution.
 */
class IncrementalExecution(
    sparkSession: SparkSession,
    logicalPlan: LogicalPlan,
    val outputMode: OutputMode,
    val checkpointLocation: String,
    val currentBatchId: Long)
  extends QueryExecution(sparkSession, logicalPlan) {

  // TODO: make this always part of planning.
  val stateStrategy = sparkSession.sessionState.planner.StatefulAggregationStrategy +:
    sparkSession.sessionState.planner.StreamingRelationStrategy +:
    sparkSession.sessionState.experimentalMethods.extraStrategies

  // Modified planner with stateful operations.
  override val planner: SparkPlanner = new SparkPlanner(
      sparkSession.sparkContext,
      sparkSession.sessionState.conf,
      sparkSession.sessionState.experimentalMethods) {
    override def strategies: Seq[Strategy] =
      extraPlanningStrategies ++
      sparkSession.sessionState.planner.strategies

    override def extraPlanningStrategies: Seq[Strategy] =
      StreamingJoinStrategy ::
      StatefulAggregationStrategy ::
      FlatMapGroupsWithStateStrategy ::
      StreamingRelationStrategy ::
      StreamingDeduplicationStrategy :: Nil
  }

  /**
   * See [SPARK-18339]
   * Walk the optimized logical plan and replace CurrentBatchTimestamp
   * with the desired literal
   */
  override lazy val optimizedPlan: LogicalPlan = {
    sparkSession.sessionState.optimizer.execute(withCachedData) transformAllExpressions {
      case ts @ CurrentBatchTimestamp(timestamp, _, _) =>
        logInfo(s"Current batch timestamp = $timestamp")
        ts.toLiteral
    }
  }

  /**
   * Records the current id for a given stateful operator in the query plan as the `state`
   * preparation walks the query plan.
   */
  private val statefulOperatorId = new AtomicInteger(0)

  /** Get the state info of the next stateful operator */
  private def nextStatefulOperationStateInfo(): StatefulOperatorStateInfo = {
    StatefulOperatorStateInfo(
      checkpointLocation, runId, statefulOperatorId.getAndIncrement(), currentBatchId)
  }

  /** Locates save/restore pairs surrounding aggregation. */
  val state = new Rule[SparkPlan] {

    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case StateStoreSaveExec(keys, None, None,
             UnaryExecNode(agg,
               StateStoreRestoreExec(keys2, None, child))) =>
        val stateId = OperatorStateId(checkpointLocation, operatorId, currentBatchId)
        val returnAllStates = if (outputMode == InternalOutputModes.Complete) true else false
        operatorId += 1

        StateStoreSaveExec(
          keys,
          Some(stateId),
          Some(returnAllStates),
          agg.withNewChildren(
            StateStoreRestoreExec(
              keys,
              Some(aggStateInfo),
              child) :: Nil))

      case StreamingDeduplicateExec(keys, child, None, None) =>
        StreamingDeduplicateExec(
          keys,
          child,
          Some(nextStatefulOperationStateInfo),
          Some(offsetSeqMetadata.batchWatermarkMs))

      case m: FlatMapGroupsWithStateExec =>
        m.copy(
          stateInfo = Some(nextStatefulOperationStateInfo),
          batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
          eventTimeWatermark = Some(offsetSeqMetadata.batchWatermarkMs))

      case j: StreamingSymmetricHashJoinExec =>
        j.copy(
          stateInfo = Some(nextStatefulOperationStateInfo),
          eventTimeWatermark = Some(offsetSeqMetadata.batchWatermarkMs),
          stateWatermarkPredicates =
            StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
              j.left.output, j.right.output, j.leftKeys, j.rightKeys, j.condition,
              Some(offsetSeqMetadata.batchWatermarkMs))
        )
    }
  }

  override def preparations: Seq[Rule[SparkPlan]] =
    Seq(state, EnsureStatefulOpPartitioning) ++ super.preparations

  /** No need assert supported, as this check has already been done */
  override def assertSupported(): Unit = { }
}

object EnsureStatefulOpPartitioning extends Rule[SparkPlan] {
  // Needs to be transformUp to avoid extra shuffles
  override def apply(plan: SparkPlan): SparkPlan = plan transformUp {
    case so: StatefulOperator =>
      val numPartitions = plan.sqlContext.sessionState.conf.numShufflePartitions
      val distributions = so.requiredChildDistribution
      val children = so.children.zip(distributions).map { case (child, reqDistribution) =>
        val expectedPartitioning = reqDistribution match {
          case AllTuples => SinglePartition
          case ClusteredDistribution(keys) => HashPartitioning(keys, numPartitions)
          case _ => throw new AnalysisException("Unexpected distribution expected for " +
            s"Stateful Operator: $so. Expect AllTuples or ClusteredDistribution but got " +
            s"$reqDistribution.")
        }
        if (child.outputPartitioning.guarantees(expectedPartitioning) &&
            child.execute().getNumPartitions == expectedPartitioning.numPartitions) {
          child
        } else {
          ShuffleExchangeExec(expectedPartitioning, child)
        }
      }
      so.withNewChildren(children)
  }
}
