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

package org.apache.spark.ui.exec

import javax.servlet.http.HttpServletRequest

import scala.xml.Node

import org.apache.spark.status.api.v1.{ExecutorSummary, MemoryMetrics}
import org.apache.spark.ui.{UIUtils, WebUIPage}

// This isn't even used anymore -- but we need to keep it b/c of a MiMa false positive
private[ui] case class ExecutorSummaryInfo(
    id: String,
    hostPort: String,
    rddBlocks: Int,
    memoryUsed: Long,
    diskUsed: Long,
    activeTasks: Int,
    failedTasks: Int,
    completedTasks: Int,
    totalTasks: Int,
    totalDuration: Long,
    totalInputBytes: Long,
    totalShuffleRead: Long,
    totalShuffleWrite: Long,
    isBlacklisted: Int,
    maxOnHeapMem: Long,
    maxOffHeapMem: Long,
    executorLogs: Map[String, String])


private[ui] class ExecutorsPage(
    parent: ExecutorsTab,
    threadDumpEnabled: Boolean)
  extends WebUIPage("") {

  def render(request: HttpServletRequest): Seq[Node] = {
    val (storageStatusList, execInfo) = listener.synchronized {
      // The follow codes should be protected by `listener` to make sure no executors will be
      // removed before we query their status. See SPARK-12784.
      val _storageStatusList = listener.storageStatusList
      val _execInfo = {
        for (statusId <- 0 until _storageStatusList.size)
          yield ExecutorsPage.getExecInfo(listener, statusId)
      }
      (_storageStatusList, _execInfo)
    }
    val maxMem = storageStatusList.map(_.maxMem).sum
    val memUsed = storageStatusList.map(_.memUsed).sum
    val diskUsed = storageStatusList.map(_.diskUsed).sum
    val execInfoSorted = execInfo.sortBy(_.id)
    val logsExist = execInfo.filter(_.executorLogs.nonEmpty).nonEmpty

    val execTable =
      <table class={UIUtils.TABLE_CLASS_STRIPED_SORTABLE}>
        <thead>
          <th>Executor ID</th>
          <th>Address</th>
          <th>RDD Blocks</th>
          <th><span data-toggle="tooltip" title={ToolTips.STORAGE_MEMORY}>Storage Memory</span></th>
          <th>Disk Used</th>
          <th>Active Tasks</th>
          <th>Failed Tasks</th>
          <th>Complete Tasks</th>
          <th>Total Tasks</th>
          <th>Task Time</th>
          <th><span data-toggle="tooltip" title={ToolTips.INPUT}>Input</span></th>
          <th><span data-toggle="tooltip" title={ToolTips.SHUFFLE_READ}>Shuffle Read</span></th>
          <th>
            <!-- Place the shuffle write tooltip on the left (rather than the default position
              of on top) because the shuffle write column is the last column on the right side and
              the tooltip is wider than the column, so it doesn't fit on top. -->
            <span data-toggle="tooltip" data-placement="left" title={ToolTips.SHUFFLE_WRITE}>
              Shuffle Write
            </span>
          </th>
          {if (logsExist) <th class="sorttable_nosort">Logs</th> else Seq.empty}
          {if (threadDumpEnabled) <th class="sorttable_nosort">Thread Dump</th> else Seq.empty}
        </thead>
        <tbody>
          {execInfoSorted.map(execRow(_, logsExist))}
        </tbody>
      </table>

>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
    val content =
      <div>
        {
          <div>
            <span class="expand-additional-metrics">
              <span class="expand-additional-metrics-arrow arrow-closed"></span>
              <a>Show Additional Metrics</a>
            </span>
            <div class="additional-metrics collapsed">
              <ul>
                <li>
                  <input type="checkbox" id="select-all-metrics"/>
                  <span class="additional-metric-title"><em>(De)select All</em></span>
                </li>
                <li>
                  <span data-toggle="tooltip"
                        title={ExecutorsPage.ON_HEAP_MEMORY_TOOLTIP} data-placement="right">
                    <input type="checkbox" name="on_heap_memory"/>
                    <span class="additional-metric-title">On Heap Storage Memory</span>
                  </span>
                </li>
                <li>
                  <span data-toggle="tooltip"
                        title={ExecutorsPage.OFF_HEAP_MEMORY_TOOLTIP} data-placement="right">
                    <input type="checkbox" name="off_heap_memory"/>
                    <span class="additional-metric-title">Off Heap Storage Memory</span>
                  </span>
                </li>
              </ul>
            </div>
          </div> ++
          <div id="active-executors" class="row-fluid"></div> ++
          <script src={UIUtils.prependBaseUri("/static/utils.js")}></script> ++
          <script src={UIUtils.prependBaseUri("/static/executorspage.js")}></script> ++
          <script>setThreadDumpEnabled({threadDumpEnabled})</script>
        }
      </div>

    UIUtils.headerSparkPage("Executors", content, parent, useDataTables = true)
  }
}

private[spark] object ExecutorsPage {
  private val ON_HEAP_MEMORY_TOOLTIP = "Memory used / total available memory for on heap " +
    "storage of data like RDD partitions cached in memory."
  private val OFF_HEAP_MEMORY_TOOLTIP = "Memory used / total available memory for off heap " +
    "storage of data like RDD partitions cached in memory."

  /** Represent an executor's info as a map given a storage status index */
  def getExecInfo(
      listener: ExecutorsListener,
      statusId: Int,
      isActive: Boolean): ExecutorSummary = {
    val status = if (isActive) {
      listener.activeStorageStatusList(statusId)
    } else {
      listener.deadStorageStatusList(statusId)
    }
    val execId = status.blockManagerId.executorId
    val hostPort = status.blockManagerId.hostPort
    val rddBlocks = status.numBlocks
    val memUsed = status.memUsed
    val maxMem = status.maxMem
    val memoryMetrics = for {
      onHeapUsed <- status.onHeapMemUsed
      offHeapUsed <- status.offHeapMemUsed
      maxOnHeap <- status.maxOnHeapMem
      maxOffHeap <- status.maxOffHeapMem
    } yield {
      new MemoryMetrics(onHeapUsed, offHeapUsed, maxOnHeap, maxOffHeap)
    }


    val diskUsed = status.diskUsed
    val taskSummary = listener.executorToTaskSummary.getOrElse(execId, ExecutorTaskSummary(execId))

    new ExecutorSummary(
      execId,
      hostPort,
      isActive,
      rddBlocks,
      memUsed,
      diskUsed,
      taskSummary.totalCores,
      taskSummary.tasksMax,
      taskSummary.tasksActive,
      taskSummary.tasksFailed,
      taskSummary.tasksComplete,
      taskSummary.tasksActive + taskSummary.tasksFailed + taskSummary.tasksComplete,
      taskSummary.duration,
      taskSummary.jvmGCTime,
      taskSummary.inputBytes,
      taskSummary.shuffleRead,
      taskSummary.shuffleWrite,
      taskSummary.isBlacklisted,
      maxMem,
      taskSummary.executorLogs,
      memoryMetrics
    )
  }
}
