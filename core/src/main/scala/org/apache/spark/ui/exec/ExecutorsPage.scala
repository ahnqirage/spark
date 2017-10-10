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

import scala.util.Try
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

  // a safe String to Int for sorting ids (converts non-numeric Strings to -1)
  private def idStrToInt(str: String) : Int = Try(str.toInt).getOrElse(-1)

  def render(request: HttpServletRequest): Seq[Node] = {
    val (activeExecutorInfo, deadExecutorInfo) = listener.synchronized {
      // The follow codes should be protected by `listener` to make sure no executors will be
      // removed before we query their status. See SPARK-12784.
      val _activeExecutorInfo = {
        for (statusId <- 0 until listener.activeStorageStatusList.size)
          yield ExecutorsPage.getExecInfo(listener, statusId, isActive = true)
      }
      val _deadExecutorInfo = {
        for (statusId <- 0 until listener.deadStorageStatusList.size)
          yield ExecutorsPage.getExecInfo(listener, statusId, isActive = false)
      }
      (_activeExecutorInfo, _deadExecutorInfo)
    }

    val execInfo = activeExecutorInfo ++ deadExecutorInfo
    implicit val idOrder = Ordering[Int].on((s: String) => idStrToInt(s)).reverse
    val execInfoSorted = execInfo.sortBy(_.id)
    val logsExist = execInfo.filter(_.executorLogs.nonEmpty).nonEmpty

    val execTable = {
      <table class={UIUtils.TABLE_CLASS_STRIPED_SORTABLE}>
        <thead>
          <th class="sorttable_numeric">Executor ID</th>
          <th>Address</th>
          <th>Status</th>
          <th>RDD Blocks</th>
          <th><span data-toggle="tooltip" title={ToolTips.STORAGE_MEMORY}>Storage Memory</span></th>
          <th>Disk Used</th>
          <th>Cores</th>
          <th>Active Tasks</th>
          <th>Failed Tasks</th>
          <th>Complete Tasks</th>
          <th>Total Tasks</th>
          <th><span data-toggle="tooltip" title={ToolTips.TASK_TIME}>Task Time (GC Time)</span></th>
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
    }

    val content =
      <div class="row">
        <div class="span12">
          <h4>Summary</h4>
          {execSummary(activeExecutorInfo, deadExecutorInfo)}
        </div>
      </div>
      <div class = "row">
        <div class="span12">
          <h4>Executors</h4>
          {execTable}
        </div>
      </div>;

    UIUtils.headerSparkPage("Executors", content, parent)
  }

  /** Render an HTML row representing an executor */
  private def execRow(info: ExecutorSummary, logsExist: Boolean): Seq[Node] = {
    val maximumMemory = info.maxMemory
    val memoryUsed = info.memoryUsed
    val diskUsed = info.diskUsed
    val executorStatus =
      if (info.isActive) {
        "Active"
      } else {
        "Dead"
      }

    <tr>
      <td sorttable_customkey={idStrToInt(info.id).toString}>{info.id}</td>
      <td>{info.hostPort}</td>
      <td sorttable_customkey={executorStatus.toString}>
        {executorStatus}
      </td>
      <td>{info.rddBlocks}</td>
      <td sorttable_customkey={memoryUsed.toString}>
        {Utils.bytesToString(memoryUsed)} /
        {Utils.bytesToString(maximumMemory)}
      </td>
      <td sorttable_customkey={diskUsed.toString}>
        {Utils.bytesToString(diskUsed)}
      </td>
      <td>{info.totalCores}</td>
      {taskData(info.maxTasks, info.activeTasks, info.failedTasks, info.completedTasks,
      info.totalTasks, info.totalDuration, info.totalGCTime)}
      <td sorttable_customkey={info.totalInputBytes.toString}>
        {Utils.bytesToString(info.totalInputBytes)}
      </td>
      <td sorttable_customkey={info.totalShuffleRead.toString}>
        {Utils.bytesToString(info.totalShuffleRead)}
      </td>
      <td sorttable_customkey={info.totalShuffleWrite.toString}>
        {Utils.bytesToString(info.totalShuffleWrite)}
      </td>
      {
        if (logsExist) {
          <td>
            {
              info.executorLogs.map { case (logName, logUrl) =>
                <div>
                  <a href={logUrl}>
                    {logName}
                  </a>
                </div>
              }
            }
          </td>
        }
      }
      {
        if (threadDumpEnabled) {
          if (info.isActive) {
            val encodedId = URLEncoder.encode(info.id, "UTF-8")
            <td>
              <a href={s"threadDump/?executorId=${encodedId}"}>Thread Dump</a>
            </td>
          } else {
            <td> </td>
          }
        } else {
          Seq.empty
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
