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

package org.apache.spark.scheduler

import java.nio.ByteBuffer

import org.apache.spark._
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config
import org.apache.spark.util.ManualClock

class FakeSchedulerBackend extends SchedulerBackend {
  def start() {}
  def stop() {}
  def reviveOffers() {}
  def defaultParallelism(): Int = 1
}

class TaskSchedulerImplSuite extends SparkFunSuite with LocalSparkContext with BeforeAndAfterEach
    with Logging with MockitoSugar {

  var failedTaskSetException: Option[Throwable] = None
  var failedTaskSetReason: String = null
  var failedTaskSet = false

  var blacklist: BlacklistTracker = null
  var taskScheduler: TaskSchedulerImpl = null
  var dagScheduler: DAGScheduler = null

  val stageToMockTaskSetBlacklist = new HashMap[Int, TaskSetBlacklist]()
  val stageToMockTaskSetManager = new HashMap[Int, TaskSetManager]()

  override def beforeEach(): Unit = {
    super.beforeEach()
    failedTaskSet = false
    failedTaskSetException = None
    failedTaskSetReason = null
    stageToMockTaskSetBlacklist.clear()
    stageToMockTaskSetManager.clear()
  }

  override def afterEach(): Unit = {
    super.afterEach()
    if (taskScheduler != null) {
      taskScheduler.stop()
      taskScheduler = null
    }
    if (dagScheduler != null) {
      dagScheduler.stop()
      dagScheduler = null
    }
  }

  def setupScheduler(confs: (String, String)*): TaskSchedulerImpl = {
    val conf = new SparkConf().setMaster("local").setAppName("TaskSchedulerImplSuite")
    confs.foreach { case (k, v) => conf.set(k, v) }
    sc = new SparkContext(conf)
    taskScheduler = new TaskSchedulerImpl(sc)
    setupHelper()
  }

  def setupSchedulerWithMockTaskSetBlacklist(): TaskSchedulerImpl = {
    blacklist = mock[BlacklistTracker]
    val conf = new SparkConf().setMaster("local").setAppName("TaskSchedulerImplSuite")
    conf.set(config.BLACKLIST_ENABLED, true)
    sc = new SparkContext(conf)
    taskScheduler =
      new TaskSchedulerImpl(sc, sc.conf.getInt("spark.task.maxFailures", 4)) {
        override def createTaskSetManager(taskSet: TaskSet, maxFailures: Int): TaskSetManager = {
          val tsm = super.createTaskSetManager(taskSet, maxFailures)
          // we need to create a spied tsm just so we can set the TaskSetBlacklist
          val tsmSpy = spy(tsm)
          val taskSetBlacklist = mock[TaskSetBlacklist]
          when(tsmSpy.taskSetBlacklistHelperOpt).thenReturn(Some(taskSetBlacklist))
          stageToMockTaskSetManager(taskSet.stageId) = tsmSpy
          stageToMockTaskSetBlacklist(taskSet.stageId) = taskSetBlacklist
          tsmSpy
        }

        override private[scheduler] lazy val blacklistTrackerOpt = Some(blacklist)
      }
    setupHelper()
  }

  def setupHelper(): TaskSchedulerImpl = {
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    dagScheduler = new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo): Unit = {}
      override def executorAdded(execId: String, host: String): Unit = {}
      override def taskSetFailed(
          taskSet: TaskSet,
          reason: String,
          exception: Option[Throwable]): Unit = {
        // Normally the DAGScheduler puts this in the event loop, which will eventually fail
        // dependent jobs
        failedTaskSet = true
        failedTaskSetReason = reason
        failedTaskSetException = exception
      }
    }
    taskScheduler
  }

  test("Scheduler does not always schedule tasks on the same workers") {
    val taskScheduler = setupScheduler()
    val numFreeCores = 1
    val workerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", numFreeCores),
      new WorkerOffer("executor1", "host1", numFreeCores))
    // Repeatedly try to schedule a 1-task job, and make sure that it doesn't always
    // get scheduled on the same executor. While there is a chance this test will fail
    // because the task randomly gets placed on the first executor all 1000 times, the
    // probability of that happening is 2^-1000 (so sufficiently small to be considered
    // negligible).
    val numTrials = 1000
    val selectedExecutorIds = 1.to(numTrials).map { _ =>
      val taskSet = FakeTask.createTaskSet(1)
      taskScheduler.submitTasks(taskSet)
      val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
      assert(1 === taskDescriptions.length)
      taskDescriptions(0).executorId
    }
    val count = selectedExecutorIds.count(_ == workerOffers(0).executorId)
    assert(count > 0)
    assert(count < numTrials)
    assert(!failedTaskSet)
  }

  test("Scheduler correctly accounts for multiple CPUs per task") {
    val taskCpus = 2
    val taskScheduler = setupScheduler("spark.task.cpus" -> taskCpus.toString)
    // Give zero core offers. Should not generate any tasks
    val zeroCoreWorkerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", 0),
      new WorkerOffer("executor1", "host1", 0))
    val taskSet = FakeTask.createTaskSet(1)
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(zeroCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // No tasks should run as we only have 1 core free.
    val numFreeCores = 1
    val singleCoreWorkerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", numFreeCores),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(singleCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)

    // Now change the offers to have 2 cores in one executor and verify if it
    // is chosen.
    val multiCoreWorkerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", taskCpus),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(1 === taskDescriptions.length)
    assert("executor0" === taskDescriptions(0).executorId)
    assert(!failedTaskSet)
  }

  test("Scheduler does not crash when tasks are not serializable") {
    val taskCpus = 2
    val taskScheduler = setupScheduler("spark.task.cpus" -> taskCpus.toString)
    val numFreeCores = 1
    val taskSet = new TaskSet(
      Array(new NotSerializableFakeTask(1, 0), new NotSerializableFakeTask(0, 1)), 0, 0, 0, null)
    val multiCoreWorkerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", taskCpus),
      new WorkerOffer("executor1", "host1", numFreeCores))
    taskScheduler.submitTasks(taskSet)
    var taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(0 === taskDescriptions.length)
    assert(failedTaskSet)
    assert(failedTaskSetReason.contains("Failed to serialize task"))

    // Now check that we can still submit tasks
    // Even if one of the task sets has not-serializable tasks, the other task set should
    // still be processed without error
    taskScheduler.submitTasks(FakeTask.createTaskSet(1))
    taskScheduler.submitTasks(taskSet)
    taskDescriptions = taskScheduler.resourceOffers(multiCoreWorkerOffers).flatten
    assert(taskDescriptions.map(_.executorId) === Seq("executor0"))
  }

  test("refuse to schedule concurrent attempts for the same stage (SPARK-8103)") {
    val taskScheduler = setupScheduler()
    val attempt1 = FakeTask.createTaskSet(1, 0)
    val attempt2 = FakeTask.createTaskSet(1, 1)
    taskScheduler.submitTasks(attempt1)
    intercept[IllegalStateException] { taskScheduler.submitTasks(attempt2) }

    // OK to submit multiple if previous attempts are all zombie
    taskScheduler.taskSetManagerForAttempt(attempt1.stageId, attempt1.stageAttemptId)
      .get.isZombie = true
    taskScheduler.submitTasks(attempt2)
    val attempt3 = FakeTask.createTaskSet(1, 2)
    intercept[IllegalStateException] { taskScheduler.submitTasks(attempt3) }
    taskScheduler.taskSetManagerForAttempt(attempt2.stageId, attempt2.stageAttemptId)
      .get.isZombie = true
    taskScheduler.submitTasks(attempt3)
    assert(!failedTaskSet)
  }

  test("don't schedule more tasks after a taskset is zombie") {
    val taskScheduler = setupScheduler()

    val numFreeCores = 1
    val workerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", numFreeCores))
    val attempt1 = FakeTask.createTaskSet(10)

    // submit attempt 1, offer some resources, some tasks get scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
    assert(1 === taskDescriptions.length)

    // now mark attempt 1 as a zombie
    taskScheduler.taskSetManagerForAttempt(attempt1.stageId, attempt1.stageAttemptId)
      .get.isZombie = true

    // don't schedule anything on another resource offer
    val taskDescriptions2 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(0 === taskDescriptions2.length)

    // if we schedule another attempt for the same stage, it should get scheduled
    val attempt2 = FakeTask.createTaskSet(10, 1)

    // submit attempt 2, offer some resources, some tasks get scheduled
    taskScheduler.submitTasks(attempt2)
    val taskDescriptions3 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(1 === taskDescriptions3.length)
    val mgr = taskScheduler.taskIdToTaskSetManager.get(taskDescriptions3(0).taskId).get
    assert(mgr.taskSet.stageAttemptId === 1)
    assert(!failedTaskSet)
  }

  test("if a zombie attempt finishes, continue scheduling tasks for non-zombie attempts") {
    val taskScheduler = setupScheduler()

    val numFreeCores = 10
    val workerOffers = IndexedSeq(new WorkerOffer("executor0", "host0", numFreeCores))
    val attempt1 = FakeTask.createTaskSet(10)

    // submit attempt 1, offer some resources, some tasks get scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(workerOffers).flatten
    assert(10 === taskDescriptions.length)

    // now mark attempt 1 as a zombie
    val mgr1 = taskScheduler.taskSetManagerForAttempt(attempt1.stageId, attempt1.stageAttemptId).get
    mgr1.isZombie = true

    // don't schedule anything on another resource offer
    val taskDescriptions2 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(0 === taskDescriptions2.length)

    // submit attempt 2
    val attempt2 = FakeTask.createTaskSet(10, 1)
    taskScheduler.submitTasks(attempt2)

    // attempt 1 finished (this can happen even if it was marked zombie earlier -- all tasks were
    // already submitted, and then they finish)
    taskScheduler.taskSetFinished(mgr1)

    // now with another resource offer, we should still schedule all the tasks in attempt2
    val taskDescriptions3 = taskScheduler.resourceOffers(workerOffers).flatten
    assert(10 === taskDescriptions3.length)

    taskDescriptions3.foreach { task =>
      val mgr = taskScheduler.taskIdToTaskSetManager.get(task.taskId).get
      assert(mgr.taskSet.stageAttemptId === 1)
    }
    assert(!failedTaskSet)
  }

  test("tasks are not re-scheduled while executor loss reason is pending") {
    val taskScheduler = setupScheduler()

    val e0Offers = IndexedSeq(new WorkerOffer("executor0", "host0", 1))
    val e1Offers = IndexedSeq(new WorkerOffer("executor1", "host0", 1))
    val attempt1 = FakeTask.createTaskSet(1)

    // submit attempt 1, offer resources, task gets scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(e0Offers).flatten
    assert(1 === taskDescriptions.length)

    // mark executor0 as dead but pending fail reason
    taskScheduler.executorLost("executor0", LossReasonPending)

    // offer some more resources on a different executor, nothing should change
    val taskDescriptions2 = taskScheduler.resourceOffers(e1Offers).flatten
    assert(0 === taskDescriptions2.length)

    // provide the actual loss reason for executor0
    taskScheduler.executorLost("executor0", SlaveLost("oops"))

    // executor0's tasks should have failed now that the loss reason is known, so offering more
    // resources should make them be scheduled on the new executor.
    val taskDescriptions3 = taskScheduler.resourceOffers(e1Offers).flatten
    assert(1 === taskDescriptions3.length)
    assert("executor1" === taskDescriptions3(0).executorId)
    assert(!failedTaskSet)
  }

  test("scheduled tasks obey task and stage blacklists") {
    taskScheduler = setupSchedulerWithMockTaskSetBlacklist()
    (0 to 2).foreach {stageId =>
      val taskSet = FakeTask.createTaskSet(numTasks = 2, stageId = stageId, stageAttemptId = 0)
      taskScheduler.submitTasks(taskSet)
    }

    // Setup our mock blacklist:
    // * stage 0 is blacklisted on node "host1"
    // * stage 1 is blacklisted on executor "executor3"
    // * stage 0, partition 0 is blacklisted on executor 0
    // (mocked methods default to returning false, ie. no blacklisting)
    when(stageToMockTaskSetBlacklist(0).isNodeBlacklistedForTaskSet("host1")).thenReturn(true)
    when(stageToMockTaskSetBlacklist(1).isExecutorBlacklistedForTaskSet("executor3"))
      .thenReturn(true)
    when(stageToMockTaskSetBlacklist(0).isExecutorBlacklistedForTask("executor0", 0))
      .thenReturn(true)

    val offers = IndexedSeq(
      new WorkerOffer("executor0", "host0", 1),
      new WorkerOffer("executor1", "host1", 1),
      new WorkerOffer("executor2", "host1", 1),
      new WorkerOffer("executor3", "host2", 10)
    )
    val firstTaskAttempts = taskScheduler.resourceOffers(offers).flatten
    // We should schedule all tasks.
    assert(firstTaskAttempts.size === 6)
    // Whenever we schedule a task, we must consult the node and executor blacklist.  (The test
    // doesn't check exactly what checks are made because the offers get shuffled.)
    (0 to 2).foreach { stageId =>
      verify(stageToMockTaskSetBlacklist(stageId), atLeast(1))
        .isNodeBlacklistedForTaskSet(anyString())
      verify(stageToMockTaskSetBlacklist(stageId), atLeast(1))
        .isExecutorBlacklistedForTaskSet(anyString())
    }

    def tasksForStage(stageId: Int): Seq[TaskDescription] = {
      firstTaskAttempts.filter{_.name.contains(s"stage $stageId")}
    }
    tasksForStage(0).foreach { task =>
      // executors 1 & 2 blacklisted for node
      // executor 0 blacklisted just for partition 0
      if (task.index == 0) {
        assert(task.executorId === "executor3")
      } else {
        assert(Set("executor0", "executor3").contains(task.executorId))
      }
    }
    tasksForStage(1).foreach { task =>
      // executor 3 blacklisted
      assert("executor3" != task.executorId)
    }
    // no restrictions on stage 2

    // Finally, just make sure that we can still complete tasks as usual with blacklisting
    // in effect.  Finish each of the tasksets -- taskset 0 & 1 complete successfully, taskset 2
    // fails.
    (0 to 2).foreach { stageId =>
      val tasks = tasksForStage(stageId)
      val tsm = taskScheduler.taskSetManagerForAttempt(stageId, 0).get
      val valueSer = SparkEnv.get.serializer.newInstance()
      if (stageId == 2) {
        // Just need to make one task fail 4 times.
        var task = tasks(0)
        val taskIndex = task.index
        (0 until 4).foreach { attempt =>
          assert(task.attemptNumber === attempt)
          tsm.handleFailedTask(task.taskId, TaskState.FAILED, TaskResultLost)
          val nextAttempts =
            taskScheduler.resourceOffers(IndexedSeq(WorkerOffer("executor4", "host4", 1))).flatten
          if (attempt < 3) {
            assert(nextAttempts.size === 1)
            task = nextAttempts(0)
            assert(task.index === taskIndex)
          } else {
            assert(nextAttempts.size === 0)
          }
        }
        // End the other task of the taskset, doesn't matter whether it succeeds or fails.
        val otherTask = tasks(1)
        val result = new DirectTaskResult[Int](valueSer.serialize(otherTask.taskId), Seq())
        tsm.handleSuccessfulTask(otherTask.taskId, result)
      } else {
        tasks.foreach { task =>
          val result = new DirectTaskResult[Int](valueSer.serialize(task.taskId), Seq())
          tsm.handleSuccessfulTask(task.taskId, result)
        }
      }
      assert(tsm.isZombie)
    }

    // the tasksSets complete, so the tracker should be notified of the successful ones
    verify(blacklist, times(1)).updateBlacklistForSuccessfulTaskSet(
      stageId = 0,
      stageAttemptId = 0,
      failuresByExec = stageToMockTaskSetBlacklist(0).execToFailures)
    verify(blacklist, times(1)).updateBlacklistForSuccessfulTaskSet(
      stageId = 1,
      stageAttemptId = 0,
      failuresByExec = stageToMockTaskSetBlacklist(1).execToFailures)
    // but we shouldn't update for the failed taskset
    verify(blacklist, never).updateBlacklistForSuccessfulTaskSet(
      stageId = meq(2),
      stageAttemptId = anyInt(),
      failuresByExec = anyObject())
  }

  test("if an executor is lost then the state for its running tasks is cleaned up (SPARK-18553)") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }

    val e0Offers = Seq(WorkerOffer("executor0", "host0", 1))
    val attempt1 = FakeTask.createTaskSet(1)

    // submit attempt 1, offer resources, task gets scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(e0Offers).flatten
    assert(1 === taskDescriptions.length)

    // mark executor0 as dead
    taskScheduler.executorLost("executor0", SlaveLost())
    assert(!taskScheduler.isExecutorAlive("executor0"))
    assert(!taskScheduler.hasExecutorsAliveOnHost("host0"))
    assert(taskScheduler.getExecutorsAliveOnHost("host0").isEmpty)


    // Check that state associated with the lost task attempt is cleaned up:
    assert(taskScheduler.taskIdToExecutorId.isEmpty)
    assert(taskScheduler.taskIdToTaskSetManager.isEmpty)
    assert(taskScheduler.runningTasksByExecutors.get("executor0").isEmpty)
  }

  test("if a task finishes with TaskState.LOST its executor is marked as dead") {
    sc = new SparkContext("local", "TaskSchedulerImplSuite")
    val taskScheduler = new TaskSchedulerImpl(sc)
    taskScheduler.initialize(new FakeSchedulerBackend)
    // Need to initialize a DAGScheduler for the taskScheduler to use for callbacks.
    new DAGScheduler(sc, taskScheduler) {
      override def taskStarted(task: Task[_], taskInfo: TaskInfo) {}
      override def executorAdded(execId: String, host: String) {}
    }

    val e0Offers = Seq(WorkerOffer("executor0", "host0", 1))
    val attempt1 = FakeTask.createTaskSet(1)

    // submit attempt 1, offer resources, task gets scheduled
    taskScheduler.submitTasks(attempt1)
    val taskDescriptions = taskScheduler.resourceOffers(e0Offers).flatten
    assert(1 === taskDescriptions.length)

    // Report the task as failed with TaskState.LOST
    taskScheduler.statusUpdate(
      tid = taskDescriptions.head.taskId,
      state = TaskState.LOST,
      serializedData = ByteBuffer.allocate(0)
    )

    // Check that state associated with the lost task attempt is cleaned up:
    assert(taskScheduler.taskIdToExecutorId.isEmpty)
    assert(taskScheduler.taskIdToTaskSetManager.isEmpty)
    assert(taskScheduler.runningTasksByExecutors.get("executor0").isEmpty)

    // Check that the executor has been marked as dead
    assert(!taskScheduler.isExecutorAlive("executor0"))
    assert(!taskScheduler.hasExecutorsAliveOnHost("host0"))
    assert(taskScheduler.getExecutorsAliveOnHost("host0").isEmpty)
  }
}
