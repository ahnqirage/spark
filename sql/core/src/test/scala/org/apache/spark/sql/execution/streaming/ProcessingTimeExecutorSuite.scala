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

import java.util.concurrent.ConcurrentHashMap

import scala.collection.mutable

import org.eclipse.jetty.util.ConcurrentHashSet
import org.scalatest.concurrent.Eventually
import org.scalatest.concurrent.PatienceConfiguration.Timeout
import org.scalatest.concurrent.TimeLimits._
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.util.{Clock, ManualClock, SystemClock}

class ProcessingTimeExecutorSuite extends SparkFunSuite {

  val timeout = 10.seconds

  test("nextBatchTime") {
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTime(100))
    assert(processingTimeExecutor.nextBatchTime(0) === 100)
    assert(processingTimeExecutor.nextBatchTime(1) === 100)
    assert(processingTimeExecutor.nextBatchTime(99) === 100)
    assert(processingTimeExecutor.nextBatchTime(100) === 200)
    assert(processingTimeExecutor.nextBatchTime(101) === 200)
    assert(processingTimeExecutor.nextBatchTime(150) === 200)
  }

  test("calling nextBatchTime with the result of a previous call should return the next interval") {
    val intervalMS = 100
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTime(intervalMS))

    val ITERATION = 10
    var nextBatchTime: Long = 0
    for (it <- 1 to ITERATION) {
      nextBatchTime = processingTimeExecutor.nextBatchTime(nextBatchTime)
    }

    // nextBatchTime should be 1000
    assert(nextBatchTime === intervalMS * ITERATION)
  }

  private def testBatchTermination(intervalMs: Long): Unit = {
    var batchCounts = 0
    val processingTimeExecutor = ProcessingTimeExecutor(ProcessingTime(intervalMs))
    processingTimeExecutor.execute(() => {
      batchCounts += 1
      // If the batch termination works correctly, batchCounts should be 3 after `execute`
      batchCounts < 3
    })
    assert(batchCounts === 3)
  }

  test("batch termination") {
    testBatchTermination(0)
    testBatchTermination(10)
  }

  test("notifyBatchFallingBehind") {
    val clock = new StreamManualClock()
    @volatile var batchFallingBehindCalled = false
    val t = new Thread() {
      override def run(): Unit = {
        val processingTimeExecutor = new ProcessingTimeExecutor(ProcessingTime(100), clock) {
          override def notifyBatchFallingBehind(realElapsedTimeMs: Long): Unit = {
            batchFallingBehindCalled = true
          }
        }
        processingTimeExecutor.execute(() => {
          clock.waitTillTime(200)
          false
        })
      }
    }
    t.start()
    // Wait until the batch is running so that we don't call `advance` too early
    eventually { assert(clock.isStreamWaitingFor(200)) }
    clock.advance(200)
    waitForThreadJoin(t)
    assert(batchFallingBehindCalled === true)
  }

  private def eventually(body: => Unit): Unit = {
    Eventually.eventually(Timeout(timeout)) { body }
  }

  private def waitForThreadJoin(thread: Thread): Unit = {
    failAfter(timeout) { thread.join() }
  }
}
