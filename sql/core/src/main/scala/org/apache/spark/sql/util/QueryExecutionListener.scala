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

package org.apache.spark.sql.util

import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable.ListBuffer
import scala.util.control.NonFatal

<<<<<<< HEAD
import org.apache.spark.annotation.{DeveloperApi, Experimental, InterfaceStability}
import org.apache.spark.internal.Logging
=======
import org.apache.spark.Logging
import org.apache.spark.annotation.{DeveloperApi, Experimental}
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
import org.apache.spark.sql.execution.QueryExecution

/**
 * :: Experimental ::
 * The interface of query execution listener that can be used to analyze execution metrics.
 *
<<<<<<< HEAD
 * @note Implementations should guarantee thread-safety as they can be invoked by
=======
 * Note that implementations should guarantee thread-safety as they can be invoked by
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
 * multiple different threads.
 */
@Experimental
@InterfaceStability.Evolving
trait QueryExecutionListener {

  /**
   * A callback function that will be called when a query executed successfully.
<<<<<<< HEAD
=======
   * Note that this can be invoked by multiple different threads.
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
   *
   * @param funcName name of the action that triggered this query.
   * @param qe the QueryExecution object that carries detail information like logical plan,
   *           physical plan, etc.
   * @param durationNs the execution time for this query in nanoseconds.
<<<<<<< HEAD
   *
   * @note This can be invoked by multiple different threads.
=======
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
   */
  @DeveloperApi
  def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit

  /**
   * A callback function that will be called when a query execution failed.
<<<<<<< HEAD
=======
   * Note that this can be invoked by multiple different threads.
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
   *
   * @param funcName the name of the action that triggered this query.
   * @param qe the QueryExecution object that carries detail information like logical plan,
   *           physical plan, etc.
   * @param exception the exception that failed this query.
   *
   * @note This can be invoked by multiple different threads.
   */
  @DeveloperApi
  def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit
}


/**
 * :: Experimental ::
 *
<<<<<<< HEAD
 * Manager for [[QueryExecutionListener]]. See `org.apache.spark.sql.SQLContext.listenerManager`.
 */
@Experimental
@InterfaceStability.Evolving
=======
 * Manager for [[QueryExecutionListener]]. See [[org.apache.spark.sql.SQLContext.listenerManager]].
 */
@Experimental
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
class ExecutionListenerManager private[sql] () extends Logging {

  /**
   * Registers the specified [[QueryExecutionListener]].
   */
  @DeveloperApi
  def register(listener: QueryExecutionListener): Unit = writeLock {
    listeners += listener
  }

  /**
   * Unregisters the specified [[QueryExecutionListener]].
   */
  @DeveloperApi
  def unregister(listener: QueryExecutionListener): Unit = writeLock {
    listeners -= listener
  }

  /**
   * Removes all the registered [[QueryExecutionListener]].
   */
  @DeveloperApi
  def clear(): Unit = writeLock {
    listeners.clear()
  }

<<<<<<< HEAD
  /**
   * Get an identical copy of this listener manager.
   */
  @DeveloperApi
  override def clone(): ExecutionListenerManager = writeLock {
    val newListenerManager = new ExecutionListenerManager
    listeners.foreach(newListenerManager.register)
    newListenerManager
  }

=======
>>>>>>> a233fac0b8bf8229d938a24f2ede2d9d8861c284
  private[sql] def onSuccess(funcName: String, qe: QueryExecution, duration: Long): Unit = {
    readLock {
      withErrorHandling { listener =>
        listener.onSuccess(funcName, qe, duration)
      }
    }
  }

  private[sql] def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {
    readLock {
      withErrorHandling { listener =>
        listener.onFailure(funcName, qe, exception)
      }
    }
  }

  private[this] val listeners = ListBuffer.empty[QueryExecutionListener]

  /** A lock to prevent updating the list of listeners while we are traversing through them. */
  private[this] val lock = new ReentrantReadWriteLock()

  private def withErrorHandling(f: QueryExecutionListener => Unit): Unit = {
    for (listener <- listeners) {
      try {
        f(listener)
      } catch {
        case NonFatal(e) => logWarning("Error executing query execution listener", e)
      }
    }
  }

  /** Acquires a read lock on the cache for the duration of `f`. */
  private def readLock[A](f: => A): A = {
    val rl = lock.readLock()
    rl.lock()
    try f finally {
      rl.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private def writeLock[A](f: => A): A = {
    val wl = lock.writeLock()
    wl.lock()
    try f finally {
      wl.unlock()
    }
  }
}
