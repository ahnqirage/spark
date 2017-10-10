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

import java.util.{List => JList}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.DynamicVariable

import org.apache.spark.{SparkContext, SparkException}
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until `start()` is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when `stop()` is called, and it will drop further events after stopping.
 */
private[spark] class LiveListenerBus(val sparkContext: SparkContext) extends SparkListenerBus {

  self =>

  import LiveListenerBus._

  // Cap the capacity of the event queue so we get an explicit error (rather than
  // an OOM exception) if it's perpetually being added to more quickly than it's being drained.
  private lazy val EVENT_QUEUE_CAPACITY = validateAndGetQueueSize()
  private lazy val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](EVENT_QUEUE_CAPACITY)

  private def validateAndGetQueueSize(): Int = {
    val queueSize = sparkContext.conf.get(LISTENER_BUS_EVENT_QUEUE_SIZE)
    if (queueSize <= 0) {
      throw new SparkException("spark.scheduler.listenerbus.eventqueue.size must be > 0!")
    }
    queueSize
  }

  // Indicate if `start()` is called
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called
  private val stopped = new AtomicBoolean(false)

  /** A counter for dropped events. It will be reset every time we log it. */
  private val droppedEventsCounter = new AtomicLong(0L)

  /** When `droppedEventsCounter` was logged last time in milliseconds. */
  @volatile private var lastReportTimestamp = 0L

  // Indicate if we are processing some event
  // Guarded by `self`
  private var processingEvent = false

  private val logDroppedEvent = new AtomicBoolean(false)

  // A counter that represents the number of events produced and consumed in the queue
  private val eventLock = new Semaphore(0)

  private val listenerThread = new Thread(name) {
    setDaemon(true)
    override def run(): Unit = Utils.tryOrStopSparkContext(sparkContext) {
      LiveListenerBus.withinListenerThread.withValue(true) {
        while (true) {
          eventLock.acquire()
          self.synchronized {
            processingEvent = true
          }
          try {
            val event = eventQueue.poll
            if (event == null) {
              // Get out of the while loop and shutdown the daemon thread
              if (!stopped.get) {
                throw new IllegalStateException("Polling `null` from eventQueue means" +
                  " the listener bus has been stopped. So `stopped` must be true")
              }
              return
            }
            postToAll(event)
          } finally {
            self.synchronized {
              processingEvent = false
            }
          }
        }
        queues.add(newQueue)
    }
  }

  def removeListener(listener: SparkListenerInterface): Unit = synchronized {
    // Remove listener from all queues it was added to, and stop queues that have become empty.
    queues.asScala
      .filter { queue =>
        queue.removeListener(listener)
        queue.listeners.isEmpty()
      }
      .foreach { toRemove =>
        if (started.get() && !stopped.get()) {
          toRemove.stop()
        }
        queues.remove(toRemove)
      }
  }

  /** Post an event to all queues. */
  def post(event: SparkListenerEvent): Unit = {
    if (!stopped.get()) {
      metrics.numEventsPosted.inc()
      val it = queues.iterator()
      while (it.hasNext()) {
        it.next().post(event)
      }
    }
  }

  /**
   * Start sending events to attached listeners.
   *
   * This first sends out all buffered events posted before this listener bus has started, then
   * listens for any additional events asynchronously while the listener bus is still running.
   * This should only be called once.
   *
   */
  def start(): Unit = {
    if (started.compareAndSet(false, true)) {
      listenerThread.start()
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
  }

  def post(event: SparkListenerEvent): Unit = {
    if (stopped.get) {
      // Drop further events to make `listenerThread` exit ASAP
      logDebug(s"$name has already stopped! Dropping event $event")
      return
    }
    val eventAdded = eventQueue.offer(event)
    if (eventAdded) {
      eventLock.release()
    } else {
      onDropEvent(event)
      droppedEventsCounter.incrementAndGet()
    }

    val droppedEvents = droppedEventsCounter.get
    if (droppedEvents > 0) {
      // Don't log too frequently
      if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
        // There may be multiple threads trying to decrease droppedEventsCounter.
        // Use "compareAndSet" to make sure only one thread can win.
        // And if another thread is increasing droppedEventsCounter, "compareAndSet" will fail and
        // then that thread will update it.
        if (droppedEventsCounter.compareAndSet(droppedEvents, 0)) {
          val prevLastReportTimestamp = lastReportTimestamp
          lastReportTimestamp = System.currentTimeMillis()
          logWarning(s"Dropped $droppedEvents SparkListenerEvents since " +
            new java.util.Date(prevLastReportTimestamp))
        }
      }
    }

    this.sparkContext = sc
    queues.asScala.foreach(_.start(sc))
    metricsSystem.registerSource(metrics)
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
   */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
    val deadline = System.currentTimeMillis + timeoutMillis
    queues.asScala.foreach { queue =>
      if (!queue.waitUntilEmpty(deadline)) {
        throw new TimeoutException(s"The event queue is not empty after $timeoutMillis ms.")
      }
    }
  }

  /**
   * Stop the listener bus. It will wait until the queued events have been processed, but drop the
   * new events after stopping.
   */
  def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop bus that has not yet started!")
    }

    if (!stopped.compareAndSet(false, true)) {
      return
    }

    synchronized {
      queues.asScala.foreach(_.stop())
      queues.clear()
    }
  }

  // For testing only.
  private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag](): Seq[T] = {
    queues.asScala.flatMap { queue => queue.findListenersByClass[T]() }
  }

  // For testing only.
  private[spark] def listeners: JList[SparkListenerInterface] = {
    queues.asScala.flatMap(_.listeners.asScala).asJava
  }

  // For testing only.
  private[scheduler] def activeQueues(): Set[String] = {
    queues.asScala.map(_.name).toSet
  }

}

private[spark] object LiveListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  private[scheduler] val SHARED_QUEUE = "shared"

  private[scheduler] val APP_STATUS_QUEUE = "appStatus"

  private[scheduler] val EXECUTOR_MANAGEMENT_QUEUE = "executorManagement"

  private[scheduler] val EVENT_LOG_QUEUE = "eventLog"
}

private[spark] class LiveListenerBusMetrics(conf: SparkConf)
  extends Source with Logging {

  override val sourceName: String = "LiveListenerBus"
  override val metricRegistry: MetricRegistry = new MetricRegistry

  /**
   * The total number of events posted to the LiveListenerBus. This is a count of the total number
   * of events which have been produced by the application and sent to the listener bus, NOT a
   * count of the number of events which have been processed and delivered to listeners (or dropped
   * without being delivered).
   */
  val numEventsPosted: Counter = metricRegistry.counter(MetricRegistry.name("numEventsPosted"))

  // Guarded by synchronization.
  private val perListenerClassTimers = mutable.Map[String, Timer]()

  /**
   * Returns a timer tracking the processing time of the given listener class.
   * events processed by that listener. This method is thread-safe.
   */
  def getTimerForListenerClass(cls: Class[_ <: SparkListenerInterface]): Option[Timer] = {
    synchronized {
      val className = cls.getName
      val maxTimed = conf.get(LISTENER_BUS_METRICS_MAX_LISTENER_CLASSES_TIMED)
      perListenerClassTimers.get(className).orElse {
        if (perListenerClassTimers.size == maxTimed) {
          logError(s"Not measuring processing time for listener class $className because a " +
            s"maximum of $maxTimed listener classes are already timed.")
          None
        } else {
          perListenerClassTimers(className) =
            metricRegistry.timer(MetricRegistry.name("listenerProcessingTime", className))
          perListenerClassTimers.get(className)
        }
      }
    }
  }

}
