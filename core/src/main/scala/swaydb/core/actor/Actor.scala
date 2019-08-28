/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.actor

import java.util.TimerTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.function.IntUnaryOperator

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.util.{Functions, Scheduler}
import swaydb.data.config.ActorConfig
import swaydb.data.config.ActorConfig.QueueOrder

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

private[swaydb] sealed trait ActorRef[-T] { self =>

  /**
   * Submits message to Actor's queue and starts message execution if not already running.
   */
  def !(message: T): Unit

  /**
   * Sends a message to this actor with delay
   */
  def schedule(message: T, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask

  def messageCount: Int

  def hasMessages: Boolean =
    messageCount > 0

  def terminate(): Unit

  /**
   * Returns an Actor that merges both Actor and sends messages
   * to both Actors.
   *
   * Currently does not guarantee the order in which the messages will get processed by both actors.
   * Is order guarantee required?
   */
  def merge[B <: T](actor: ActorRef[B]): ActorRef[B] =
    new ActorRef[B] {
      def !(message: B): Unit =
        this.synchronized {
          self ! message
          actor ! message
        }

      /**
       * Sends a message to this actor with delay
       */
      def schedule(message: B, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask =
        this.synchronized {
          self.schedule(message, delay)
          actor.schedule(message, delay)
        }

      def messageCount: Int =
        self.messageCount + actor.messageCount

      def terminate(): Unit =
        this.synchronized {
          self.terminate()
          actor.terminate()
        }
    }
}

private[swaydb] object Actor {

  private[actor] val defaultMaxMessagesToProcessAtOnce = 10000
  private[actor] val defaultMaxOverflowAllowed = defaultMaxMessagesToProcessAtOnce * 10
  private[actor] val incrementDelayBy = 100.millisecond

  def fromConfig[T](config: ActorConfig)(execution: (T, Actor[T, Unit]) => Unit): ActorRef[T] =
    config match {
      case config: ActorConfig.Basic =>
        apply[T](
          maxMessagesToProcessAtOnce = config.maxMessagesToProcessAtOnce
        )(execution)(config.ec, QueueOrder.FIFO)

      case config: ActorConfig.Timer =>
        timer(
          maxMessagesToProcessAtOnce = config.maxMessagesToProcessAtOnce,
          overflowAllowed = config.maxOverflow,
          fixedDelay = config.delay
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)

      case config: ActorConfig.TimeLoop =>
        timerLoop(
          initialDelay = config.delay,
          maxMessagesToProcessAtOnce = config.maxMessagesToProcessAtOnce,
          overflowAllowed = config.maxOverflow
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)
    }

  def fromConfig[T, S](config: ActorConfig,
                       state: S)(execution: (T, Actor[T, S]) => Unit): ActorRef[T] =
    config match {
      case config: ActorConfig.Basic =>
        apply[T, S](
          state = state,
          maxMessagesToProcessAtOnce = config.maxMessagesToProcessAtOnce
        )(execution)(config.ec, QueueOrder.FIFO)

      case config: ActorConfig.Timer =>
        timer[T, S](
          state = state,
          maxMessagesToProcessAtOnce = config.maxMessagesToProcessAtOnce,
          overflowAllowed = config.maxOverflow,
          fixedDelay = config.delay
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)

      case config: ActorConfig.TimeLoop =>
        timerLoop[T, S](
          state = state,
          maxMessagesToProcessAtOnce = config.maxMessagesToProcessAtOnce,
          overflowAllowed = config.maxOverflow,
          initialDelay = config.delay
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)
    }

  /**
   * Basic stateless Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T](execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                       queueOrder: QueueOrder[T]): ActorRef[T] =
    apply[T, Unit]((), defaultMaxMessagesToProcessAtOnce)(execution)

  /**
   * Basic stateless Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T](maxMessagesToProcessAtOnce: Int)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                                        queueOrder: QueueOrder[T]): ActorRef[T] =
    apply[T, Unit]((), maxMessagesToProcessAtOnce)(execution)

  def apply[T, S](state: S)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                 queueOrder: QueueOrder[T]): ActorRef[T] =
    apply(state, defaultMaxMessagesToProcessAtOnce)(execution)

  def cache[T](maxMessagesToProcessAtOnce: Int,
               maxWeight: Int,
               weigher: T => Int)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                          queueOrder: QueueOrder[T]): ActorRef[T] =
    new Actor[T, Unit](
      state = Unit,
      minWeight = maxMessagesToProcessAtOnce,
      maxWeight = maxWeight,
      execution = execution,
      cached = true,
      weigher = Functions.safe((_: T) => 1, weigher),
      queue = ActorQueue(queueOrder),
      defaultDelay = None
    )

  def cache[T, S](state: S,
                  maxMessagesToProcessAtOnce: Int,
                  maxWeight: Int,
                  weigher: T => Int)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                          queueOrder: QueueOrder[T]): ActorRef[T] =
    new Actor[T, S](
      state = state,
      minWeight = maxMessagesToProcessAtOnce,
      maxWeight = maxWeight,
      execution = execution,
      cached = true,
      weigher = Functions.safe((_: T) => 1, weigher),
      queue = ActorQueue(queueOrder),
      defaultDelay = None
    )

  /**
   * Basic stateful Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T, S](state: S,
                  maxMessagesToProcessAtOnce: Int)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                                        queueOrder: QueueOrder[T]): ActorRef[T] =
    new Actor[T, S](
      state = state,
      minWeight = maxMessagesToProcessAtOnce,
      maxWeight = maxMessagesToProcessAtOnce,
      execution = execution,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      defaultDelay = None
    )

  /**
   * Stateless [[timer]] actor
   */
  def timer[T](fixedDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                   queueOrder: QueueOrder[T]): ActorRef[T] =
    timer(
      state = (),
      overflowAllowed = defaultMaxOverflowAllowed,
      maxMessagesToProcessAtOnce = defaultMaxMessagesToProcessAtOnce,
      fixedDelay = fixedDelay
    )(execution)

  /**
   * Stateless [[timer]] actor
   */
  def timer[T](maxMessagesToProcessAtOnce: Int,
               overflowAllowed: Int,
               fixedDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                   queueOrder: QueueOrder[T]): ActorRef[T] =
    timer(
      state = (),
      overflowAllowed = overflowAllowed,
      maxMessagesToProcessAtOnce = maxMessagesToProcessAtOnce,
      fixedDelay = fixedDelay
    )(execution)

  /**
   * Processes messages at regular intervals.
   *
   * If there are no messages in the queue the timer
   * is stopped and restarted only when a new message is added the queue.
   */
  def timer[T, S](state: S,
                  maxMessagesToProcessAtOnce: Int,
                  overflowAllowed: Int,
                  fixedDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                   queueOrder: QueueOrder[T]): ActorRef[T] =
    new Actor[T, S](
      state = state,
      minWeight = maxMessagesToProcessAtOnce,
      maxWeight = overflowAllowed,
      execution = execution,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      defaultDelay = Some(fixedDelay, false, scheduler)
    )(scheduler.ec)

  /**
   * Processes messages at regular intervals.
   *
   * If there are no messages in the queue the timer
   * is stopped and restarted only when a new message is added the queue.
   */
  def timer[T, S](state: S,
                  fixedDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                   queueOrder: QueueOrder[T]): ActorRef[T] =
    new Actor[T, S](
      state = state,
      minWeight = defaultMaxMessagesToProcessAtOnce,
      maxWeight = defaultMaxOverflowAllowed,
      execution = execution,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      defaultDelay = Some(fixedDelay, false, scheduler)
    )(scheduler.ec)

  /**
   * Stateless [[timerLoop]]
   */
  def timerLoop[T](initialDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                         queueOrder: QueueOrder[T]): ActorRef[T] =
    timerLoop(
      state = (),
      overflowAllowed = defaultMaxOverflowAllowed,
      maxMessagesToProcessAtOnce = defaultMaxMessagesToProcessAtOnce,
      initialDelay = initialDelay
    )(execution)

  /**
   * Stateless [[timerLoop]]
   */
  def timerLoop[T](initialDelay: FiniteDuration,
                   maxMessagesToProcessAtOnce: Int,
                   overflowAllowed: Int)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                 queueOrder: QueueOrder[T]): ActorRef[T] =
    timerLoop(
      state = (),
      overflowAllowed = overflowAllowed,
      maxMessagesToProcessAtOnce = maxMessagesToProcessAtOnce,
      initialDelay = initialDelay
    )(execution)

  /**
   * Checks the message queue for new messages at regular intervals
   * indefinitely and processes them if the queue is non-empty.
   *
   * Use .submit instead of !. There should be a type-safe way of handling this but.
   */
  def timerLoop[T, S](state: S,
                      initialDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                         queueOrder: QueueOrder[T]): ActorRef[T] =
    new Actor[T, S](
      state = state,
      execution = execution,
      maxWeight = defaultMaxOverflowAllowed,
      minWeight = defaultMaxMessagesToProcessAtOnce,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      defaultDelay = Some(initialDelay, true, scheduler)
    )(scheduler.ec)

  /**
   * Checks the message queue for new messages at regular intervals
   * indefinitely and processes them if the queue is non-empty.
   *
   * Use .submit instead of !. There should be a type-safe way of handling this but.
   */
  def timerLoop[T, S](state: S,
                      maxMessagesToProcessAtOnce: Int,
                      overflowAllowed: Int,
                      initialDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                         queueOrder: QueueOrder[T]): ActorRef[T] =
    new Actor[T, S](
      state = state,
      execution = execution,
      maxWeight = overflowAllowed,
      minWeight = maxMessagesToProcessAtOnce,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      defaultDelay = Some(initialDelay, true, scheduler)
    )(scheduler.ec)

  /**
   * Adjust delay based on the input parameter.
   *
   * It basically decides if the delay should be incremented or decremented to control
   * message overflow as quickly without hogging the thread for too long and without
   * keep messages in-memory for too long.
   */
  private[actor] def adjustDelay(currentQueueSize: Int,
                                 defaultQueueSize: Int,
                                 previousDelay: FiniteDuration,
                                 defaultDelay: FiniteDuration): FiniteDuration =
  //if there is no overflow increment previous delay or return the default it's overflow is controlled.
    if (currentQueueSize <= defaultQueueSize)
      (previousDelay + incrementDelayBy) min defaultDelay
    else { //else adjust overflow.
      val overflow = defaultQueueSize.toFloat / currentQueueSize
      val adjustDelay = previousDelay.toMillis * overflow
      FiniteDuration(adjustDelay.toLong, TimeUnit.MILLISECONDS)
    }

  def wire[T](impl: T)(implicit scheduler: Scheduler): WiredActor[T, Unit] =
    new WiredActor[T, Unit](impl, None, ())

  def wireTimer[T](delays: FiniteDuration, impl: T)(implicit scheduler: Scheduler): WiredActor[T, Unit] =
    new WiredActor[T, Unit](impl, Some(delays), ())
}

private[swaydb] class Actor[T, +S](val state: S,
                                   queue: ActorQueue[T],
                                   minWeight: Int,
                                   maxWeight: Int,
                                   weigher: T => Int,
                                   cached: Boolean,
                                   execution: (T, Actor[T, S]) => Unit,
                                   defaultDelay: Option[(FiniteDuration, Boolean, Scheduler)])(implicit ec: ExecutionContext) extends ActorRef[T] with LazyLogging { self =>

  private val busy = new AtomicBoolean(false)
  private val queueSize = new AtomicInteger(0)
  @volatile private var terminated = false
  @volatile private var task = Option.empty[TimerTask]

  //if initial delay is defined this actor will keep checking for messages at
  //regular interval. This interval can be updated via the execution function.
  val isLoop = defaultDelay.exists(_._2)
  //if initial detail is defined, trigger processMessages() to start the timer loop.
  if (isLoop) processMessages(runNow = false)

  override def !(message: T): Unit =
    if (terminated) {
      logger.debug("Message not processed. Terminated actor.")
    } else {
      queue add message
      val currentWeight =
        queueSize updateAndGet {
          new IntUnaryOperator {
            override def applyAsInt(operand: Int): Int =
              operand + weigher(message)
          }
        }

      if (defaultDelay.isDefined) {
        if (currentWeight >= maxWeight)
          processMessages(runNow = false)
      } else if (cached) {
        if (currentWeight >= maxWeight)
          processMessages(runNow = false)
      } else {
        processMessages(runNow = false)
      }
    }

  override def messageCount: Int =
    queueSize.get()

  override def schedule(message: T, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask =
    scheduler.task(delay)(this ! message)

  override def terminate(): Unit = {
    logger.debug(s"${this.getClass.getSimpleName} terminated.")
    terminated = true
    queue.clear()
    queueSize.set(0)
  }

  def isOverflown() =
    queueSize.get() > {
      if (cached)
        minWeight
      else
        0
    }

  /**
   * @param runNow ignores default delays and processes actor.
   */
  private def processMessages(runNow: Boolean): Unit =
    if (!terminated && (isLoop || isOverflown()) && busy.compareAndSet(false, true)) {
      //clear task
      task foreach {
        task =>
          task.cancel()
          this.task = None
      }

      if (runNow)
        Future(receive())
      else
        defaultDelay match {
          case None =>
            Future(receive())

          case Some((interval, _, _)) if interval.fromNow.isOverdue() =>
            Future(receive())

          case Some((delay, _, scheduler)) =>
            busy.set(false) //cancel so that task can be overwritten if there is a message overflow.
            task = Some(scheduler.task(delay)(processMessages(runNow = true)))
        }
    }

  private def receive(): Unit = {
    var processed = 0
    try
      while (!terminated && processed < minWeight) {
        val message = queue.poll()
        if (message != null) {
          try
            execution(message, self)
          catch {
            case exception: Throwable =>
              logger.error("Failed to process message. Continuing!", exception)
          } finally {
            processed += weigher(message)
          }
        } else {
          queueSize getAndUpdate {
            new IntUnaryOperator {
              override def applyAsInt(operand: Int): Int =
                operand - processed
            }
          }
          processed = minWeight
        }
      }
    finally {
      busy.set(false)
      processMessages(runNow = false)
    }
  }
}
