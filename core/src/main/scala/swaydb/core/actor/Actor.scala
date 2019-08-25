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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import java.util.function.IntUnaryOperator

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.util.Scheduler
import swaydb.data.config.ActorConfig

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

private[swaydb] sealed trait ActorRef[-T] {

  /**
   * Submits message to Actor's queue and starts message execution if not already running.
   */
  def !(message: T): Unit

  /**
   * Submits message to Actor's queue but does not trigger the message execution.
   *
   * Used when guaranteed submission of the message is the only requirement.
   *
   * Used in timer actors where messages value processed after a delay interval.
   */
  def submit(message: T): Unit

  /**
   * Sends a message to this actor with delay
   */
  def schedule(message: T, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask

  def hasMessages: Boolean

  def messages: Int

  def terminate(): Unit
}

private[swaydb] object Actor {

  private[actor] val defaultMaxMessagesToProcessAtOnce = 10000
  private[actor] val defaultMaxOverflowAllowed = defaultMaxMessagesToProcessAtOnce * 10
  private[actor] val incrementDelayBy = 100.millisecond

  def fromConfig[T](config: ActorConfig)(execution: (T, Actor[T, Unit]) => Unit): ActorRef[T] =
    config match {
      case actorQueue: ActorConfig.Basic =>
        apply[T](
          maxMessagesToProcessAtOnce = actorQueue.maxMessagesToProcessAtOnce
        )(execution)(actorQueue.ec)

      case actorQueue: ActorConfig.Timer =>
        timer(
          maxMessagesToProcessAtOnce = actorQueue.maxMessagesToProcessAtOnce,
          overflowAllowed = actorQueue.maxOverflow,
          fixedDelay = actorQueue.delay
        )(execution)(Scheduler.create()(actorQueue.ec))

      case actorQueue: ActorConfig.TimeLoop =>
        timerLoop(
          initialDelay = actorQueue.delay,
          maxMessagesToProcessAtOnce = actorQueue.maxMessagesToProcessAtOnce,
          overflowAllowed = actorQueue.maxOverflow
        )(execution)(Scheduler.create()(actorQueue.ec))
    }

  def fromConfig[T, S](config: ActorConfig,
                       state: S)(execution: (T, Actor[T, S]) => Unit): ActorRef[T] =
    config match {
      case actorQueue: ActorConfig.Basic =>
        apply[T, S](
          state = state,
          maxMessagesToProcessAtOnce = actorQueue.maxMessagesToProcessAtOnce
        )(execution)(actorQueue.ec)

      case actorQueue: ActorConfig.Timer =>
        timer[T, S](
          state = state,
          maxMessagesToProcessAtOnce = actorQueue.maxMessagesToProcessAtOnce,
          overflowAllowed = actorQueue.maxOverflow,
          fixedDelay = actorQueue.delay
        )(execution)(Scheduler.create()(actorQueue.ec))

      case actorQueue: ActorConfig.TimeLoop =>
        timerLoop[T, S](
          state = state,
          maxMessagesToProcessAtOnce = actorQueue.maxMessagesToProcessAtOnce,
          overflowAllowed = actorQueue.maxOverflow,
          initialDelay = actorQueue.delay
        )(execution)(Scheduler.create()(actorQueue.ec))
    }

  /**
   * Basic stateless Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T](execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext): ActorRef[T] =
    apply[T, Unit]((), defaultMaxMessagesToProcessAtOnce)(execution)

  /**
   * Basic stateless Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T](maxMessagesToProcessAtOnce: Int)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext): ActorRef[T] =
    apply[T, Unit]((), maxMessagesToProcessAtOnce)(execution)

  def apply[T, S](state: S)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext): ActorRef[T] =
    apply(state, defaultMaxMessagesToProcessAtOnce)(execution)

  /**
   * Basic stateful Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T, S](state: S,
                  maxMessagesToProcessAtOnce: Int)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext): ActorRef[T] =
    new Actor[T, S](
      state = state,
      maxMessagesToProcessAtOnce = maxMessagesToProcessAtOnce,
      overflow = maxMessagesToProcessAtOnce,
      execution = execution,
      defaultDelay = None
    )

  /**
   * Stateless [[timer]] actor
   */
  def timer[T](fixedDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler): ActorRef[T] =
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
               fixedDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler): ActorRef[T] =
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
                  fixedDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler): ActorRef[T] =
    new Actor[T, S](
      state = state,
      maxMessagesToProcessAtOnce = maxMessagesToProcessAtOnce,
      overflow = overflowAllowed,
      execution = execution,
      defaultDelay = Some(fixedDelay, false, scheduler)
    )(scheduler.ec)

  /**
   * Processes messages at regular intervals.
   *
   * If there are no messages in the queue the timer
   * is stopped and restarted only when a new message is added the queue.
   */
  def timer[T, S](state: S,
                  fixedDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler): ActorRef[T] =
    new Actor[T, S](
      state = state,
      maxMessagesToProcessAtOnce = defaultMaxMessagesToProcessAtOnce,
      overflow = defaultMaxOverflowAllowed,
      execution = execution,
      defaultDelay = Some(fixedDelay, false, scheduler)
    )(scheduler.ec)

  /**
   * Stateless [[timerLoop]]
   */
  def timerLoop[T](initialDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler): ActorRef[T] =
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
                   overflowAllowed: Int)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler): ActorRef[T] =
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
                      initialDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler): ActorRef[T] =
    new Actor[T, S](
      state = state,
      execution = execution,
      overflow = defaultMaxOverflowAllowed,
      maxMessagesToProcessAtOnce = defaultMaxMessagesToProcessAtOnce,
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
                      initialDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler): ActorRef[T] =
    new Actor[T, S](
      state = state,
      execution = execution,
      overflow = overflowAllowed,
      maxMessagesToProcessAtOnce = maxMessagesToProcessAtOnce,
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
                                   maxMessagesToProcessAtOnce: Int,
                                   overflow: Int,
                                   execution: (T, Actor[T, S]) => Unit,
                                   defaultDelay: Option[(FiniteDuration, Boolean, Scheduler)])(implicit ec: ExecutionContext) extends ActorRef[T] with LazyLogging { self =>

  private val busy = new AtomicBoolean(false)
  private val queue = new ConcurrentLinkedQueue[T]
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
      queue offer message
      queueSize.incrementAndGet()
      processMessages(runNow = false)
    }

  override def hasMessages: Boolean =
    queue.isEmpty

  override def messages: Int =
    queueSize.get()

  override def schedule(message: T, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask =
    scheduler.task(delay)(this ! message)

  override def submit(message: T): Unit =
    if (queueSize.get() >= overflow) {
      self ! message
    } else {
      queue offer message
      queueSize.incrementAndGet()
    }

  override def terminate(): Unit = {
    logger.debug(s"${this.getClass.getSimpleName} terminated.")
    terminated = true
    queue.clear()
    queueSize.set(0)
  }

  private def clearTask(): Unit =
    task foreach {
      task =>
        task.cancel()
        this.task = None
    }

  /**
   * @param runNow ignores default delays and processes actor.
   */
  private def processMessages(runNow: Boolean): Unit =
    if (!terminated && (isLoop || queueSize.get() > 0) && busy.compareAndSet(false, true)) {
      clearTask()

      if (runNow)
        Future(receive(maxMessagesToProcessAtOnce))
      else
        defaultDelay match {
          case None =>
            Future(receive(maxMessagesToProcessAtOnce))

          case Some((interval, _, _)) if interval.fromNow.isOverdue() =>
            Future(receive(maxMessagesToProcessAtOnce))

          case Some((delay, _, scheduler)) =>
            busy.set(false) //cancel so that task can be overwritten if there is a message overflow.
            task = Some(scheduler.task(delay)(processMessages(runNow = true)))
        }
    }

  private def receive(max: Int): Unit = {
    var processed = 0
    try
      while (!terminated && processed < max) {
        val message = queue.poll
        if (message != null) {
          try
            execution(message, self)
          catch {
            case exception: Throwable =>
              logger.error("Failed to process message. Continuing!", exception)
          }
          processed += 1
        } else {
          queueSize getAndUpdate {
            new IntUnaryOperator {
              override def applyAsInt(operand: Int): Int =
                operand - processed
            }
          }
          processed = max
        }
      }
    finally {
      busy.set(false)
      processMessages(runNow = false)
    }
  }
}
