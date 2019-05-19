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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.actor

import com.typesafe.scalalogging.LazyLogging
import java.util.TimerTask
import java.util.concurrent.{ConcurrentLinkedQueue, TimeUnit}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.function.IntUnaryOperator
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}
import swaydb.core.util.Delay
import swaydb.data.IO

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
    * Used in timer actors where messages get processed after a delay interval.
    */
  def submit(message: T): Unit

  /**
    * Sends a message to this actor with delay
    */
  def schedule(message: T, delay: FiniteDuration): TimerTask

  def hasMessages: Boolean

  def terminate(): Unit
}

private[swaydb] object Actor {

  private[actor] val incrementDelayBy = 100.millisecond

  /**
    * Basic stateless Actor that processes all incoming messages sequentially.
    *
    * On each message send (!) the Actor is woken up if it's not already running.
    */
  def apply[T](execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext): ActorRef[T] =
    apply[T, Unit]()(execution)

  /**
    * Basic stateful Actor that processes all incoming messages sequentially.
    *
    * On each message send (!) the Actor is woken up if it's not already running.
    */
  def apply[T, S](state: S)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext): ActorRef[T] =
    new Actor[T, S](
      state = state,
      execution =
        (message, actor) => {
          execution(message, actor)
          None
        },
      defaultDelay = None
    )

  /**
    * Stateless [[timer]] actor
    */
  def timer[T](fixedDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext): ActorRef[T] =
    timer((), fixedDelay)(execution)

  /**
    * Processes messages at regular intervals.
    *
    * If there are no messages in the queue the timer
    * is stopped and restarted only when a new message is added the queue.
    */
  def timer[T, S](state: S,
                  fixedDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext): ActorRef[T] =
    new Actor[T, S](
      state = state,
      execution =
        (message, actor) => {
          execution(message, actor)
          Some(fixedDelay)
        },
      defaultDelay = None
    )

  /**
    * Stateless [[timerLoop]]
    */
  def timerLoop[T](initialDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext): ActorRef[T] =
    timerLoop((), initialDelay)(execution)

  /**
    * Checks the message queue for new messages at regular intervals
    * indefinitely and processes them if the queue is non-empty.
    *
    * Use .submit instead of !. There should be a type-safe way of handling this but.
    */
  def timerLoop[T, S](state: S,
                      initialDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext): ActorRef[T] =
    new Actor[T, S](
      state = state,
      execution =
        (message, actor) => {
          Some(execution(message, actor))
        },
      defaultDelay = Some(initialDelay)
    )

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

  def wire[T](impl: T)(implicit ec: ExecutionContext): WiredActor[T] =
    new WiredActor[T](impl, None)

  def wireTimer[T](delays: FiniteDuration, impl: T)(implicit ec: ExecutionContext): WiredActor[T] =
    new WiredActor[T](impl, Some(delays))
}

private[swaydb] class Actor[T, +S](val state: S,
                                   execution: (T, Actor[T, S]) => Unit,
                                   private val defaultDelay: Option[FiniteDuration])(implicit ec: ExecutionContext) extends ActorRef[T] with LazyLogging { self =>

  private val busy = new AtomicBoolean(false)
  private val queue = new ConcurrentLinkedQueue[T]
  private val queueSize = new AtomicInteger(0)
  @volatile private var terminated = false

  val maxMessagesToProcessAtOnce = 10000
  //if initial delay is defined this actor will keep checking for messages at
  //regular interval. This interval can be updated via the execution function.
  val continueIfEmpty = defaultDelay.isDefined
  //if initial detail is defined, trigger processMessages() to start the timer loop.
  if (continueIfEmpty) processMessages()

  override def !(message: T): Unit =
    if (!terminated) {
      queue offer message
      queueSize.incrementAndGet()
      processMessages()
    }

  private def clearMessages(): Unit = {
    queue.clear()
    queueSize.set(0)
  }

  override def hasMessages: Boolean =
    queue.isEmpty

  override def schedule(message: T, delay: FiniteDuration): TimerTask =
    Delay.task(delay)(this ! message)

  override def submit(message: T): Unit =
    if (queueSize.get() >= maxMessagesToProcessAtOnce * 10) {
      self ! message
    } else {
      queue offer message
      queueSize.incrementAndGet()
    }

  override def terminate(): Unit = {
    logger.debug(s"${this.getClass.getSimpleName} terminated.")
    terminated = true
    clearMessages()
  }

  private def processMessages(): Unit =
    if (!terminated && (continueIfEmpty || !queue.isEmpty) && busy.compareAndSet(false, true))
      Future(receive((queueSize.get() - maxMessagesToProcessAtOnce) max maxMessagesToProcessAtOnce))

  private def receive(max: Int): Unit = {
    var processed = 0
    try {
      while (!terminated && processed < max) {
        val message = queue.poll
        if (message != null) {
          IO(execution(message, self))
          processed += 1
        } else {
          queueSize.updateAndGet {
            new IntUnaryOperator {
              override def applyAsInt(operand: Int): Int =
                operand - processed
            }
          }
          processed = max
        }
      }
    } finally {
      busy.set(false)
      processMessages()
    }
  }
}