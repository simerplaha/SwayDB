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
import swaydb.IO
import swaydb.IO.ExceptionHandler
import swaydb.core.util.{Functions, Scheduler}
import swaydb.data.config.ActorConfig
import swaydb.data.config.ActorConfig.QueueOrder

import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future}

private[swaydb] sealed trait ActorRef[-T, S] { self =>

  /**
   * Submits message to Actor's queue and starts message execution if not already running.
   */
  def !(message: T): Unit

  /**
   * Sends a message to this actor with delay
   */
  def schedule(message: T, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask

  def totalWeight: Int

  def messageCount: Int

  def hasMessages: Boolean =
    totalWeight > 0

  def terminate(): Unit

  def recover[M <: T, E: ExceptionHandler](f: (M, IO[E, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S]

  /**
   * Returns an Actor that merges both Actor and sends messages
   * to both Actors.
   *
   * Currently does not guarantee the order in which the messages will get processed by both actors.
   * Is order guarantee required?
   */
  def merge[TT <: T](actor: ActorRef[TT, S]): ActorRef[TT, S] =
    new ActorRef[TT, S] {
      def !(message: TT): Unit =
        this.synchronized {
          self ! message
          actor ! message
        }

      /**
       * Sends a message to this actor with delay
       */
      def schedule(message: TT, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask =
        this.synchronized {
          self.schedule(message, delay)
          actor.schedule(message, delay)
        }

      def totalWeight: Int =
        self.totalWeight + actor.totalWeight

      def terminate(): Unit =
        this.synchronized {
          self.terminate()
          actor.terminate()
        }

      def messageCount: Int =
        self.messageCount + actor.messageCount

      override def recover[M <: TT, E: ExceptionHandler](f: (M, IO[E, Actor.Error], Actor[TT, S]) => Unit): ActorRef[TT, S] =
        throw new NotImplementedError("Recovery on merged Actors is currently not supported.")
    }
}

private[swaydb] object Actor {

  private[actor] val defaultMaxWeight = 1000
  private[actor] val incrementDelayBy = 100.millisecond

  sealed trait Error
  object Error {
    case object TerminatedActor extends Actor.Error
  }

  def fromConfig[T](config: ActorConfig)(execution: (T, Actor[T, Unit]) => Unit): ActorRef[T, Unit] =
    config match {
      case config: ActorConfig.Basic =>
        apply[T](
          maxWeight = defaultMaxWeight
        )(execution)(config.ec, QueueOrder.FIFO)

      case config: ActorConfig.Timer =>
        timer(
          maxWeight = defaultMaxWeight,
          fixedDelay = config.delay
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)

      case config: ActorConfig.TimeLoop =>
        timerLoop(
          initialDelay = config.delay,
          maxWeight = defaultMaxWeight
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)
    }

  def fromConfig[T, S](config: ActorConfig,
                       state: S)(execution: (T, Actor[T, S]) => Unit): ActorRef[T, S] =
    config match {
      case config: ActorConfig.Basic =>
        apply[T, S](
          state = state,
          maxWeight = defaultMaxWeight
        )(execution)(config.ec, QueueOrder.FIFO)

      case config: ActorConfig.Timer =>
        timer[T, S](
          state = state,
          maxWeight = defaultMaxWeight,
          fixedDelay = config.delay
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)

      case config: ActorConfig.TimeLoop =>
        timerLoop[T, S](
          state = state,
          maxWeight = defaultMaxWeight,
          initialDelay = config.delay
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)
    }

  /**
   * Basic stateless Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T](execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                       queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    apply[T, Unit](
      state = (),
      maxWeight = defaultMaxWeight
    )(execution)

  /**
   * Basic stateless Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T](maxWeight: Int)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                       queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    apply[T, Unit](
      state = (),
      maxWeight = maxWeight
    )(execution)

  def apply[T, S](state: S)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                 queueOrder: QueueOrder[T]): ActorRef[T, S] =
    apply(
      state = state,
      maxWeight = defaultMaxWeight
    )(execution)

  def cache[T](maxWeight: Int,
               weigher: T => Int)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                          queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    new Actor[T, Unit](
      state = Unit,
      maxWeight = maxWeight,
      execution = execution,
      cached = true,
      weigher = Functions.safe((_: T) => 1, weigher),
      queue = ActorQueue(queueOrder),
      defaultDelay = None,
      recovery = None
    )

  def cache[T, S](state: S,
                  maxWeight: Int,
                  weigher: T => Int)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                          queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      maxWeight = maxWeight,
      execution = execution,
      cached = true,
      weigher = Functions.safe((_: T) => 1, weigher),
      queue = ActorQueue(queueOrder),
      defaultDelay = None,
      recovery = None
    )

  /**
   * Basic stateful Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T, S](state: S,
                  maxWeight: Int)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                       queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      maxWeight = maxWeight,
      execution = execution,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      defaultDelay = None,
      recovery = None
    )

  /**
   * Stateless [[timer]] actor
   */
  def timer[T](fixedDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                   queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timer(
      state = (),
      maxWeight = defaultMaxWeight,
      fixedDelay = fixedDelay
    )(execution)

  /**
   * Stateless [[timer]] actor
   */
  def timer[T](maxWeight: Int,
               fixedDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                   queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timer(
      state = (),
      maxWeight = maxWeight,
      fixedDelay = fixedDelay
    )(execution)

  /**
   * Processes messages at regular intervals.
   *
   * If there are no messages in the queue the timer
   * is stopped and restarted only when a new message is added the queue.
   */
  def timer[T, S](state: S,
                  maxWeight: Int,
                  fixedDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                   queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      maxWeight = maxWeight,
      execution = execution,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      defaultDelay = Some(fixedDelay, scheduler),
      recovery = None
    )(scheduler.ec)

  /**
   * Processes messages at regular intervals.
   *
   * If there are no messages in the queue the timer
   * is stopped and restarted only when a new message is added the queue.
   */
  def timer[T, S](state: S,
                  fixedDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                   queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      maxWeight = defaultMaxWeight,
      execution = execution,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      defaultDelay = Some(fixedDelay, scheduler),
      recovery = None
    )(scheduler.ec)

  /**
   * Stateless [[timer]] actor
   */
  def timerCache[T](maxWeight: Int,
                    fixedDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                        queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timer(
      state = (),
      maxWeight = maxWeight,
      fixedDelay = fixedDelay
    )(execution)

  /**
   * Stateless [[timerLoop]]
   */
  def timerLoop[T](initialDelay: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                         queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timerLoop(
      state = (),
      maxWeight = defaultMaxWeight,
      initialDelay = initialDelay
    )(execution)

  /**
   * Stateless [[timerLoop]]
   */
  def timerLoop[T](initialDelay: FiniteDuration,
                   maxWeight: Int)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                           queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timerLoop(
      state = (),
      maxWeight = maxWeight,
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
                                                                                         queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      queue = ActorQueue(queueOrder),
      maxWeight = defaultMaxWeight,
      weigher = _ => 1,
      cached = false,
      execution = execution,
      defaultDelay = Some(initialDelay, scheduler),
      recovery = None
    )(scheduler.ec)

  /**
   * Checks the message queue for new messages at regular intervals
   * indefinitely and processes them if the queue is non-empty.
   *
   * Use .submit instead of !. There should be a type-safe way of handling this but.
   */
  def timerLoop[T, S](state: S,
                      maxWeight: Int,
                      initialDelay: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                         queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      execution = execution,
      maxWeight = maxWeight,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      defaultDelay = Some(initialDelay, scheduler),
      recovery = None
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

private[swaydb] class Actor[-T, S](val state: S,
                                   queue: ActorQueue[(T, Int)],
                                   maxWeight: Int,
                                   weigher: T => Int,
                                   cached: Boolean,
                                   execution: (T, Actor[T, S]) => Unit,
                                   defaultDelay: Option[(FiniteDuration, Scheduler)],
                                   recovery: Option[(T, IO[Throwable, Actor.Error], Actor[T, S]) => Unit])(implicit ec: ExecutionContext) extends ActorRef[T, S] with LazyLogging { self =>

  private val busy = new AtomicBoolean(false)
  private val weight = new AtomicInteger(0)
  @volatile private var terminated = false
  @volatile private var task = Option.empty[TimerTask]
  //minimum number of message to leave if the Actor is cached.
  private val cacheWeight =
    if (cached)
      maxWeight
    else
      0

  override def totalWeight: Int =
    weight.get()

  def messageCount: Int =
    queue.size

  override def schedule(message: T, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask =
    scheduler.task(delay)(this ! message)

  override def !(message: T): Unit =
    if (terminated) {
      recovery foreach {
        f =>
          f(message, IO.Right(Actor.Error.TerminatedActor), this)
      }
    } else {
      val messageWeight = weigher(message)
      queue.add(message, messageWeight)
      val currentWeight =
        weight updateAndGet {
          new IntUnaryOperator {
            override def applyAsInt(operand: Int): Int =
              operand + messageWeight
          }
        }

      processMessages(
        scheduledRun = false,
        currentWeight = currentWeight
      )
    }

  /**
   * @param scheduledRun ignores default delays and processes actor.
   */
  private def processMessages(scheduledRun: Boolean, currentWeight: => Int): Unit = {
    val overflow = currentWeight - cacheWeight
    val isOverflown = overflow > 0
    if (!terminated && isOverflown && busy.compareAndSet(false, true)) {
      task foreach {
        task =>
          task.cancel()
          this.task = None
      }

      if (scheduledRun && isOverflown)
        Future(receive(overflow))
      else
        defaultDelay match {
          case None =>
            Future(receive(overflow))

          case Some((delay, scheduler)) =>
            busy.set(false) //cancel so that task can be overwritten if there is a message overflow.
            task = Some(scheduler.task(delay)(processMessages(scheduledRun = true, currentWeight = weight.get)))
        }
    }
  }

  private def receive(overflow: Int): Unit = {
    var processedWeight = 0
    var break = false
    try
      while (!terminated && !break && processedWeight < overflow) {
        val (message, messageWeight) = queue.poll()
        if (message != null) {
          try
            execution(message, self)
          catch {
            case throwable: Throwable =>
              recovery foreach {
                f =>
                  f(message, IO.Left(throwable), this)
              }
          } finally {
            processedWeight += messageWeight
          }
        } else {
          break = true
        }
      }
    finally {
      busy.set(false)
      val newWeight =
        weight updateAndGet {
          new IntUnaryOperator {
            override def applyAsInt(operand: Int): Int =
              operand - processedWeight
          }
        }
      processMessages(scheduledRun = false, newWeight)
    }
  }

  override def recover[M <: T, E: ExceptionHandler](f: (M, IO[E, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      queue = queue,
      maxWeight = maxWeight,
      weigher = weigher,
      cached = cached,
      execution = execution,
      defaultDelay = defaultDelay,
      recovery =
        Some {
          case (message: M@unchecked, error, actor) =>
            try
              error match {
                case IO.Right(actorError) =>
                  Some(f(message, IO.Right(actorError), actor))

                case IO.Left(throwable) =>
                  Some(f(message, IO.Left(ExceptionHandler.toError(throwable)), actor))
              }
            catch {
              case exception: Exception =>
                logger.debug("Failed to recover failed message.", exception)
            }

          case (_, error, _) =>
            error match {
              case IO.Right(Actor.Error.TerminatedActor) =>
                logger.debug("Failed to process message.", new Exception("Cause: Terminated Actor"))

              case IO.Left(exception: Throwable) =>
                logger.debug("Failed to process message.", exception)
            }
        }
    )

  override def terminate(): Unit = {
    terminated = true
    queue.terminate()
    weight.set(0)
  }
}
