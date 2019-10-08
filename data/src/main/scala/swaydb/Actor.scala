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

package swaydb

import java.util.{Comparator, TimerTask}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CompletionStage, ExecutorService, TimeUnit}
import java.util.function.{BiConsumer, IntUnaryOperator, Function => JavaFunction}

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO.ExceptionHandler
import swaydb.data.config.ActorConfig
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.data.util.Functions

import scala.annotation.unchecked.uncheckedVariance
import scala.compat.java8.DurationConverters._
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.FutureConverters._
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}
import swaydb.data.util.Javaz._

sealed trait ActorRef[-T, S] { self =>

  def ec: ExecutionContext

  def send(message: T): Unit

  def ask[R, X[_]](message: ActorRef[R, Unit] => T)(implicit tag: Tag.Async[X]): X[R]
  def javaAsk[R](message: JavaFunction[ActorRef[R, Unit], T]@uncheckedVariance): CompletionStage[R] =
    ask[R, Future](message.asScala)(Tag.future(ec)).toJava

  /**
   * Sends a message to this actor with delay
   */
  def send(message: T, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask

  def ask[R, X[_]](message: ActorRef[R, Unit] => T, delay: FiniteDuration)(implicit scheduler: Scheduler, tag: Tag.Async[X]): Actor.Task[R, X]
  def javaAsk[R](message: JavaFunction[ActorRef[R, Unit], T]@uncheckedVariance, delay: java.time.Duration)(implicit scheduler: Scheduler): Actor.Task[R, CompletionStage] = {
    val task = ask[R, Future](message.asScala, delay.toScala)(scheduler, Tag.future(ec))
    new Actor.Task(task.task.toJava, task.timer)
  }

  def totalWeight: Int

  def messageCount: Int

  def hasMessages: Boolean =
    totalWeight > 0

  def terminate(): Unit

  def isTerminated: Boolean

  def clear(): Unit

  def terminateAndClear(): Unit

  def recover[M <: T, E: ExceptionHandler](f: (M, IO[E, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S]

  def terminateAndRecover[M <: T, E: ExceptionHandler](f: (M, IO[E, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S]

  def recoverException[M <: T](f: (M, IO[Throwable, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S] =
    recover[M, Throwable](f)

  def javaRecoverException[M <: T](function: TriFunction[M, IO[Throwable, Actor.Error], Actor[T, S], Unit]@uncheckedVariance): ActorRef[T, S] =
    recoverException[M](function.apply)

  def terminateAndRecoverException[M <: T](function: (M, IO[Throwable, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S] =
    terminateAndRecover[M, Throwable](function)

  def javaTerminateAndRecoverException(function: TriFunction[T, IO[Throwable, Actor.Error], Actor[T, S], Unit]@uncheckedVariance): ActorRef[T, S] =
    terminateAndRecoverException[T](function.apply)
}

object Actor {

  private[swaydb] val incrementDelayBy = 100.millisecond

  class Task[R, T[_]](val task: T[R], val timer: TimerTask)

  sealed trait Error
  object Error {
    case object TerminatedActor extends Actor.Error
  }

  def cacheFromConfig[T](config: ActorConfig,
                         stashCapacity: Int,
                         weigher: T => Int)(execution: (T, Actor[T, Unit]) => Unit): ActorRef[T, Unit] =
    config match {
      case config: ActorConfig.Basic =>
        cache[T](
          stashCapacity = stashCapacity,
          weigher = weigher
        )(execution)(config.ec, QueueOrder.FIFO)

      case config: ActorConfig.Timer =>
        timerCache(
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)

      case config: ActorConfig.TimeLoop =>
        timerLoopCache(
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)
    }

  def cacheFromConfig[T, S](config: ActorConfig,
                            state: S,
                            stashCapacity: Int,
                            weigher: T => Int)(execution: (T, Actor[T, S]) => Unit): ActorRef[T, S] =
    config match {
      case config: ActorConfig.Basic =>
        cache[T, S](
          state = state,
          stashCapacity = stashCapacity,
          weigher = weigher
        )(execution)(config.ec, QueueOrder.FIFO)

      case config: ActorConfig.Timer =>
        timerCache[T, S](
          state = state,
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)

      case config: ActorConfig.TimeLoop =>
        timerLoopCache[T, S](
          state = state,
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(Scheduler()(config.ec), QueueOrder.FIFO)
    }

  /**
   * Basic stateless Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T](execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                       queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    apply[T, Unit](state = ())(execution)

  def javaCreateFIFO[T](execution: BiConsumer[T, Actor[T, Unit]])(implicit ec: ExecutorService): ActorRef[T, Unit] =
    apply(execution.asScala)(ExecutionContext.fromExecutorService(ec), QueueOrder.FIFO)

  def javaCreateOrdered[T](execution: BiConsumer[T, Actor[T, Unit]])(implicit ec: ExecutorService,
                                                                     comparator: Comparator[T]): ActorRef[T, Unit] =
    apply(execution.asScala)(ExecutionContext.fromExecutorService(ec), QueueOrder.Ordered(Ordering.comparatorToOrdering(comparator)))

  def apply[T, S](state: S)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                 queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      stashCapacity = 0,
      execution = execution,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      interval = None,
      recovery = None
    )

  def javaCreateFIFO[T, S](state: S)(execution: BiConsumer[T, Actor[T, S]])(implicit ec: ExecutorService): ActorRef[T, S] =
    apply[T, S](state)(execution.asScala)(ec.asScala, QueueOrder.FIFO)

  def javaCreateOrdered[T, S](state: S)(execution: BiConsumer[T, Actor[T, S]])(implicit ec: ExecutorService,
                                                                               comparator: Comparator[T]): ActorRef[T, S] =
    apply[T, S](state)(execution.asScala)(ec.asScala, QueueOrder.Ordered(comparator.asScala))

  def cache[T](stashCapacity: Int,
               weigher: T => Int)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                          queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    cache[T, Unit](
      state = (),
      stashCapacity = stashCapacity,
      weigher = weigher
    )(execution)

  def cache[T, S](state: S,
                  stashCapacity: Int,
                  weigher: T => Int)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                          queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      stashCapacity = stashCapacity,
      execution = execution,
      cached = true,
      weigher = Functions.safe((_: T) => 1, weigher),
      queue = ActorQueue(queueOrder),
      interval = None,
      recovery = None
    )

  def timer[T](stashCapacity: Int,
               interval: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                 queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timer(
      state = (),
      stashCapacity = stashCapacity,
      interval = interval
    )(execution)

  /**
   * Processes messages at regular intervals.
   *
   * If there are no messages in the queue the timer
   * is stopped and restarted only when a new message is added the queue.
   */
  def timer[T, S](state: S,
                  stashCapacity: Int,
                  interval: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                 queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      stashCapacity = stashCapacity,
      execution = execution,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      interval = Some(new Interval(interval, scheduler, false)),
      recovery = None
    )(scheduler.ec)

  def timerCache[T](stashCapacity: Int,
                    weigher: T => Int,
                    interval: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                      queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timerCache[T, Unit](
      state = (),
      stashCapacity = stashCapacity,
      weigher = weigher,
      interval = interval
    )(execution)

  def timerCache[T, S](state: S,
                       stashCapacity: Int,
                       weigher: T => Int,
                       interval: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                      queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      stashCapacity = stashCapacity,
      execution = execution,
      cached = true,
      weigher = weigher,
      queue = ActorQueue(queueOrder),
      interval = Some(new Interval(interval, scheduler, false)),
      recovery = None
    )(scheduler.ec)

  /**
   * Stateless [[timerLoop]]
   */
  def timerLoop[T](stashCapacity: Int,
                   interval: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                     queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timerLoop(
      state = (),
      stashCapacity = stashCapacity,
      interval = interval
    )(execution)

  /**
   * Checks the message queue for new messages at regular intervals
   * indefinitely and processes them if the queue is non-empty.
   *
   * Use .submit instead of !. There should be a type-safe way of handling this but.
   */
  def timerLoop[T, S](state: S,
                      stashCapacity: Int,
                      interval: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                     queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      execution = execution,
      stashCapacity = stashCapacity,
      cached = false,
      weigher = _ => 1,
      queue = ActorQueue(queueOrder),
      interval = Some(new Interval(interval, scheduler, true)),
      recovery = None
    )(scheduler.ec)

  def timerLoopCache[T](stashCapacity: Int,
                        weigher: T => Int,
                        interval: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit scheduler: Scheduler,
                                                                                          queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timerLoopCache[T, Unit](
      state = (),
      stashCapacity = stashCapacity,
      weigher = weigher,
      interval = interval
    )(execution)

  def timerLoopCache[T, S](state: S,
                           stashCapacity: Int,
                           weigher: T => Int,
                           interval: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit scheduler: Scheduler,
                                                                                          queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      queue = ActorQueue(queueOrder),
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = true,
      execution = execution,
      interval = Some(new Interval(interval, scheduler, true)),
      recovery = None
    )(scheduler.ec)

  def wire[T](impl: T)(implicit scheduler: Scheduler): ActorWire[T, Unit] =
    new ActorWire(
      impl = impl,
      interval = None,
      state = ()
    )

  def wire[T, S](impl: T, state: S)(implicit scheduler: Scheduler): ActorWire[T, S] =
    new ActorWire(
      impl = impl,
      interval = None,
      state = state
    )

  def wireTimer[T](interval: FiniteDuration,
                   stashCapacity: Int,
                   impl: T)(implicit scheduler: Scheduler): ActorWire[T, Unit] =
    new ActorWire(
      impl = impl,
      interval = Some((interval, stashCapacity)),
      state = ()
    )

  def wireTimer[T, S](interval: FiniteDuration,
                      stashCapacity: Int,
                      impl: T,
                      state: S)(implicit scheduler: Scheduler): ActorWire[T, S] =
    new ActorWire(
      impl = impl,
      interval = Some((interval, stashCapacity)),
      state = state
    )

  /**
   * Adjust delay based on the input parameter.
   *
   * It basically decides if the delay should be incremented or decremented to control
   * message overflow as quickly without hogging the thread for too long and without
   * keep messages in-memory for too long.
   */
  private[swaydb] def adjustDelay(currentQueueSize: Int,
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
}

private class Interval(val delay: FiniteDuration, val scheduler: Scheduler, val isLoop: Boolean)

class Actor[-T, S](val state: S,
                   queue: ActorQueue[(T, Int)],
                   stashCapacity: Int,
                   weigher: T => Int,
                   cached: Boolean,
                   execution: (T, Actor[T, S]) => Unit,
                   interval: Option[Interval],
                   recovery: Option[(T, IO[Throwable, Actor.Error], Actor[T, S]) => Unit])(implicit val ec: ExecutionContext) extends ActorRef[T, S] with LazyLogging { self =>

  private val busy = new AtomicBoolean(false)
  private val weight = new AtomicInteger(0)
  private val isBasic = interval.isEmpty
  private val isTimerLoop = interval.exists(_.isLoop)
  private val isTimerNoLoop = interval.exists(!_.isLoop)

  //minimum number of message to leave if the Actor is cached.
  private val fixedStashSize =
    if (cached)
      stashCapacity
    else
      0

  @volatile private var terminated = false
  @volatile private var task = Option.empty[TimerTask]

  override def totalWeight: Int =
    weight.get()

  def messageCount: Int =
    queue.size

  override def send(message: T, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask =
    scheduler.task(delay)(self send message)

  override def send(message: T): Unit =
    if (terminated) {
      recovery foreach {
        f =>
          f(message, IO.Right(Actor.Error.TerminatedActor), self)
      }
    } else {
      //message weight cannot be <= 0 as that could lead to messages in queue with empty weight.
      val messageWeight = weigher(message) max 1
      queue.add(message, messageWeight)

      val currentStashed =
        weight updateAndGet {
          new IntUnaryOperator {
            override def applyAsInt(currentWeight: Int): Int =
              currentWeight + messageWeight
          }
        }

      //skip wakeUp if it's a timerLoop. Run only if no task is scheduled for the task.
      //if eager wakeUp on overflow is required using timer instead of timerLoop.
      if (!(isTimerLoop && task.nonEmpty))
        wakeUp(currentStashed = currentStashed)
    }

  override def ask[R, X[_]](message: ActorRef[R, Unit] => T)(implicit tag: Tag.Async[X]): X[R] = {
    val promise = Promise[R]()

    implicit val queueOrder = QueueOrder.FIFO

    val replyTo: ActorRef[R, Unit] = Actor[R]((response, _) => promise.success(response))
    this send message(replyTo)

    tag fromPromise promise
  }

  override def ask[R, X[_]](message: ActorRef[R, Unit] => T, delay: FiniteDuration)(implicit scheduler: Scheduler, tag: Tag.Async[X]): Actor.Task[R, X] = {
    val promise = Promise[R]()

    implicit val queueOrder = QueueOrder.FIFO

    val replyTo: ActorRef[R, Unit] = Actor[R]((response, _) => promise.success(response))
    val task = this.send(message(replyTo), delay)

    new Actor.Task(tag fromPromise promise, task)
  }

  @inline private def wakeUp(currentStashed: Int): Unit =
    if (isBasic)
      basicWakeUp(currentStashed)
    else
      timerWakeUp(currentStashed, this.stashCapacity)

  @inline private def basicWakeUp(currentStashed: Int) = {
    val overflow = currentStashed - fixedStashSize
    val isOverflown = overflow > 0
    //do not check for terminated actor here for receive to apply recovery.
    if (isOverflown && busy.compareAndSet(false, true))
      Future(receive(overflow))
  }

  /**
   * @param stashCapacity will mostly be >= [[Actor.stashCapacity]].
   *                      It can be < [[Actor.stashCapacity]] if the [[weigher]]
   *                      results in an item with weight > current overflow.
   *                      For example: if item's weight is 10 but overflow is 1. This
   *                      will result in the cached messages to be [[Actor.stashCapacity]] - 10.
   */
  private def timerWakeUp(currentStashed: Int, stashCapacity: Int): Unit = {
    val overflow = currentStashed - stashCapacity
    val isOverflown = overflow > 0
    val hasMessages = currentStashed > 0

    //run timer if it's an overflow or if task is empty and there is work to do.
    if ((isOverflown || (task.isEmpty && (isTimerLoop || (isTimerNoLoop && hasMessages)))) && busy.compareAndSet(false, true)) {

      //if it's overflown then wakeUp Actor now!
      try {
        if (isOverflown)
          Future(receive(overflow))

        //if there is no task schedule based on current stash so that eventually all messages get dropped without hammering.
        if (task.isEmpty)
          interval foreach {
            interval =>
              //cancel any existing task.
              task foreach {
                task =>
                  task.cancel()
                  this.task = None
              }

              //schedule a task is there are messages to process or if the Actor is a looper.
              if (currentStashed > 0 || isTimerLoop) {
                //reduce stash capacity to eventually processed stashed messages.

                val nextTask =
                  interval.scheduler.task(interval.delay) {
                    //clear the existing task so that next one gets scheduled/
                    this.task = None
                    //get the current weight during the schedule.
                    val currentStashedAtSchedule = weight.get

                    //adjust the stash capacity so that eventually all messages get processed.
                    val adjustedStashCapacity =
                      if (stashCapacity <= 0)
                        this.stashCapacity
                      else
                        (currentStashedAtSchedule / 2) max fixedStashSize //if cached stashCapacity cannot be lower than this.stashCapacity.

                    //schedule wakeUp.
                    timerWakeUp(
                      currentStashed = currentStashedAtSchedule,
                      stashCapacity = adjustedStashCapacity
                    )
                  }

                task = Some(nextTask)
              }
          }
      } finally {
        //if receive was not executed set free after a task is scheduled.
        if (!isOverflown)
          busy.set(false)
      }
    }
  }

  private def receive(overflow: Int): Unit = {
    var processedWeight = 0
    var break = false
    try
      while (!break && processedWeight < overflow) {
        val (message, messageWeight) = queue.poll()
        if (message == null)
          break = true
        else if (terminated) //apply recovery if actor is terminated.
          try
            recovery foreach {
              f =>
                f(message, IO.Right(Actor.Error.TerminatedActor), self)
            }
          finally {
            processedWeight += messageWeight
          }
        else //if the actor is not terminated, process the message.
          try
            execution(message, self)
          catch {
            case throwable: Throwable =>
              //apply recovery if failed to process messages.
              recovery foreach {
                f =>
                  f(message, IO.Left(throwable), self)
              }
          } finally {
            processedWeight += messageWeight
          }
      }
    finally {
      val currentStashed =
        weight updateAndGet {
          new IntUnaryOperator {
            override def applyAsInt(currentWeight: Int): Int =
              currentWeight - processedWeight
          }
        }
      busy.set(false)
      wakeUp(currentStashed = currentStashed)
    }
  }

  override def terminateAndRecover[M <: T, E: ExceptionHandler](f: (M, IO[E, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S] =
    recover[M, E] {
      case (message, error, actor) =>
        actor.terminate()
        f(message, error, actor)
    }

  override def recover[M <: T, E: ExceptionHandler](f: (M, IO[E, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S] =
    new Actor[T, S](
      state = state,
      queue = queue,
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = cached,
      execution = execution,
      interval = interval,
      recovery =
        Some {
          case (message: M@unchecked, error, actor) =>
            error match {
              case IO.Right(actorError) =>
                Some(f(message, IO.Right(actorError), actor))

              case IO.Left(throwable) =>
                Some(f(message, IO.Left(ExceptionHandler.toError(throwable)), actor))
            }

          case (_, error, _) =>
            error match {
              case IO.Right(Actor.Error.TerminatedActor) =>
                logger.error("Failed to recover failed message.", new Exception("Cause: Terminated Actor"))

              case IO.Left(exception: Throwable) =>
                logger.error("Failed to recover failed message.", exception)
            }
        }
    )

  override def clear(): Unit =
    queue.clear()

  override def terminate(): Unit =
    terminated = true

  def isTerminated: Boolean =
    terminated

  override def terminateAndClear(): Unit = {
    terminate()
    clear()
  }
}
