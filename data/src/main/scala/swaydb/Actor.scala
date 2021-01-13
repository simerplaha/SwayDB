/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb

import java.util.TimerTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.function.{IntUnaryOperator, LongUnaryOperator}
import com.typesafe.scalalogging.LazyLogging
import swaydb.Bag.Implicits._
import swaydb.IO.ExceptionHandler
import swaydb.data.Reserve
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.data.config.ActorConfig
import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.data.util.{AtomicThreadLocalBoolean, FunctionSafe, Options}

import scala.annotation.tailrec
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

sealed trait ActorRef[-T, S] { self =>

  def name: String

  def executionContext: ExecutionContext

  def send(message: T): Unit

  def ask[R, X[_]](message: ActorRef[R, Unit] => T)(implicit bag: Bag.Async[X]): X[R]

  /**
   * Sends a message to this actor with delay
   */
  def send(message: T, delay: FiniteDuration): TimerTask

  def ask[R, X[_]](message: ActorRef[R, Unit] => T, delay: FiniteDuration)(implicit bag: Bag.Async[X]): Actor.Task[R, X]

  def totalWeight: Long

  def messageCount: Int

  @inline def isEmpty: Boolean

  @inline def isNotEmpty: Boolean =
    !isEmpty

  def hasMessages: Boolean =
    totalWeight > 0

  def recover[M <: T, E: ExceptionHandler](f: (M, IO[E, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S]

  def recoverException[M <: T](f: (M, IO[Throwable, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S] =
    recover[M, Throwable](f)

  def receiveAllForce[BAG[_], R](f: S => R)(implicit bag: Bag[BAG]): BAG[R]

  def terminate[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit]

  def onPreTerminate(f: Actor[T, S] => Unit): ActorRef[T, S]

  def onPostTerminate(f: Actor[T, S] => Unit): ActorRef[T, S]

  def terminateAfter(timeout: FiniteDuration): ActorRef[T, S]

  def isTerminated: Boolean

  def hasRecovery: Boolean

  def clear(): Unit

  def terminateAndClear[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit]

  def terminateAndRecover[BAG[_], R](f: S => R)(implicit bag: Bag[BAG]): BAG[Option[R]]
}

object Actor {

  private[swaydb] val incrementDelayBy = 100.millisecond

  class Task[R, T[_]](val task: T[R], val timer: TimerTask)

  sealed trait Error
  object Error {
    case object TerminatedActor extends Actor.Error
  }

  def deadActor[T, S](): ActorRef[T, S] =
    new ActorRef[T, S] {
      override def name: String = "Dead actor"
      override def executionContext: ExecutionContext = throw new Exception("Dead Actor")
      override def send(message: T): Unit = throw new Exception("Dead Actor")
      override def ask[R, X[_]](message: ActorRef[R, Unit] => T)(implicit bag: Bag.Async[X]): X[R] = throw new Exception("Dead Actor")
      override def send(message: T, delay: FiniteDuration): TimerTask = throw new Exception("Dead Actor")
      override def ask[R, X[_]](message: ActorRef[R, Unit] => T, delay: FiniteDuration)(implicit bag: Bag.Async[X]): Task[R, X] = throw new Exception("Dead Actor")
      override def totalWeight: Long = throw new Exception("Dead Actor")
      override def messageCount: Int = throw new Exception("Dead Actor")
      override def isEmpty: Boolean = throw new Exception("Dead Actor")
      override def recover[M <: T, E: ExceptionHandler](f: (M, IO[E, Error], Actor[T, S]) => Unit): ActorRef[T, S] = throw new Exception("Dead Actor")
      override def isTerminated: Boolean = throw new Exception("Dead Actor")
      override def clear(): Unit = throw new Exception("Dead Actor")
      override def terminateAfter(timeout: FiniteDuration): ActorRef[T, S] = throw new Exception("Dead Actor")
      override def hasRecovery: Boolean = throw new Exception("Dead Actor")
      override def onPreTerminate(f: Actor[T, S] => Unit): ActorRef[T, S] = throw new Exception("Dead Actor")
      override def onPostTerminate(f: Actor[T, S] => Unit): ActorRef[T, S] = throw new Exception("Dead Actor")
      override def terminate[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] = throw new Exception("Dead Actor")
      override def terminateAndClear[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] = throw new Exception("Dead Actor")
      def terminateAndRecover[BAG[_], R](f: S => R)(implicit bag: Bag[BAG]): BAG[Option[R]] = throw new Exception("Dead Actor")
      def receiveAllForce[BAG[_], R](f: S => R)(implicit bag: Bag[BAG]): BAG[R] = throw new Exception("Dead Actor")
    }

  def cacheFromConfig[T](config: ActorConfig,
                         stashCapacity: Long,
                         weigher: T => Int)(execution: (T, Actor[T, Unit]) => Unit): ActorRef[T, Unit] =
    config match {
      case config: ActorConfig.Basic =>
        cache[T](
          name = config.name,
          stashCapacity = stashCapacity,
          weigher = weigher
        )(execution)(config.ec, QueueOrder.FIFO)

      case config: ActorConfig.Timer =>
        timerCache(
          name = config.name,
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(config.ec, QueueOrder.FIFO)

      case config: ActorConfig.TimeLoop =>
        timerLoopCache(
          name = config.name,
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(config.ec, QueueOrder.FIFO)
    }

  def cacheFromConfig[T, S](config: ActorConfig,
                            state: S,
                            stashCapacity: Long,
                            weigher: T => Int)(execution: (T, Actor[T, S]) => Unit): ActorRef[T, S] =
    config match {
      case config: ActorConfig.Basic =>
        cache[T, S](
          name = config.name,
          state = state,
          stashCapacity = stashCapacity,
          weigher = weigher
        )(execution)(config.ec, QueueOrder.FIFO)

      case config: ActorConfig.Timer =>
        timerCache[T, S](
          name = config.name,
          state = state,
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(config.ec, QueueOrder.FIFO)

      case config: ActorConfig.TimeLoop =>
        timerLoopCache[T, S](
          name = config.name,
          state = state,
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(config.ec, QueueOrder.FIFO)
    }

  /**
   * Basic stateless Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T](name: String)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                     queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    apply[T, Unit](name, state = ())(execution)

  def apply[T, S](name: String, state: S)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                               queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      name = name,
      state = state,
      queue = ActorQueue(queueOrder),
      stashCapacity = 0,
      weigher = _ => 1,
      cached = false,
      execution = execution,
      scheduler = Cache.noIO[Unit, Scheduler](synchronised = true, stored = true, initial = None)((_, _) => Scheduler()),
      interval = None,
      preTerminate = None,
      postTerminate = None,
      recovery = None
    )

  def cache[T](name: String,
               stashCapacity: Long,
               weigher: T => Int)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                          queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    cache[T, Unit](
      name = name,
      state = (),
      stashCapacity = stashCapacity,
      weigher = weigher
    )(execution)

  def cache[T, S](name: String,
                  state: S,
                  stashCapacity: Long,
                  weigher: T => Int)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                          queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      name = name,
      state = state,
      queue = ActorQueue(queueOrder),
      stashCapacity = stashCapacity,
      weigher = FunctionSafe.safe((_: T) => 1, weigher),
      cached = true,
      execution = execution,
      scheduler = Cache.noIO[Unit, Scheduler](synchronised = true, stored = true, initial = None)((_, _) => Scheduler()),
      interval = None,
      preTerminate = None,
      postTerminate = None,
      recovery = None
    )

  def timer[T](name: String,
               stashCapacity: Long,
               interval: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                                 queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timer(
      name = name,
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
  def timer[T, S](name: String,
                  state: S,
                  stashCapacity: Long,
                  interval: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                                 queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      name = name,
      state = state,
      queue = ActorQueue(queueOrder),
      stashCapacity = stashCapacity,
      weigher = _ => 1,
      cached = false,
      execution = execution,
      scheduler = Cache.noIO[Unit, Scheduler](synchronised = true, stored = true, initial = None)((_, _) => Scheduler()),
      interval = Some(new Interval(interval, false)),
      preTerminate = None,
      postTerminate = None,
      recovery = None
    )

  def timerCache[T](name: String,
                    stashCapacity: Long,
                    weigher: T => Int,
                    interval: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                                      queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timerCache[T, Unit](
      name = name,
      state = (),
      stashCapacity = stashCapacity,
      weigher = weigher,
      interval = interval
    )(execution)

  def timerCache[T, S](name: String,
                       state: S,
                       stashCapacity: Long,
                       weigher: T => Int,
                       interval: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                                      queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      name = name,
      state = state,
      queue = ActorQueue(queueOrder),
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = true,
      execution = execution,
      scheduler = Cache.noIO[Unit, Scheduler](synchronised = true, stored = true, initial = None)((_, _) => Scheduler()),
      interval = Some(new Interval(interval, false)),
      preTerminate = None,
      postTerminate = None,
      recovery = None
    )

  /**
   * Stateless [[timerLoop]]
   */
  def timerLoop[T](name: String,
                   stashCapacity: Long,
                   interval: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                                     queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timerLoop(
      name = name,
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
  def timerLoop[T, S](name: String,
                      state: S,
                      stashCapacity: Long,
                      interval: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                                     queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      name = name,
      state = state,
      queue = ActorQueue(queueOrder),
      stashCapacity = stashCapacity,
      weigher = _ => 1,
      cached = false,
      execution = execution,
      scheduler = Cache.noIO[Unit, Scheduler](synchronised = true, stored = true, initial = None)((_, _) => Scheduler()),
      interval = Some(new Interval(interval, true)),
      preTerminate = None,
      postTerminate = None,
      recovery = None
    )

  def timerLoopCache[T](name: String,
                        stashCapacity: Long,
                        weigher: T => Int,
                        interval: FiniteDuration)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                                          queueOrder: QueueOrder[T]): ActorRef[T, Unit] =
    timerLoopCache[T, Unit](
      name = name,
      state = (),
      stashCapacity = stashCapacity,
      weigher = weigher,
      interval = interval
    )(execution)

  def timerLoopCache[T, S](name: String,
                           state: S,
                           stashCapacity: Long,
                           weigher: T => Int,
                           interval: FiniteDuration)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                                          queueOrder: QueueOrder[T]): ActorRef[T, S] =
    new Actor[T, S](
      name = name,
      state = state,
      queue = ActorQueue(queueOrder),
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = true,
      execution = execution,
      scheduler = Cache.noIO[Unit, Scheduler](synchronised = true, stored = true, initial = None)((_, _) => Scheduler()),
      interval = Some(new Interval(interval, true)),
      preTerminate = None,
      postTerminate = None,
      recovery = None
    )

  def wire[T](name: String, init: ActorWire[T, Unit] => T)(implicit ec: ExecutionContext): ActorWire[T, Unit] =
    ActorWire(
      name = name,
      init = init,
      interval = None,
      state = ()
    )

  def wire[T, S](name: String, state: S, init: ActorWire[T, S] => T)(implicit ec: ExecutionContext): ActorWire[T, S] =
    ActorWire(
      name = name,
      init = init,
      interval = None,
      state = state
    )

  def wireTimer[T](name: String,
                   interval: FiniteDuration,
                   stashCapacity: Long,
                   init: ActorWire[T, Unit] => T)(implicit ec: ExecutionContext): ActorWire[T, Unit] =
    ActorWire(
      name = name,
      init = init,
      interval = Some((interval, stashCapacity)),
      state = ()
    )

  def wireTimer[T, S](name: String,
                      interval: FiniteDuration,
                      stashCapacity: Long,
                      state: S,
                      init: ActorWire[T, S] => T)(implicit ec: ExecutionContext): ActorWire[T, S] =
    ActorWire(
      name = name,
      init = init,
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

private class Interval(val delay: FiniteDuration, val isLoop: Boolean)

class Actor[-T, S](val name: String,
                   val state: S,
                   queue: ActorQueue[(T, Int)],
                   stashCapacity: Long,
                   weigher: T => Int,
                   cached: Boolean,
                   execution: (T, Actor[T, S]) => Unit,
                   scheduler: CacheNoIO[Unit, Scheduler],
                   interval: Option[Interval],
                   preTerminate: Option[Actor[T, S] => Unit],
                   postTerminate: Option[Actor[T, S] => Unit],
                   recovery: Option[(T, IO[Throwable, Actor.Error], Actor[T, S]) => Unit])(implicit val executionContext: ExecutionContext) extends ActorRef[T, S] with LazyLogging { self =>

  //only a single thread can invoke preTerminate.
  private val terminated = new AtomicBoolean(false)
  //used to wakeUp a sleeping actor when messages arrive or when timer is overdue.
  private val busy = Reserve.free[Unit](name + "-busy-reserve")
  //used for cases where on processing messages in thread-safe manner is important than the ordered
  //like when the actor is terminated and we just to apply recovery on all dropped messages eg: closing MMAP files.
  private val priority = AtomicThreadLocalBoolean()

  private val weight = new AtomicLong(0)
  private val isBasic = interval.isEmpty
  private val isTimerLoop = interval.exists(_.isLoop)
  private val isTimerNoLoop = interval.exists(!_.isLoop)

  //minimum number of message to leave if the Actor is cached.
  private val fixedStashSize =
    if (cached)
      stashCapacity
    else
      0

  @volatile private var task = Option.empty[TimerTask]

  override def totalWeight: Long =
    weight.get()

  def messageCount: Int =
    queue.size

  override def send(message: T, delay: FiniteDuration): TimerTask =
    scheduler.value(()).task(delay)(self send message)

  override def send(message: T): Unit = {
    //message weight cannot be <= 0 as that could lead to messages in queue with empty weight.
    val messageWeight = weigher(message) max 1
    queue.add((message, messageWeight))

    val currentStashed =
      weight updateAndGet {
        new LongUnaryOperator {
          override def applyAsLong(currentWeight: Long): Long =
            currentWeight + messageWeight
        }
      }

    //skip wakeUp if it's a timerLoop. Run only if no task is scheduled for the task.
    //if eager wakeUp on overflow is required using timer instead of timerLoop.
    if (!(isTimerLoop && task.nonEmpty))
      wakeUp(currentStashed = currentStashed)
  }

  override def ask[R, X[_]](message: ActorRef[R, Unit] => T)(implicit bag: Bag.Async[X]): X[R] =
    bag.suspend {
      val promise = Promise[R]()

      implicit val queueOrder = QueueOrder.FIFO

      val replyTo: ActorRef[R, Unit] = Actor[R](name + "_response")((response, _) => promise.success(response))
      this send message(replyTo)

      bag fromPromise promise
    }

  override def ask[R, X[_]](message: ActorRef[R, Unit] => T, delay: FiniteDuration)(implicit bag: Bag.Async[X]): Actor.Task[R, X] = {
    val promise = Promise[R]()

    implicit val queueOrder = QueueOrder.FIFO

    val replyTo: ActorRef[R, Unit] = Actor[R](name + "_response")((response, _) => promise.success(response))
    val task = this.send(message(replyTo), delay)

    new Actor.Task(bag fromPromise promise, task)
  }

  @inline private def compareSetBusy(): Boolean =
    Reserve.compareAndSet(Options.unit, busy)

  @inline private def setFree(): Unit =
    Reserve.setFree(busy)

  @inline private def wakeUp(currentStashed: Long): Unit =
    if (isTerminated) //if it's terminated ignore fixedStashSize and process messages immediately.
      terminatedWakeUp() //not using currentStashed here because terminated should always check for current latest count to be more accurate.
    else if (isBasic) //if it's not a timed actor.
      basicWakeUp(currentStashed = currentStashed)
    else
      timerWakeUp(currentStashed = currentStashed, stashCapacity = this.stashCapacity)

  @inline private def terminatedWakeUp(): Unit =
    if (isNotEmpty && compareSetBusy())
      Future(receive(overflow = Int.MaxValue, wakeUpOnComplete = true, isPriorityReceive = false))

  @inline private def basicWakeUp(currentStashed: Long): Unit = {
    val overflow = currentStashed - fixedStashSize
    val isOverflown = overflow > 0
    //do not check for terminated actor here for receive to apply recovery.
    if (isOverflown && compareSetBusy())
      Future(receive(overflow, wakeUpOnComplete = true, isPriorityReceive = false))
  }

  /**
   * @param stashCapacity will mostly be >= [[Actor.stashCapacity]].
   *                      It can be < [[Actor.stashCapacity]] if the [[weigher]]
   *                      results in an item with weight > current overflow.
   *                      For example: if item's weight is 10 but overflow is 1. This
   *                      will result in the cached messages to be [[Actor.stashCapacity]] - 10.
   */
  private def timerWakeUp(currentStashed: Long, stashCapacity: Long): Unit =
    if (!terminated.get()) {
      val overflow = currentStashed - stashCapacity
      val isOverflown = overflow > 0
      val hasMessages = currentStashed > 0

      //run timer if it's an overflow or if task is empty and there is work to do.
      if ((isOverflown || (task.isEmpty && (isTimerLoop || (isTimerNoLoop && hasMessages)))) && compareSetBusy()) {
        //if it's overflown then wakeUp Actor now!
        try {
          if (isOverflown)
            Future(receive(overflow = overflow, wakeUpOnComplete = true, isPriorityReceive = false))

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
                    scheduler.value(()).task(interval.delay) {
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
            setFree()
        }
      }
    }

  /**
   * Executes the release function in a non blocking manner.
   *
   * @param continueIfNonEmpty if true will continue executing releaseFunction until the queue is empty
   * @param releaseFunction    the function to execute that takes a Boolean indicating if this execution is
   *                           a priority execution. Priority execution don't setFree the busy Boolean.
   */
  @inline private def whileNotReceivingAsync[A](continueIfNonEmpty: Boolean)(releaseFunction: Boolean => A): Future[A] = {
    val busySet = compareSetBusy()
    val isPriorityReceive = if (busySet) false else priority.compareAndSet(expect = false, update = true)

    if (busySet || isPriorityReceive) {
      //if unable to set busy via busy AtomicBoolean then set via receiving to make this execution a priority.
      //in this functions case receiving is used because at the API level is the Await.result is used we want
      //to make sure that we process the messages which in this thread without waiting for busy to be released.
      Future(releaseFunction(isPriorityReceive)) flatMap {
        result =>
          if (!continueIfNonEmpty || isEmpty)
            Future.successful(result)
          else
            whileNotReceivingAsync(continueIfNonEmpty)(releaseFunction)
      }
    } else {
      logger.info(s"""$name is processing $messageCount tasks. isTerminated = $isTerminated.""")
      Reserve.promise(busy).future flatMap {
        _ =>
          whileNotReceivingAsync(continueIfNonEmpty)(releaseFunction)
      }
    }
  }

  /**
   * Executes the release function in a blocking manner. Do not block block until it's absolutely
   * necessary. Using the [[priority]] AtomicBoolean overwrites [[busy]] if another thread has already set
   * it to true allowing this thread to invoke the [[receive]] function.
   *
   * Why is this needed? To avoid deadlocks when blocking [[Bag]] is used and [[ExecutionContext]] has
   * a very small number of threads in it's thread pool.
   *
   * [[compareSetBusy()]] could get set to true without being able to allocate a [[Future]] because threads are being
   * blocked by this function. So to overwrite blocking, [[priority]] is used. If the Future is unable to get a thread
   * then at least we can using the current thread to process the messages which would eventually release the thread
   * allowing a Future thread to get allocated.
   *
   * @param continueIfNonEmpty if true will continue executing releaseFunction until the queue is empty
   * @param releaseFunction    the function to execute given if it's is a priority execution or normal.
   */
  @tailrec
  @inline private def whileNotReceivingSync[A](continueIfNonEmpty: Boolean)(releaseFunction: Boolean => A): Try[A] = {
    val busySet = compareSetBusy()
    val isPriorityReceive = if (busySet) false else priority.compareAndSet(expect = false, update = true)

    if (busySet || isPriorityReceive) {
      //if unable to set busy via busy AtomicBoolean then set via receiving to make this execution a priority.
      //in this functions case receiving is used so that blocking does not occur when compareSetBusy is set to
      //true but the Future is unable to spawn a thread because they are blocked.
      Try(releaseFunction(isPriorityReceive)) match {
        case success @ Success(_) =>
          if (!continueIfNonEmpty || isEmpty)
            success
          else
            whileNotReceivingSync(continueIfNonEmpty)(releaseFunction)

        case failure @ Failure(_) =>
          failure
      }
    } else {
      logger.info(s"""$name is processing $messageCount tasks. isTerminated = $isTerminated.""")
      Reserve.blockUntilFree(busy)
      whileNotReceivingSync(continueIfNonEmpty)(releaseFunction)
    }
  }

  /**
   * Release function will get executed when the thread was able to set [[busy]] to true but
   * busy.set(false) should be invoked by the input releaseFunction similar to [[receive]] function
   * under try & finally.
   *
   * @param continueIfNonEmpty will continue executing the function while the queue is non-empty.
   */
  @inline def whileNotReceiving[BAG[_], A](continueIfNonEmpty: Boolean)(releaseFunction: Boolean => A)(implicit bag: Bag[BAG]): BAG[A] =
    bag match {
      case _: Bag.Sync[BAG] =>
        bag.fromIO {
          IO.fromTry {
            whileNotReceivingSync(continueIfNonEmpty)(releaseFunction)
          }
        }

      case bag: Bag.Async[BAG] =>
        bag.suspend {
          bag.fromFuture {
            whileNotReceivingAsync(continueIfNonEmpty)(releaseFunction)
          }
        }
    }

  /**
   * Forces the Actor to process all queued messages.
   *
   * @note afterReceive can be called multiple times but only the final
   *       result will be returned.
   */
  def receiveAllForce[BAG[_], R](onReceiveComplete: S => R)(implicit bag: Bag[BAG]): BAG[R] =
    whileNotReceiving(continueIfNonEmpty = true) {
      isPriorityReceive =>
        receive(overflow = Int.MaxValue, wakeUpOnComplete = false, isPriorityReceive = isPriorityReceive)
        onReceiveComplete(state)
    }

  //receive can be invoked by busy or receiving booleans. busy is always tried first
  //or receiving is used for priority execution.
  private def receive(overflow: Long, wakeUpOnComplete: Boolean, isPriorityReceive: Boolean): Unit = {
    var processedWeight = 0L
    var break = false
    try
      //Execute if this the current thread the already set receiving to true or set to true now.
      if (priority.isExecutingThread() || priority.compareAndSet(expect = false, update = true))
        while (!break && processedWeight < overflow) {
          val messageAndWeight = queue.poll()
          if (messageAndWeight == null) {
            break = true
          } else {
            val (message, messageWeight) = messageAndWeight

            if (terminated.get())
              try //apply recovery if actor is terminated.
                recovery foreach {
                  f =>
                    f(message, IO.Right(Actor.Error.TerminatedActor), self)
                }
              finally
                processedWeight += messageWeight
            else // else if the actor is not terminated, process the message.
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
        }
    finally {
      if (processedWeight != 0)
        weight updateAndGet {
          new LongUnaryOperator {
            override def applyAsLong(currentWeight: Long): Long =
              currentWeight - processedWeight
          }
        }

      priority.setFree()

      if (!isPriorityReceive) //because receive can always be invoked through whileNotBusy
        setFree()

      //after setting busy to false fetch the totalWeight again.
      if (wakeUpOnComplete)
        wakeUp(currentStashed = totalWeight)
    }
  }

  override def recover[M <: T, E: ExceptionHandler](f: (M, IO[E, Actor.Error], Actor[T, S]) => Unit): ActorRef[T, S] =
    new Actor[T, S](
      name = name,
      state = state,
      queue = queue,
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = cached,
      execution = execution,
      interval = interval,
      scheduler = scheduler,
      preTerminate = preTerminate,
      postTerminate = postTerminate,
      recovery =
        Some {
          case (message: M@unchecked, error, actor) =>
            error match {
              case IO.Right(actorError) =>
                f(message, IO.Right(actorError), actor)

              case IO.Left(throwable) =>
                f(message, IO.Left(ExceptionHandler.toError(throwable)), actor)
            }

          case (_, error, _) =>
            error match {
              case IO.Right(Actor.Error.TerminatedActor) =>
                logger.error(s"""Actor("$name") - Failed to recover failed message.""", new Exception("Cause: Terminated Actor"))

              case IO.Left(exception: Throwable) =>
                logger.error(s"""Actor("$name") - Failed to recover failed message.""", exception)
            }
        }
    )

  override def onPreTerminate(f: Actor[T, S] => Unit): ActorRef[T, S] =
    new Actor[T, S](
      name = name,
      state = state,
      queue = queue,
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = cached,
      execution = execution,
      scheduler = scheduler,
      interval = interval,
      preTerminate = Some(f),
      postTerminate = postTerminate,
      recovery = recovery
    )

  override def onPostTerminate(f: Actor[T, S] => Unit): ActorRef[T, S] =
    new Actor[T, S](
      name = name,
      state = state,
      queue = queue,
      stashCapacity = stashCapacity,
      weigher = weigher,
      cached = cached,
      execution = execution,
      scheduler = scheduler,
      interval = interval,
      preTerminate = preTerminate,
      postTerminate = Some(f),
      recovery = recovery
    )

  private def compareSetTerminated(): Boolean = {
    val canTerminate = terminated.compareAndSet(false, true)
    if (canTerminate) task.foreach(_.cancel())
    canTerminate
  }

  /**
   * Terminates the Actor and applies [[recover]] function to all queued messages.
   *
   * If [[recover]] function  is not specified then all queues messages are cleared.
   */
  def terminateAndRecover[BAG[_], R](f: S => R)(implicit bag: Bag[BAG]): BAG[Option[R]] =
    bag.suspend {
      if (compareSetTerminated())
        runPreTerminate() flatMap {
          executedPreTermination =>
            if (recovery.isDefined)
              receiveAllForce(f) flatMap {
                result =>
                  runPostTerminate(executedPreTermination) transform {
                    _ =>
                      Option(result)
                  }
              }
            else
              terminateAndClear()
                .and(runPostTerminate(executedPreTermination))
                .and(bag.none)
        }
      else
        bag.none
    }

  override def terminateAndClear[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
    terminate().andTransform(clear())

  /**
   * We cannot invoke terminate from within the Actor itself via block because [[busy]] is already set to true
   * within [[receive]]. [[AtomicThreadLocalBoolean]] can be used instead to allow calling terminating from
   * within the Actor itself but this is currently not required anywhere.
   *
   * Currently there is no use to terminate from within the Actor itself.
   */
  def terminate[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit] =
    bag.suspend {
      if (preTerminate.isEmpty && postTerminate.isEmpty)
        clear()

      //preTerminate and postTerminate should always run even if
      //they are not set so that all pending messages in the Actor are
      //processed. Specially for windows where cleaning MMAP files are required.
      if (compareSetTerminated())
        bag.flatMap(runPreTerminate()) {
          executed =>
            runPostTerminate(executed)
        }
      else
        bag.unit
    }

  def terminateAfter(timeout: FiniteDuration): ActorRef[T, S] = {
    scheduler.value(()).task(timeout)(this.terminate[Future]())
    this
  }

  /**
   * Executed after setting the Actor to be terminated and before exection [[recovery]] and [[postTerminate]].
   *
   * @return returns the token which can be used to execute [[runPostTerminate(*)]].
   */
  private def runPreTerminate[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Boolean] =
    whileNotReceiving(continueIfNonEmpty = false) {
      isPriorityReceive =>
        try {
          preTerminate.foreach(_ (self))
          true
        } catch {
          case exception: Throwable =>
            logger.error(s"""Actor("$name") - Exception in running Pre-termination. Post termination will continue.""", exception)
            true
        } finally {
          if (!isPriorityReceive)
            setFree()
        }
    }

  private def runPostTerminate[BAG[_]](executedPreTerminate: Boolean)(implicit bag: Bag[BAG]): BAG[Unit] =
    if (executedPreTerminate)
      whileNotReceiving(continueIfNonEmpty = false) {
        isPriorityReceive =>
          try
            postTerminate.foreach(_ (self))
          catch {
            case exception: Throwable =>
              logger.error(s"""Actor("$name") - Exception in running Post-Termination. Termination will continue.""", exception)
          } finally {
            scheduler.get().foreach(_.terminate())
            if (!isPriorityReceive)
              setFree()
          }
      }
    else
      bag.unit

  override def hasRecovery: Boolean =
    recovery.isDefined

  def isTerminated: Boolean =
    terminated.get()

  override def isEmpty: Boolean =
    queue.isEmpty

  override def clear(): Unit =
    queue.clear()
}
