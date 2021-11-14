/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb

import com.typesafe.scalalogging.LazyLogging
import swaydb.ActorConfig.QueueOrder
import swaydb.Bag.Implicits._
import swaydb.data.cache.{Cache, CacheNoIO}
import swaydb.effect.Reserve
import swaydb.utils.{AtomicThreadLocalBoolean, FunctionSafe, Options}

import java.util.TimerTask
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.function.LongUnaryOperator
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

  def receiveAllForce[BAG[_], R](f: S => R)(implicit bag: Bag[BAG]): BAG[R]

  def terminate[BAG[_]]()(implicit bag: Bag[BAG]): BAG[Unit]

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

  def cacheFromConfig[T](config: ActorConfig,
                         stashCapacity: Long,
                         queueOrder: QueueOrder[T],
                         weigher: T => Int)(execution: (T, Actor[T, Unit]) => Unit): ActorHooks[T, Unit] =
    config match {
      case config: ActorConfig.Basic =>
        cache[T](
          name = config.name,
          stashCapacity = stashCapacity,
          weigher = weigher
        )(execution)(config.ec, queueOrder)

      case config: ActorConfig.Timer =>
        timerCache(
          name = config.name,
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(config.ec, queueOrder)

      case config: ActorConfig.TimeLoop =>
        timerLoopCache(
          name = config.name,
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(config.ec, queueOrder)
    }

  def cacheFromConfig[T, S](config: ActorConfig,
                            state: S,
                            stashCapacity: Long,
                            queueOrder: QueueOrder[T],
                            weigher: T => Int)(execution: (T, Actor[T, S]) => Unit): ActorHooks[T, S] =
    config match {
      case config: ActorConfig.Basic =>
        cache[T, S](
          name = config.name,
          state = state,
          stashCapacity = stashCapacity,
          weigher = weigher
        )(execution)(config.ec, queueOrder)

      case config: ActorConfig.Timer =>
        timerCache[T, S](
          name = config.name,
          state = state,
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(config.ec, queueOrder)

      case config: ActorConfig.TimeLoop =>
        timerLoopCache[T, S](
          name = config.name,
          state = state,
          stashCapacity = stashCapacity,
          interval = config.delay,
          weigher = weigher
        )(execution)(config.ec, queueOrder)
    }

  /**
   * Basic stateless Actor that processes all incoming messages sequentially.
   *
   * On each message send (!) the Actor is woken up if it's not already running.
   */
  def apply[T](name: String)(execution: (T, Actor[T, Unit]) => Unit)(implicit ec: ExecutionContext,
                                                                     queueOrder: QueueOrder[T]): ActorHooks[T, Unit] =
    apply[T, Unit](name, state = ())(execution)

  def apply[T, S](name: String, state: S)(execution: (T, Actor[T, S]) => Unit)(implicit ec: ExecutionContext,
                                                                               queueOrder: QueueOrder[T]): ActorHooks[T, S] =
    new ActorHooks[T, S](
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
                                                                          queueOrder: QueueOrder[T]): ActorHooks[T, Unit] =
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
                                                                          queueOrder: QueueOrder[T]): ActorHooks[T, S] =
    new ActorHooks[T, S](
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
                                                                                 queueOrder: QueueOrder[T]): ActorHooks[T, Unit] =
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
                                                                                 queueOrder: QueueOrder[T]): ActorHooks[T, S] =
    new ActorHooks[T, S](
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
                                                                                      queueOrder: QueueOrder[T]): ActorHooks[T, Unit] =
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
                                                                                      queueOrder: QueueOrder[T]): ActorHooks[T, S] =
    new ActorHooks[T, S](
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
                                                                                     queueOrder: QueueOrder[T]): ActorHooks[T, Unit] =
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
                                                                                     queueOrder: QueueOrder[T]): ActorHooks[T, S] =
    new ActorHooks[T, S](
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
                                                                                          queueOrder: QueueOrder[T]): ActorHooks[T, Unit] =
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
                                                                                          queueOrder: QueueOrder[T]): ActorHooks[T, S] =
    new ActorHooks[T, S](
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

  def define[T](name: String, init: DefActor[T] => T)(implicit ec: ExecutionContext): DefActor.Hooks[T] =
    DefActor(
      name = name,
      init = init,
      interval = None
    )

  def defineTimer[T](name: String,
                     interval: FiniteDuration,
                     stashCapacity: Long,
                     init: DefActor[T] => T)(implicit ec: ExecutionContext): DefActor.Hooks[T] =
    DefActor(
      name = name,
      init = init,
      interval = Some((interval, stashCapacity))
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

class Actor[-T, S] private[swaydb](val name: String,
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

  @inline private def stashMessage(message: T): Long = {
    //message weight cannot be <= 0 as that could lead to messages in queue with empty weight.
    val messageWeight = weigher(message) max 1
    queue.add((message, messageWeight))

    weight updateAndGet {
      new LongUnaryOperator {
        override def applyAsLong(currentWeight: Long): Long =
          currentWeight + messageWeight
      }
    }
  }

  override def send(message: T): Unit = {
    val currentStashed = stashMessage(message)
    //skip wakeUp if it's a timerLoop. Run only if no task is scheduled for the task.
    //if eager wakeUp on overflow is required using timer instead of timerLoop.
    if (!(isTimerLoop && task.nonEmpty))
      wakeUp(currentStashed = currentStashed)
  }

  override def ask[R, X[_]](message: ActorRef[R, Unit] => T)(implicit bag: Bag.Async[X]): X[R] =
    bag.suspend {
      val promise = Promise[R]()

      implicit val queueOrder: QueueOrder.FIFO.type =
        QueueOrder.FIFO

      val replyTo: ActorRef[R, Unit] = Actor[R](name + "_response")((response, _) => promise.success(response)).start()
      this send message(replyTo)

      bag fromPromise promise
    }

  override def ask[R, X[_]](message: ActorRef[R, Unit] => T, delay: FiniteDuration)(implicit bag: Bag.Async[X]): Actor.Task[R, X] = {
    val promise = Promise[R]()

    implicit val queueOrder = QueueOrder.FIFO

    val replyTo: ActorRef[R, Unit] = Actor[R](name + "_response")((response, _) => promise.success(response)).start()
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
