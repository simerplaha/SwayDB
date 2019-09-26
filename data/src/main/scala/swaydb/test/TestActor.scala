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

package swaydb.test

import java.util.TimerTask
import java.util.concurrent.ConcurrentLinkedQueue

import swaydb.data.config.ActorConfig.QueueOrder
import swaydb.{Actor, ActorQueue, IO, Scheduler}

import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, TimeoutException}
import scala.util.{Failure, Success, Try}
import scala.concurrent.duration._

/**
 * TO-DO - Move this to a test module.
 */
//@formatter:off
case class TestActor[T](implicit ec: ExecutionContext) extends Actor[T, Unit](state = (),
                                                                              queue = ActorQueue(QueueOrder.FIFO),
                                                                              stashCapacity = 0,
                                                                              weigher = _ => 1,
                                                                              cached = false,
                                                                              execution = (_, _) => None,
                                                                              interval = None,
                                                                              recovery = None) {
//@formatter:on

  private val queue = new ConcurrentLinkedQueue[T]

  override def send(message: T, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask =
    scheduler.task(delay)(this send message)

  override def hasMessages: Boolean =
    !queue.isEmpty

  override def send(message: T): Unit =
    queue add message

  private def sleep(time: FiniteDuration): Unit =
    Thread.sleep(time.toMillis)

  private def eventually(timeoutDuration: FiniteDuration,
                         interval: FiniteDuration)(f: => T): T = {
    val deadline = timeoutDuration.fromNow
    var keepTrying: Boolean = true
    var result: Either[Throwable, T] = Left(new TimeoutException("Test timed-out!"))

    while (keepTrying)
      Try(f) match {
        case Failure(exception) =>
          if (deadline.isOverdue()) {
            result = Left(exception)
            keepTrying = false
          } else {
            sleep(interval)
          }
        case Success(value) =>
          result = Right(value)
          keepTrying = false
      }

    result match {
      case Right(success) =>
        success
      case Left(failure) =>
        throw failure
    }
  }

  def getMessage(timeoutDuration: FiniteDuration = 1.second,
                 interval: FiniteDuration = 100.millisecond): T =
    eventually(timeoutDuration, interval)(Option(queue.poll()).get)

  def expectMessage(timeoutDuration: FiniteDuration = 1.second,
                    interval: FiniteDuration = 100.millisecond): T =
    eventually(timeoutDuration, interval)(Option(queue.poll()).get)

  def expectNoMessage(after: FiniteDuration = 100.millisecond)(implicit scheduler: Scheduler): Unit =
    Await.result(
      awaitable =
        scheduler.future(after) {
          Option(queue.poll()) match {
            case Some(item) =>
              throw IO.throwable(s"Has message: ${item.getClass.getSimpleName}")

            case None =>
              ()
          }
        },
      atMost = after.plus(200.millisecond)
    )
}
