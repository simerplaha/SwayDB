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

package swaydb.test

import swaydb.ActorConfig.QueueOrder
import swaydb.{Actor, ActorQueue, IO, Scheduler}

import java.util.concurrent.ConcurrentLinkedQueue
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.{Await, ExecutionContext, TimeoutException}
import scala.util.{Failure, Success, Try}

/**
 * TO-DO - Move this to a test module.
 */
//@formatter:off
case class TestActor[T]()(implicit override val executionContext: ExecutionContext) extends Actor[T, Unit](
                                                                                           name = "TestActor",
                                                                                           state = (),
                                                                                           queue = ActorQueue(QueueOrder.FIFO),
                                                                                           stashCapacity = 0,
                                                                                           weigher = _ => 1,
                                                                                           cached = false,
                                                                                           execution = (_, _) => None,
                                                                                           interval = None,
                                                                                           scheduler = null,
                                                                                           preTerminate = None,
                                                                                           postTerminate = None,
                                                                                           recovery = None) {
//@formatter:on

  private val queue = new ConcurrentLinkedQueue[T]

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
              throw new Exception(s"Has message: ${item.getClass.getSimpleName}")

            case None =>
              ()
          }
        },
      atMost = after.plus(200.millisecond)
    )
}
