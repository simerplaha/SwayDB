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
import java.util.concurrent.ConcurrentLinkedQueue

import org.scalatest.Matchers
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.Span._
import swaydb.{Actor, ActorQueue, Scheduler}
import swaydb.core.RunThis._
import swaydb.data.config.ActorConfig.QueueOrder

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

//@formatter:off
case class TestActor[T](implicit ec: ExecutionContext) extends Actor[T, Unit](state = (),
                                                                              queue = ActorQueue(QueueOrder.FIFO),
                                                                              stashCapacity = 0,
                                                                              weigher = _ => 1,
                                                                              cached = false,
                                                                              execution = (_, _) => None,
                                                                              interval = None,
                                                                              recovery = None) with Eventually with Matchers with ScalaFutures {
//@formatter:on

  private val queue = new ConcurrentLinkedQueue[T]

  override def schedule(message: T, delay: FiniteDuration)(implicit scheduler: Scheduler): TimerTask =
    scheduler.task(delay)(this ! message)

  override def hasMessages: Boolean =
    !queue.isEmpty

  override def !(message: T): Unit =
    queue add message

  def getMessage(timeoutDuration: FiniteDuration = 1.second): T =
    eventually(timeout(timeoutDuration)) {
      val message = Option(queue.poll())
      message should not be empty
      message.get
    }

  def expectMessage[A <: T](timeoutDuration: FiniteDuration = 1.second)(implicit classTag: ClassTag[A]): A =
    eventually(timeout(timeoutDuration)) {
      val message = Option(queue.poll())
      message should not be empty
      message.get shouldBe a[A]
      message.get.asInstanceOf[A]
    }

  def expectNoMessage(after: FiniteDuration = 100.millisecond)(implicit scheduler: Scheduler): Unit = {
    scheduler.future(after) {
      queue.isEmpty shouldBe true
    }.await(after.plus(1.second))
  }
}
