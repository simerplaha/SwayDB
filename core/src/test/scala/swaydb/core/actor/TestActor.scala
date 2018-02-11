/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import java.util.concurrent.ConcurrentLinkedQueue

import org.scalatest.Matchers
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.time.Span._
import swaydb.core.FutureBase
import swaydb.core.util.Delay

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag
import scala.util.Random

object TestActor {
  def apply[T]()(implicit ec: ExecutionContext) =
    new TestActor[T](Random.nextInt())
}

class TestActor[T](id: Int)(implicit ec: ExecutionContext) extends Actor[T, Unit](id, (_, _) => None, None) with Eventually with Matchers with ScalaFutures with FutureBase {

  private val queue = new ConcurrentLinkedQueue[T]

  override def submit(message: T): Unit =
    queue offer message

  override def schedule(message: T, delay: FiniteDuration): Unit =
    throw new NotImplementedError(s"${this.getClass.getSimpleName} does not implement schedule")

  override def hasMessages: Boolean =
    !queue.isEmpty

  override def messageCount: Int =
    queue.size()

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

  def expectNoMessage(after: FiniteDuration = 100.millisecond): Unit = {
    Delay.future(after) {
      queue.isEmpty shouldBe true
    }(ec).await(after.plus(1.second))
  }
}
