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

package swaydb.java.data

import java.util.TimerTask
import java.util.function.Function

import swaydb.java.data
import swaydb.{ActorRef, Tag}

import scala.compat.java8.DurationConverters._
import scala.compat.java8.FutureConverters
import scala.concurrent.{Future => ScalaFuture}

class StatelessActor[T](actor: swaydb.ActorRef[T, Unit]) {

  implicit val futureTag = Tag.future(actor.ec)
  implicit val scheduler = swaydb.Scheduler()(actor.ec)

  def send(message: T): Unit =
    actor.send(message)

  def send(message: T, delay: java.time.Duration): TimerTask =
    actor.send(message, delay.toScala)(scheduler)

  def ask[R](message: Function[StatelessActor[R], T], delay: java.time.Duration): Actor.Asked[R] = {
    val future =
      actor.ask[R, ScalaFuture](
        message = (self: ActorRef[R, Unit]) => message(new StatelessActor(self)),
        delay = delay.toScala
      )

    val javaFuture = FutureConverters.toJava(future.task).toCompletableFuture
    new data.Actor.Asked(javaFuture, future.timer)
  }

  def totalWeight: Integer =
    actor.totalWeight

  def messageCount: Integer =
    actor.messageCount

  def hasMessages: java.lang.Boolean =
    actor.hasMessages

  def terminate(): Unit =
    actor.terminateAndClear()

  def isTerminated: Boolean =
    actor.isTerminated

  def clear(): Unit =
    actor.clear()

  def terminateAndClear(): Unit =
    actor.terminateAndClear()
}
