/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.actor

import swaydb.ActorConfig.QueueOrder
import swaydb.core.TestExecutionContext
import swaydb.testkit.RunThis._
import swaydb.{Actor, ActorRef}

import scala.concurrent.duration._

object PingPong extends App {

  implicit val ec = TestExecutionContext.executionContext

  implicit val ordering = QueueOrder.FIFO

  case class Pong(replyTo: ActorRef[Ping, State])
  case class Ping(replyTo: ActorRef[Pong, State])
  case class State(var count: Int)

  val ping =
    Actor[Ping, State]("Ping", State(0)) {
      case (message, self) =>
        self.state.count += 1
        println(s"Ping: ${self.state.count}")
        sleep(100.millisecond)
        message.replyTo send Pong(self)
    }.start()

  val pong =
    Actor[Pong, State]("Pong", State(0)) {
      case (message, self) =>
        self.state.count += 1
        println(s"Pong: ${self.state.count}")
        sleep(100.millisecond)
        message.replyTo send Ping(self)
    }.start()

  pong send Pong(ping)

  sleep(5.seconds)
}
