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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.actor

import scala.concurrent.duration._
import swaydb.core.RunThis._
object PingPong extends App {

  case class Pong(replyTo: ActorRef[Ping])
  case class Ping(replyTo: ActorRef[Pong])
  case class State(var count: Int)

  val ping =
    Actor[Ping, State](State(0)) {
      case (message, self) =>
        self.state.count += 1
        println(s"Ping: ${self.state.count}")
        sleep(100.millisecond)
        message.replyTo ! Pong(self)
    }

  val pong =
    Actor[Pong, State](State(0)) {
      case (message, self) =>
        self.state.count += 1
        println(s"Pong: ${self.state.count}")
        sleep(100.millisecond)
        message.replyTo ! Ping(self)
    }

  pong ! Pong(ping)

  sleep(5.seconds)
}
