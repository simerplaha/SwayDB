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
