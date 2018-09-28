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

import org.scalatest.{Matchers, WordSpec}
import swaydb.core.FutureBase
import swaydb.core.util.Benchmark

import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration._

class ActorSpec extends WordSpec with Matchers with FutureBase {

  "Actor" should {

    "process messages in order of arrival" in {
      val messageCount = 1000

      case class State(processed: ListBuffer[Int])
      val state = State(ListBuffer.empty)

      val actor =
        Actor[Int, State](state) {
          case (int, self) =>
            self.state.processed += int
        }

      (1 to messageCount) foreach (actor ! _)

      //same thread, messages should arrive in order
      eventual {
        state.processed.size shouldBe messageCount
        state.processed should contain inOrderElementsOf (1 to messageCount)
      }
    }

    "process all messages in any order when submitted concurrently" in {
      val messageCount = 1000

      case class State(processed: ListBuffer[String])
      val state = State(ListBuffer.empty)

      val actor =
        Actor[String, State](state) {
          case (int, self) =>
            self.state.processed += int
        }

      (1 to messageCount) foreach {
        message =>
          Future(actor ! message.toString)
      }
      //concurrent sends, messages should arrive in any order but all messages should get processed
      eventual {
        state.processed.size shouldBe messageCount
        state.processed should contain allElementsOf (1 to messageCount).map(_.toString)
      }
    }

    "continue processing messages if execution of one message fails" in {
      case class State(processed: ListBuffer[Int])
      val state = State(ListBuffer.empty)

      val actor =
        Actor[Int, State](state) {
          case (int, self) =>
            if (int == 2) throw new Exception(s"Oh no! Failed at $int")
            self.state.processed += int
        }

      (1 to 3) foreach (actor ! _)
      //
      eventual {
        state.processed.size shouldBe 2
        //2nd message failed
        state.processed should contain only(1, 3)
      }
    }
  }

  "Actor.timer" should {

    "process all messages after a fixed interval" in {

      case class State(processed: ListBuffer[Int])
      val state = State(ListBuffer.empty)

      val actor =
        Actor.timer[Int, State](state, 1.second) {
          case (int, self) =>
            self.state.processed += int
            //delay sending message to self so that it does get processed in the same batch
            println(s"Message: $int")
            if (int >= 6)
              ()
            else
              self.schedule(int + 1, 100.millisecond)
        }

      actor ! 1
      //sleep for 5.seconds
      sleep(5.second)
      //ensure that within those 5.second interval at least 3 and no more then 5 messages get processed.
      state.processed.size should be >= 3
      state.processed.size should be <= 5
    }
  }

  //cannot run this test with other test cases as the timerLoop thread runs indefinitely.
  //  "Actor.timerLoop" should {
  //
  //    "continue processing incoming messages at the next specified interval" in {
  //      case class State(processed: ListBuffer[Int])
  //      val state = State(ListBuffer.empty)
  //
  //      var actor =
  //        Actor.timerLoop[Int, State](state, 1.second) {
  //          case (int, self) =>
  //            self.state.processed += int
  //            //after 5 messages decrement time so that it's visible
  //            val nextMessageAndDelay = if (state.processed.size <= 2) int + 1 else int - 1
  //            println("nextMessageAndDelay: " + nextMessageAndDelay)
  //
  //            self.schedule(nextMessageAndDelay, 100.millisecond)
  //            val nextDelay = int.second
  //            println("nextDelay: " + nextDelay)
  ////            if (int <= -5)
  ////              null //stop this timer
  ////            else
  ////              nextDelay
  //        }
  //
  //      actor ! 1
  //      sleep(10.seconds)
  //      state.processed.size should be >= 1
  //      state.processed.size should be <= 10
  //    }
  //  }
}
