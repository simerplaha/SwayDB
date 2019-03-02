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

import java.util.concurrent.ConcurrentSkipListSet
import org.scalatest.{Matchers, WordSpec}
import scala.collection.mutable.ListBuffer
import scala.concurrent.Future
import scala.concurrent.duration._
import swaydb.core.RunThis._

class ActorSpec extends WordSpec with Matchers {

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

    "stop processing messages on termination" in {
      case class State(processed: ConcurrentSkipListSet[Int])
      val state = State(new ConcurrentSkipListSet[Int]())

      val actor =
        Actor[Int, State](state) {
          case (int, self) =>
            self.state.processed add int
        }

      (1 to 3) foreach {
        i =>
          actor ! i
          if (i == 2)
            actor.terminate()
      }
      eventual(10.seconds) {
        state.processed.size shouldBe 2
        //2nd message failed
        state.processed should contain only(1, 2)
      }
    }
  }

  "timer" should {

    "process all messages after a fixed interval and terminate" in {

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
              self.schedule(int + 1, 500.millisecond)
        }

      actor ! 1
      sleep(7.second)
      //ensure that within those 5.second interval at least 3 and no more then 5 messages get processed.
      state.processed.size should be >= 3
      state.processed.size should be <= 6

      actor.terminate() //terminate the actor
      val countAfterTermination = state.processed.size //this is the current message count
      sleep(2.second) //sleep
      state.processed.size shouldBe countAfterTermination //no messages are processed after termination
    }
  }

  "timerLoop" should {

    "continue processing incoming messages at the next specified interval" in {
      case class State(processed: ListBuffer[Int])
      val state = State(ListBuffer.empty)

      val actor =
        Actor.timerLoop[Int, State](state, 1.second) {
          case (int, self) =>
            self.state.processed += int
            //after 5 messages decrement time so that it's visible
            val nextMessageAndDelay = if (state.processed.size <= 2) int + 1 else int - 1
            println("nextMessageAndDelay: " + nextMessageAndDelay)

            self.schedule(nextMessageAndDelay, 100.millisecond)
            val nextDelay = int.second
            println("nextDelay: " + nextDelay)
        }

      actor ! 1
      sleep(10.seconds)
      state.processed.size should be >= 1
      state.processed.size should be <= 10
      actor.terminate()
      val sizeAfterTerminate = state.processed.size
      sleep(1.second)
      //after termination the size does not change. i.e. no new messages are processed and looper is stopped.
      state.processed.size shouldBe sizeAfterTerminate
    }
  }

  "adjustDelay" should {
    "not increment delay if there is no overflow" in {
      (1 to 100) foreach {
        queueSize =>
          Actor.adjustDelay(
            currentQueueSize = queueSize,
            defaultQueueSize = 100,
            previousDelay = 10.seconds,
            defaultDelay = 10.seconds
          ) shouldBe 10.seconds
      }
    }

    "adjust delay if the overflow is half" in {
      Actor.adjustDelay(
        currentQueueSize = 101,
        defaultQueueSize = 100,
        previousDelay = 10.seconds,
        defaultDelay = 10.seconds
      ) shouldBe 9.9.seconds

      Actor.adjustDelay(
        currentQueueSize = 200,
        defaultQueueSize = 100,
        previousDelay = 10.seconds,
        defaultDelay = 10.seconds
      ) shouldBe 5.seconds

      Actor.adjustDelay(
        currentQueueSize = 1000,
        defaultQueueSize = 100,
        previousDelay = 10.seconds,
        defaultDelay = 10.seconds
      ) shouldBe 1.second
    }

    "start increment adjusted delay when overflow is controlled" in {
      (1 to 100) foreach {
        queueSize =>
          Actor.adjustDelay(
            currentQueueSize = queueSize,
            defaultQueueSize = 100,
            previousDelay = 5.seconds,
            defaultDelay = 10.seconds
          ) shouldBe 5.seconds + Actor.incrementDelayBy
      }
    }

    "reset to default when overflow is controlled" in {
      (1 to 100) foreach {
        queueSize =>
          Actor.adjustDelay(
            currentQueueSize = queueSize,
            defaultQueueSize = 100,
            previousDelay = 9.9.seconds,
            defaultDelay = 10.seconds
          ) shouldBe 10.seconds
      }
    }
  }
}
