/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import java.util.concurrent.ConcurrentSkipListSet

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb.IOValues._
import swaydb._
import swaydb.core.RunThis._
import swaydb.data.config.ActorConfig.QueueOrder

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.collection.parallel.CollectionConverters._

class ActorSpec extends AnyWordSpec with Matchers {

  import swaydb.core.TestData._

  implicit val ordering = QueueOrder.FIFO

  "Actor" should {

    "process messages in order of arrival" in {
      val messageCount = 10000

      case class State(processed: ListBuffer[Int])
      val state = State(ListBuffer.empty)

      val actor =
        Actor[Int, State](state) {
          case (int, self) =>
            self.state.processed += int
        }

      (1 to messageCount) foreach (actor send _)

      //same thread, messages should arrive in order
      eventual {
        state.processed.size shouldBe messageCount
        state.processed should contain inOrderElementsOf (1 to messageCount)
        state.processed.distinct should have size messageCount
      }
      //      sleep(10.seconds)
    }

    "process all messages in any order when submitted concurrently" in {
      val messageCount = 10000

      case class State(processed: ListBuffer[String])
      val state = State(ListBuffer.empty)
      val expectedMessages = (1 to messageCount).map(_.toString)

      val actor =
        Actor[String, State](state) {
          case (int, self) =>
            self.state.processed += int
        }

      (1 to messageCount).par foreach {
        message =>
          actor send message.toString
      }
      //concurrent sends, messages should arrive in any order but all messages should value processed
      eventual {
        state.processed.size shouldBe messageCount
        state.processed should contain allElementsOf expectedMessages
        state.processed.distinct should have size messageCount
      }
      //      sleep(10.seconds)
    }

    "continue processing messages if execution of one message fails and has no recovery" in {
      case class State(processed: ListBuffer[Int])
      val state = State(ListBuffer.empty)

      val actor =
        Actor[Int, State](state) {
          case (int, self) =>
            if (int == 2) throw IO.throwable(s"Oh no! Failed at $int")
            self.state.processed += int
        }

      (1 to 3) foreach (actor send _)
      //
      eventual {
        state.processed.size shouldBe 2
        //2nd message failed
        state.processed should contain only(1, 3)
      }
    }

    "continue processing messages if execution of one message fails and has recovery" in {
      case class State(processed: ListBuffer[Int], recovered: ListBuffer[Int])
      val state = State(ListBuffer.empty, ListBuffer.empty)

      val actor =
        Actor[Int, State](state) {
          case (int, self) =>
            if (int == 2) throw IO.throwable(s"Oh no! Failed at $int")
            self.state.processed += int
        } recoverException[Int] {
          case (message, error: IO[Throwable, Actor.Error], actor) =>
            message shouldBe 2
            actor.state.recovered += message
        }

      (1 to 3) foreach (actor send _)
      //
      eventual {
        //2nd message failed
        state.processed should contain only(1, 3)
        state.recovered should contain only 2
      }
    }

    "terminate actor in recovery" in {
      case class State(processed: ListBuffer[Int], errors: ListBuffer[Int])
      val state = State(ListBuffer.empty, ListBuffer.empty)

      val actor =
        Actor[Int, State](state) {
          case (int, self) =>
            if (int == 2) throw IO.throwable(s"Oh no! Failed at $int")
            self.state.processed += int
        } recoverException[Int] {
          case (message, error: IO[Throwable, Actor.Error], actor) =>
            actor.state.errors += message
            if (message == 2)
              actor.terminate()
            else
              error.right.value shouldBe Actor.Error.TerminatedActor
        }

      (1 to 4) foreach (actor send _)
      //
      eventual {
        //2nd message failed
        state.processed should contain only 1
        state.errors should contain only(2, 3, 4)
      }
    }

    "stop processing messages on termination" in {
      case class State(processed: ConcurrentSkipListSet[Int], failed: ConcurrentSkipListSet[Int])
      val state = State(new ConcurrentSkipListSet[Int](), new ConcurrentSkipListSet[Int]())

      val actor =
        Actor[Int, State](state) {
          case (int, self) =>
            self.state.processed add int
        } recover[Int, Throwable] {
          case (message, error, actor) =>
            actor.state.failed add message
        }

      (1 to 10) foreach {
        i =>
          actor send i
          if (i == 2) {
            while (state.processed.size() != 2) {
              sleep(100.millisecond)
            }
            actor.terminate()
          }
      }
      eventual(10.seconds) {
        state.processed.size shouldBe 2
        //2nd message failed
        state.processed should contain only(1, 2)
        state.failed should contain only (3 to 10: _*)
      }
    }
  }

  "timer" should {

    "process all messages after a fixed interval and terminate" in {

      case class State(processed: ListBuffer[Int])
      val state = State(ListBuffer.empty)

      val actor =
        Actor.timer[Int, State](state, 100, 1.second) {
          case (int, self) =>
            println("Message: " + int)
            self.state.processed += int
            //delay sending message to self so that it does value processed in the same batch
            self.send(int + 1, 500.millisecond)
        }

      actor send 1
      sleep(7.second)
      //ensure that within those 5.second interval at least 3 and no more then 5 messages value processed.
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
        Actor.timerLoop[Int, State](state, stashCapacity = 100, 1.second) {
          case (int, self) =>
            self.state.processed += int
            //after 5 messages decrement time so that it's visible
            val nextMessageAndDelay = if (state.processed.size <= 2) int + 1 else int - 1
            println("nextMessageAndDelay: " + nextMessageAndDelay)

            self.send(nextMessageAndDelay, 100.millisecond)
            val nextDelay = int.second
            println("nextDelay: " + nextDelay)
        }

      actor send 1
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

  "cache" should {
    case class State(processed: ConcurrentSkipListSet[Int], failed: ConcurrentSkipListSet[Int])

    "not process message until overflow" when {
      "basic actor is used" in {
        val state = State(new ConcurrentSkipListSet[Int](), new ConcurrentSkipListSet[Int]())

        val actor =
          Actor.cache[Int, State](state, 10, _ => 1) {
            case (int, self) =>
              self.state.processed add int
          }

        (1 to 10) foreach (actor send _)

        sleep(2.seconds)
        state.processed shouldBe empty

        actor send 11
        eventual(state.processed should contain only 1)
        actor send 12
        eventual(state.processed should contain only(1, 2))

        (1 to 10000).par foreach (actor send _)
        eventual(actor.messageCount shouldBe 10)

        sleep(5.second)
        actor.messageCount shouldBe 10

        actor.terminate()
      }
    }
  }

  "timerCache" should {
    "not drop stash" in {
      implicit val scheduler = Scheduler()

      @volatile var runs = 0

      val stash = randomIntMax(100) max 1

      val actor =
        Actor.timerCache[Int](stashCapacity = stash, weigher = _ => 1, interval = 5.second) {
          (int: Int, self: ActorRef[Int, Unit]) =>
            runs += 1
        }

      (1 to 10000).par foreach {
        i =>
          actor send i
        //          Thread.sleep(randomIntMax(10))
      }

      println(s"StashSize: $stash")

      eventual(5.seconds) {
        runs should be > 1000
        actor.messageCount shouldBe stash
      }

      actor.terminate()
    }
  }

  "timerLoopCache" should {
    "not drop stash" in {
      implicit val scheduler = Scheduler()

      @volatile var checks = 0

      //      val stash = randomIntMax(100) max 1
      val stash = 10

      val actor =
        Actor.timerLoopCache[Int](stash, _ => 1, 5.second) {
          (_: Int, self: ActorRef[Int, Unit]) =>
            checks += 1
        }

      (1 to 10000).par foreach {
        i =>
          actor send i
          Thread.sleep(randomIntMax(10))
      }

      eventual(40.seconds) {
        checks should be > 1000
        actor.messageCount shouldBe stash
      }

      actor.terminate()
    }
  }

  "ask" should {
    case class ToInt(string: String)(val replyTo: ActorRef[Int, Unit])
    implicit val futureTag = Bag.future(ec)

    "ask" in {
      val actor =
        Actor[ToInt] {
          (message, _) =>
            message.replyTo send message.string.toInt
        }

      import scala.concurrent.duration._

      val futures =
        Future.sequence {
          (1 to 100) map {
            request =>
              actor ask ToInt(request.toString) map {
                response =>
                  (request, response)
              }
          }
        }

      val responses = Await.result(futures, 10.second)
      responses should have size 100
      responses foreach {
        case (request, response) =>
          response shouldBe request
      }
    }
  }

  //  "play" in {
  //    implicit val scheduler = Scheduler()
  //
  //    //    val actor =
  //    //      Actor[Int] {
  //    //        (int: Int, self: ActorRef[Int, Unit]) =>
  //    //          println(int)
  //    //      }
  //
  //    val actor =
  //      Actor.timerLoop[Int](stashCapacity = 10, interval = 5.second) {
  //        (int: Int, self: ActorRef[Int, Unit]) =>
  //          println(s"Received: $int")
  //      }
  //
  //    (1 to 20) foreach {
  //      i =>
  //        actor ! i
  //        Thread.sleep(1000)
  //    }
  //
  //    //    val actor =
  //    //      Actor.timerCache[Int](10, _ => 1, 5.second) {
  //    //        (int: Int, self: ActorRef[Int, Unit]) =>
  //    //          self.messageCount shouldBe 10
  //    //          println(s"Received: $int")
  //    //      }
  //
  //    Future {
  //      sleep(40.seconds)
  //      var int = 0
  //      while (true) {
  //        scheduler.future(1.second)(actor ! int)
  //        Thread.sleep(100)
  //        int = int + 1
  //      }
  //    }
  //
  //    sleep(100.seconds)
  //  }
}
