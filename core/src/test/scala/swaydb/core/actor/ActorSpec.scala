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

import java.util.concurrent.{ConcurrentLinkedDeque, ConcurrentSkipListSet}

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import swaydb._
import swaydb.core.CommonAssertions._
import swaydb.core.TestCaseSweeper._
import swaydb.core.{TestCaseSweeper, TestExecutionContext}
import swaydb.data.RunThis._
import swaydb.data.config.ActorConfig.QueueOrder

import scala.collection.mutable.ListBuffer
import scala.collection.parallel.CollectionConverters._
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}
import scala.jdk.CollectionConverters._
import scala.util.Try

class ActorSpec extends AnyWordSpec with Matchers {

  import swaydb.core.TestData._

  implicit val ordering = QueueOrder.FIFO

  implicit val ec = TestExecutionContext.executionContext

  "it" should {

    "process messages in order of arrival" in {
      TestCaseSweeper {
        implicit sweeper =>

          val messageCount = 100000

          case class State(processed: ListBuffer[Int])
          val state = State(ListBuffer.empty)

          val actor =
            Actor[Int, State]("", state) {
              case (int, self) =>
                self.state.processed += int
            }

          (1 to messageCount) foreach (actor send _)

          val expected = 1 to messageCount

          //same thread, messages should arrive in order
          eventual {
            state.processed.size shouldBe messageCount
            state.processed shouldBe expected
            state.processed.distinct should have size messageCount
          }
        //      sleep(10.seconds)
      }
    }

    "process all messages in any order when submitted concurrently" in {
      TestCaseSweeper {
        implicit sweeper =>

          val messageCount = 10000

          case class State(processed: ListBuffer[String])
          val state = State(ListBuffer.empty)
          val expectedMessages = (1 to messageCount).map(_.toString)

          val actor =
            Actor[String, State]("", state) {
              case (int, self) =>
                self.state.processed += int
            }.sweep()

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
    }

    "continue processing messages if execution of one message fails and has no recovery" in {
      TestCaseSweeper {
        implicit sweeper =>

          case class State(processed: ListBuffer[Int])
          val state = State(ListBuffer.empty)

          val actor =
            Actor[Int, State]("", state) {
              case (int, self) =>
                if (int == 2) throw IO.throwable(s"Oh no! Failed at $int")
                self.state.processed += int
            }.sweep()

          (1 to 3) foreach (actor send _)
          //
          eventual {
            state.processed.size shouldBe 2
            //2nd message failed
            state.processed should contain only(1, 3)
          }
      }
    }

    "continue processing messages if execution of one message fails and has recovery" in {
      TestCaseSweeper {
        implicit sweeper =>

          case class State(processed: ListBuffer[Int], recovered: ListBuffer[Int])
          val state = State(ListBuffer.empty, ListBuffer.empty)

          val actor =
            Actor[Int, State]("", state) {
              case (int, self) =>
                if (int == 2) throw IO.throwable(s"Oh no! Failed at $int")
                self.state.processed += int
            }.sweep() recoverException[Int] {
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
    }

    //FIXME - currently cannot terminate in recovery since the actor is already busy.
    //
    //    "terminate actor in recovery" in {
    //      TestCaseSweeper {
    //        implicit sweeper =>
    //          import sweeper._
    //          case class State(processed: ListBuffer[Int], errors: ListBuffer[Int])
    //          val state = State(ListBuffer.empty, ListBuffer.empty)
    //
    //          val actor =
    //            Actor[Int, State]("", state) {
    //              case (int, self) =>
    //                if (int == 2) throw IO.throwable(s"Oh no! Failed at $int")
    //                self.state.processed += int
    //            }.recoverException[Int] {
    //              case (message, error: IO[Throwable, Actor.Error], actor) =>
    //                actor.state.errors += message
    //                if (message == 2)
    //                  actor.terminate[Bag.Less]()
    //                else
    //                  error.right.value shouldBe Actor.Error.TerminatedActor
    //            }.sweep()
    //
    //          (1 to 4) foreach (actor send _)
    //          //
    //          eventual {
    //            //2nd message failed
    //            state.processed should contain only 1
    //            state.errors should contain only(2, 3, 4)
    //          }
    //      }
    //    }

    "stop processing messages on termination" in {
      TestCaseSweeper {
        implicit sweeper =>

          case class State(processed: ConcurrentSkipListSet[Int], failed: ConcurrentSkipListSet[Int])
          val state = State(new ConcurrentSkipListSet[Int](), new ConcurrentSkipListSet[Int]())

          val actor =
            Actor[Int, State]("", state) {
              case (int, self) =>
                self.state.processed add int
            }.sweep() recover[Int, Throwable] {
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
                actor.terminate[Glass]()
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
  }

  "timer" should {

    "process all messages after a fixed interval and terminate" in {
      TestCaseSweeper {
        implicit sweeper =>

          case class State(processed: ListBuffer[Int])
          val state = State(ListBuffer.empty)

          val actor =
            Actor.timer[Int, State]("", state, 100, 1.second) {
              case (int, self) =>
                println("Message: " + int)
                self.state.processed += int
                //delay sending message to self so that it does value processed in the same batch
                self.send(int + 1, 500.millisecond)
            }.sweep()

          actor send 1
          sleep(7.second)
          //ensure that within those 5.second interval at least 3 and no more then 5 messages value processed.
          state.processed.size should be >= 3
          state.processed.size should be <= 6

          actor.terminate[Glass]() //terminate the actor
          val countAfterTermination = state.processed.size //this is the current message count
          sleep(2.second) //sleep
          state.processed.size shouldBe countAfterTermination //no messages are processed after termination
      }
    }
  }

  "timerLoop" should {

    "continue processing incoming messages at the next specified interval" in {
      TestCaseSweeper {
        implicit sweeper =>

          case class State(processed: ListBuffer[Int])
          val state = State(ListBuffer.empty)

          val actor =
            Actor.timerLoop[Int, State]("", state, stashCapacity = 100, 1.second) {
              case (int, self) =>
                self.state.processed += int
                //after 5 messages decrement time so that it's visible
                val nextMessageAndDelay = if (state.processed.size <= 2) int + 1 else int - 1
                println("nextMessageAndDelay: " + nextMessageAndDelay)

                self.send(nextMessageAndDelay, 100.millisecond)
                val nextDelay = int.second
                println("nextDelay: " + nextDelay)
            }.sweep()

          actor send 1
          sleep(10.seconds)
          state.processed.size should be >= 1
          state.processed.size should be <= 10
          actor.terminate[Glass]()
          val sizeAfterTerminate = state.processed.size
          sleep(1.second)
          //after termination the size does not change. i.e. no new messages are processed and looper is stopped.
          state.processed.size shouldBe sizeAfterTerminate
      }
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
        TestCaseSweeper {
          implicit sweeper =>

            val state = State(new ConcurrentSkipListSet[Int](), new ConcurrentSkipListSet[Int]())

            val actor =
              Actor.cache[Int, State]("", state, 10, _ => 1) {
                case (int, self) =>
                  self.state.processed add int
              }.sweep()

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

            actor.terminate[Glass]()
        }
      }
    }
  }

  "timerCache" should {
    "not drop stash" in {
      TestCaseSweeper {
        implicit sweeper =>

          @volatile var runs = 0

          val stash = randomIntMax(100) max 1

          val actor =
            Actor.timerCache[Int]("", stashCapacity = stash, weigher = _ => 1, interval = 5.second) {
              (int: Int, self: ActorRef[Int, Unit]) =>
                runs += 1
            }.sweep()

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

          actor.terminate[Glass]()
      }
    }
  }

  "timerLoopCache" should {
    "not drop stash" in {
      TestCaseSweeper {
        implicit sweeper =>

          @volatile var checks = 0

          //      val stash = randomIntMax(100) max 1
          val stash = 10

          val actor =
            Actor.timerLoopCache[Int]("", stash, _ => 1, 5.second) {
              (_: Int, self: ActorRef[Int, Unit]) =>
                checks += 1
            }.sweep()

          (1 to 10000).par foreach {
            i =>
              actor send i
              Thread.sleep(randomIntMax(10))
          }

          eventual(40.seconds) {
            checks should be > 1000
            actor.messageCount shouldBe stash
          }

          actor.terminate[Glass]()
      }
    }
  }

  "ask" should {
    case class ToInt(string: String)(val replyTo: ActorRef[Int, Unit])
    implicit val futureTag = Bag.future(ec)

    "ask" in {
      TestCaseSweeper {
        implicit sweeper =>

          val actor =
            Actor[ToInt]("") {
              (message, _) =>
                message.replyTo send message.string.toInt
            }.sweep()

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

          actor.terminate[Glass]()
      }
    }
  }

  "it" should {

    def assertActor(actor: ActorRef[() => Any, Unit]) = {
      (1 to 100000) foreach {
        i =>
          if (i % 10000 == 0) println(s"Message number: $i")
          val promise = Promise[Unit]()
          actor.send(() => promise.tryComplete(Try(i)))
          promise.future.await(2.seconds)
      }

      actor.totalWeight shouldBe 0
      actor.messageCount shouldBe 0

      actor.terminate[Glass]()
    }

    "process all messages" when {
      "basic" in {
        TestCaseSweeper {
          implicit sweeper =>

            val actor = Actor[() => Any]("") {
              (run, _) =>
                run()
            }.sweep()

            assertActor(actor)
        }
      }

      "timer" in {
        TestCaseSweeper {
          implicit sweeper =>

            val actor = Actor.timer[() => Any]("", 0, 1.second) {
              (run, _) =>
                run()
            }.sweep()

            assertActor(actor)
        }
      }
    }
  }

  "terminateAndRecover" should {
    "do not apply recovery if no recovery function is specified" in {
      runThis(100.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            val queue = new ConcurrentLinkedDeque[Int]()

            val actor =
              Actor[Int]("") {
                case (message, self) =>
                  queue add message
              }.sweep()

            (1 to 100) foreach {
              i =>
                actor send i
            }

            val result = actor.terminateAndRecover[Future, Unit](_ => ())

            (101 to 200) foreach {
              i =>
                actor send i
            }

            //all messages are still cleared
            eventual(10.seconds) {
              actor.messageCount shouldBe 0
            }

            //Future does not throw exception
            result.await(10.seconds)

            actor.terminate[Glass]()
        }
      }
    }

    "apply recovery if recovery function is specified" in {
      TestCaseSweeper {
        implicit sweeper =>

          //Stores successful messages
          val success = new ConcurrentLinkedDeque[Int]()
          //Stores failure messages
          val recovered = new ConcurrentLinkedDeque[Int]()

          val actor =
            Actor[Int]("") {
              case (message, self) =>
                success add message
            }.sweep() recoverException[Int] {
              case (message, error: IO[Throwable, Actor.Error], self) =>
                error match {
                  case IO.Right(error) =>
                    error shouldBe Actor.Error.TerminatedActor
                    //message sent after termination should be >= 101
                    message should be >= 101
                    recovered add message

                  case IO.Left(exception) =>
                    exception.printStackTrace()
                    fail(exception)
                }
            }

          (1 to 100) foreach {
            i =>
              actor send i
          }

          eventual(20.seconds) {
            success.asScala.toList shouldBe (1 to 100).toList
            recovered.asScala shouldBe empty
          }

          //force termination so that recovery happens.
          actor.terminate[Glass]()

          (101 to 200) foreach {
            i =>
              actor send i
          }

          //terminate here does not work because it's in the future
          //and the second send messages will get returned as success
          //so actor.terminate() is invoked before terminateAndRecover.
          val future = actor.terminateAndRecover[Future, Unit](_ => ())

          eventual(20.seconds) {
            success.asScala.toList shouldBe (1 to 100).toList
            recovered.asScala.toList shouldBe (101 to 200).toList
          }

          //does not throw exception
          future.await(20.seconds)

          actor.terminate[Glass]()
      }
    }

    "apply recovery on concurrent message does not result in duplicate messages" in {
      runThis(100.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>

            // number of messages to send
            val maxMessages = 1000

            //Stores successful messages
            val success = new ConcurrentLinkedDeque[Int]()
            //Stores failure messages
            val recovered = new ConcurrentLinkedDeque[Int]()

            //randomly create Actors.
            val noRecoveryActor =
              eitherOne(
                Actor[Int]("Basic Actor") {
                  case (message, self) =>
                    success add message
                },

                Actor.timer[Int](
                  name = "Timer Actor",
                  stashCapacity = randomIntMax(maxMessages),
                  interval = randomIntMax(5).second
                ) {
                  case (message, self) =>
                    success add message
                },

                Actor.timerLoop[Int](
                  name = "TimerLoop Actor",
                  stashCapacity = randomIntMax(maxMessages),
                  interval = randomIntMax(5).seconds
                ) {
                  case (message, self) =>
                    success add message
                },

                Actor.timerCache[Int](
                  name = "TimerCache Actor",
                  stashCapacity = randomIntMax(maxMessages),
                  weigher = _ => 1,
                  interval = randomIntMax(5).seconds
                ) {
                  case (message, self) =>
                    success add message
                },

                Actor.timerLoopCache[Int](
                  name = "TimerLoopCache Actor",
                  stashCapacity = randomIntMax(maxMessages),
                  weigher = _ => 1,
                  interval = randomIntMax(5).seconds
                ) {
                  case (message, self) =>
                    success add message
                }
              )

            val actor =
              noRecoveryActor
                .sweep()
                .recoverException[Int] {
                  case (message, error: IO[Throwable, Actor.Error], self) =>
                    error match {
                      case IO.Right(error) =>
                        error shouldBe Actor.Error.TerminatedActor
                        recovered add message

                      case IO.Left(exception) =>
                        exception.printStackTrace()
                        fail(exception)
                    }
                }

            //not yet terminated
            actor.isTerminated shouldBe false

            val messages = (1 to maxMessages).toList

            println(s"Sending $maxMessages messages concurrently to ${actor.name}.")

            //concurrently send messages and randomly invoke terminateAndRecover
            messages.par.foreach {
              i =>
                actor send i
              // See history - This is removed because now an actor can only terminated once by
              // a single thread.
              //                if (Random.nextDouble() < 0.001 && terminated.compareAndSet(false, true))
              //                  actor.terminateAndRecover[Future](0.seconds)
              //                else
              //                  Futures.unit
            }

            //if it's not already terminates then terminate it so that cache Actors also drop their
            //stashed messages.
            actor.terminateAndRecover[Future, Unit](_ => ()).await(1.minute)

            println(s"Messages sent. Success: ${success.size()}. Recovered messages: ${recovered.size()}")

            actor.isTerminated shouldBe true

            //there should be no duplicate messages either in success or recovered messages.
            val allMessages = (success.asScala ++ recovered.asScala).toList
            allMessages.sorted shouldBe messages
            allMessages should have size maxMessages
        }
      }
    }
  }
}
