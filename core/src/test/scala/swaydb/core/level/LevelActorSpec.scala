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

package swaydb.core.level

import java.nio.file.Paths

import org.scalamock.scalatest.MockFactory
import swaydb.core.TestBase
import swaydb.core.actor.TestActor
import swaydb.core.level.LevelException.ContainsOverlappingBusySegments
import swaydb.core.level.actor.LevelCommand._
import swaydb.core.level.actor.LevelState.{PushScheduled, Pushing, Sleeping, WaitingPull}
import swaydb.core.level.actor._
import swaydb.core.segment.Segment
import swaydb.core.util.{Delay, TryUtil}
import swaydb.order.KeyOrder
import swaydb.core.util.FiniteDurationUtil._
import swaydb.data.config.Dir

import scala.concurrent.duration._
import scala.util.{Failure, Success}

/**
  * Unit tests for [[LevelActor]]'s functions.
  */
class LevelActorSpec extends TestBase with MockFactory {

  implicit val ordering = KeyOrder.default

  "LevelActor.wakeUp" should {
    "return PushJob and PushScheduled state when the current state is Sleeping and there is lower level" in {
      implicit val level = mock[LevelActorAPI]
      level.hasNextLevel _ expects() returning true
      level.nextPushDelay _ expects() returning 1.second

      implicit val state = Sleeping(hasSmallSegments = false, task = None)

      LevelActor.wakeUp should contain((PushScheduled(state.hasSmallSegments, state.task), PushTask(1.second, Push)))
    }

    "return PushJob on Sleeping if the level does not have lower level but has small segments" in {
      implicit val level = mock[LevelActorAPI]
      level.hasNextLevel _ expects() returning false
      level.nextPushDelay _ expects() returning 1.second

      implicit val state = Sleeping(hasSmallSegments = true, task = None)

      LevelActor.wakeUp should contain((PushScheduled(state.hasSmallSegments, state.task), PushTask(1.second, Push)))
    }

    "not return PushJob if there is no lower level and no small segments" in {
      implicit val level = mock[LevelActorAPI]
      level.hasNextLevel _ expects() returning false
      implicit val state = Sleeping(hasSmallSegments = false, task = None)

      LevelActor.wakeUp shouldBe empty
    }

    "not return PushJob when the state is Pushing" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Pushing(List.empty, false, None, None)
      LevelActor.wakeUp shouldBe empty
    }

    "not return PushJob when the state is PushScheduled" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = PushScheduled(false, None)
      LevelActor.wakeUp shouldBe empty
    }

    "not return PushJob when the state is WaitingPull" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = WaitingPull(false, None)
      LevelActor.wakeUp shouldBe empty
    }

  }

  "LevelActor.collapseSmallSegments" should {
    "set hasSmallSegments to false on successful collapse" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = true, task = None)
      implicit val self = TestActor[LevelCommand]()

      level.nextBatchSize _ expects() returning 10
      level.collapseAllSmallSegments _ expects 10 returning Success(0)

      LevelActor.collapseSmallSegments shouldBe Sleeping(hasSmallSegments = false, task = None)
    }

    "set hasSmallSegments to true on successful and if success returned > 0" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = true, task = None)
      implicit val self = TestActor[LevelCommand]()

      level.nextBatchSize _ expects() returning 10
      level.collapseAllSmallSegments _ expects 10 returning Success(1)

      LevelActor.collapseSmallSegments shouldBe Sleeping(hasSmallSegments = true, task = None)
    }

    "set hasSmallSegments to false if collapse fails and scheduled a task to continue collapsing" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = true, task = None)
      implicit val self = TestActor[LevelCommand]()

      level.nextBatchSize _ expects() returning 10
      level.collapseAllSmallSegments _ expects 10 returning Failure(LevelException.ContainsOverlappingBusySegments)

      LevelActor.collapseSmallSegments shouldBe state.copyWithHasSmallSegments(false)

      self.expectMessage[CollapseSmallSegments](LevelActor.unexpectedFailureRetry + 1.second) shouldBe LevelCommand.CollapseSmallSegments
    }
  }

  "LevelActor.clearExpiredKeyValues" should {
    "schedule a ClearExpiredKeyValues if the expiry time hasTimeLeft and execute clearExpiredKeyValues on overdue" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = true, task = None)
      implicit val self = TestActor[LevelCommand]()

//      level.paths _ expects() returning PathsDistributor(Seq(Dir(Paths.get("testPath"), 1)), () => Seq.empty) repeat 3.times
      level.clearExpiredKeyValues _ expects() returning Success()

      val deadline = 5.second.fromNow
      val newState = LevelActor.clearExpiredKeyValues(deadline)
      newState shouldBe a[Sleeping]
      newState.task shouldBe defined

      //message scheduled
      self.expectMessage[LevelCommand.ClearExpiredKeyValues](LevelActor.unexpectedFailureRetry + 5.second) shouldBe LevelCommand.ClearExpiredKeyValues(deadline)

      //invoke function with the current state which should execute clearExpiredKeyValues
      LevelActor.clearExpiredKeyValues(deadline).task shouldBe empty
    }

    "cancel previously scheduled task if new deadline is overdue than currently scheduled deadline" in {
      @volatile var taskRan = false
      val initialTaskDelay = 2.seconds
      val existingTask = Delay.task(initialTaskDelay) {
        taskRan = true
      }

      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = true, task = Some(existingTask))
      implicit val self = TestActor[LevelCommand]()

//      level.paths _ expects() returning PathsDistributor(Seq(Dir(Paths.get("testPath"), 1)), () => Seq.empty) repeat 2.times
      level.clearExpiredKeyValues _ expects() returning Success()

      val deadline = 0.second.fromNow
      val newState = LevelActor.clearExpiredKeyValues(deadline)
      newState shouldBe a[Sleeping]
      newState.task shouldBe empty

      //sleep for initial task delay and check that it did not get executed
      sleep(initialTaskDelay + 1.second)
      taskRan shouldBe false
    }

    "cancel previously scheduled task if new deadline is occurring earlier than currently scheduled deadline" in {
      @volatile var taskRan = false
      val initialTaskDelay = 5.seconds
      val existingTask = Delay.task(initialTaskDelay) {
        taskRan = true
      }

      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = true, task = Some(existingTask))
      implicit val self = TestActor[LevelCommand]()

//      level.paths _ expects() returning PathsDistributor(Seq(Dir(Paths.get("testPath"), 1)), () => Seq.empty) repeat 3.times
      level.clearExpiredKeyValues _ expects() returning Success()

      val deadline = 2.second.fromNow
      val newState = LevelActor.clearExpiredKeyValues(deadline)
      newState shouldBe a[Sleeping]
      newState.task shouldBe defined

      self.expectMessage[LevelCommand.ClearExpiredKeyValues](5.second) shouldBe LevelCommand.ClearExpiredKeyValues(deadline)

      LevelActor.clearExpiredKeyValues(deadline).task shouldBe empty
      //sleep for initial task delay and check that it did not get executed
      sleep(initialTaskDelay + 1.second)
      taskRan shouldBe false
    }

    "ignore new deadline if it's larger than existing scheduled deadline" in {
      @volatile var taskRan = false
      val initialTaskDelay = 2.seconds
      val existingTask = Delay.task(initialTaskDelay) {
        taskRan = true
      }

      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = true, task = Some(existingTask))
      implicit val self = TestActor[LevelCommand]()

//      level.paths _ expects() returning PathsDistributor(Seq(Dir(Paths.get("testPath"), 1)), () => Seq.empty)

      val deadline = 10.second.fromNow
      val newState = LevelActor.clearExpiredKeyValues(deadline)
      newState shouldBe a[Sleeping]
      newState.task shouldBe defined

      self.expectNoMessage(5.seconds)
    }

    "scheduled task with unexpectedFailureRetry during on failure" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = true, task = None)
      implicit val self = TestActor[LevelCommand]()

//      level.paths _ expects() returning PathsDistributor(Seq(Dir(Paths.get("testPath"), 1)), () => Seq.empty)
      level.clearExpiredKeyValues _ expects() returning Failure(LevelException.ContainsOverlappingBusySegments)

      val deadline = 0.second.fromNow
      val newState = LevelActor.clearExpiredKeyValues(deadline)
      newState shouldBe a[Sleeping]
      newState.task shouldBe defined

      //message scheduled
      val retryDeadline = self.expectMessage[LevelCommand.ClearExpiredKeyValues](LevelActor.unexpectedFailureRetry + 5.second).nextDeadline
      retryDeadline shouldBe <=(LevelActor.unexpectedFailureRetry.fromNow)
    }
  }

  "LevelActor.doPush" should {
    "not run push if it's already in Pushing state" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Pushing(List.empty, false, None, None)
      implicit val self = TestActor[LevelCommand]()

      LevelActor.doPush shouldBe state
      self.expectNoMessage(1.second)
    }

    "execute collapse small segments first if hasSmallSegments = true" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = PushScheduled(hasSmallSegments = true, task = None)
      implicit val self = TestActor[LevelCommand]()

      level.nextBatchSize _ expects() returning 10
      level.collapseAllSmallSegments _ expects 10 returning Success(0)
      level.hasNextLevel _ expects() returns false

      LevelActor.doPush shouldBe Sleeping(hasSmallSegments = false, task = None)
      self.expectNoMessage()
    }

    "go into sleeping mode if the level is empty" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = false, task = None)
      implicit val self = TestActor[LevelCommand]()

      level.hasNextLevel _ expects() returns true
      level.nextBatchSizeAndSegmentsCount _ expects() returning(10, 0)

      val updatedState = LevelActor.doPush.asInstanceOf[Sleeping]
      updatedState.hasSmallSegments shouldBe false
      updatedState.waitingPull shouldBe empty
      updatedState.busySegments shouldBe empty
    }

    "go into sleeping mode if the level is not empty but" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = false, task = None)
      implicit val self = TestActor[LevelCommand]()

      level.hasNextLevel _ expects() returns true
      level.nextBatchSizeAndSegmentsCount _ expects() returning(10, 0)

      val updatedState = LevelActor.doPush.asInstanceOf[Sleeping]
      updatedState.hasSmallSegments shouldBe false
      updatedState.waitingPull shouldBe empty
      updatedState.busySegments shouldBe empty
    }

    "send segments to lower level" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = false, task = None)
      implicit val self = TestActor[LevelCommand]()

      val testSegments = Seq(TestSegment().assertGet, TestSegment().assertGet)

      level.hasNextLevel _ expects() returns true
      level.nextBatchSizeAndSegmentsCount _ expects() returning(10, 10)
      level.pickSegmentsToPush _ expects 10 returning testSegments
      level.push _ expects * onCall {
        command: LevelAPI =>
          command match {
            case PushSegments(segments, replyTo) =>
              segments shouldHaveSameInOrderedIds testSegments
          }
      }

      val updatedState = LevelActor.doPush.asInstanceOf[Pushing]
      updatedState.hasSmallSegments shouldBe false
      updatedState.waitingPull shouldBe empty
      updatedState.busySegments shouldHaveSameInOrderedIds testSegments
    }

    "send pull request to lower level if non of the current Segments are mergeable with lower level" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = false, task = None)
      implicit val self = TestActor[LevelCommand]()

      level.hasNextLevel _ expects() returns true
      level.nextBatchSizeAndSegmentsCount _ expects() returning(10, 10)
      //batch size returned 10 segments but pickSegmentsToPush returned 0, which means there are busy overlapping
      //segments so this level submits a Pull requests to lower level.
      level.pickSegmentsToPush _ expects 10 returning Seq.empty
      level.push _ expects * onCall {
        command: LevelAPI =>
          command match {
            case PullRequest(pullFrom) =>
              pullFrom ! Pull
          }
      }
      //state of this level is not WaitingPull
      LevelActor.doPush shouldBe a[WaitingPull]
    }

    "return sleeping state if the Throttle returned 0 Segments to pick" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Sleeping(hasSmallSegments = false, task = None)
      implicit val self = TestActor[LevelCommand]()

      level.hasNextLevel _ expects() returns true
      //There are Segments in the level but the batch size 0. So the level goes in sleeping mode.
      level.nextBatchSizeAndSegmentsCount _ expects() returning(0, 10)
      LevelActor.doPush shouldBe a[Sleeping]
    }
  }

  "LevelActor.doRequest" should {
    "forward request to lower level" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Pushing(List.empty, false, None, None)
      implicit val self = TestActor[LevelCommand]()

      val testSegments = Seq(TestSegment().assertGet, TestSegment().assertGet)

      level.forward _ expects * onCall {
        command: LevelAPI =>
          command match {
            case PushSegments(segments, replyTo) =>
              segments shouldHaveSameInOrderedIds testSegments
              TryUtil.successUnit
          }
      }

      val sender = TestActor[LevelCommand]()

      LevelActor.doRequest(PushSegments(testSegments, sender)) shouldBe state //forward is successful, state remains unchanged
      self.expectNoMessage()
      sender.expectNoMessage()
    }

    "process request in self level if forward fails and respond success to sender" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Pushing(List.empty, false, None, None)
      implicit val self = TestActor[LevelCommand]()

      val testSegments = Seq(TestSegment().assertGet, TestSegment().assertGet)

      level.forward _ expects * returning Failure(new Exception("Failed"))
      (level.put(_: Iterable[Segment])) expects * returns TryUtil.successUnit

      val sender = TestActor[LevelCommand]()

      LevelActor.doRequest(PushSegments(testSegments, sender)) shouldBe state
      //on successful put, the actor is also woken up for Pushing.
      self.expectMessage[LevelCommand]() shouldBe WakeUp
      val response = sender.expectMessage[PushSegmentsResponse]()
      response.request.segments shouldHaveSameInOrderedIds testSegments
      response.result.assertGet
    }

    "process request in self level if forward fails and respond failure to sender" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Pushing(List.empty, false, None, None)
      implicit val self = TestActor[LevelCommand]()

      val testSegments = Seq(TestSegment().assertGet, TestSegment().assertGet)

      level.forward _ expects * returning Failure(new Exception("Failed"))
      (level.put(_: Iterable[Segment])) expects * returns Failure(new Exception("Failed"))

      val sender = TestActor[LevelCommand]()

      LevelActor.doRequest(PushSegments(testSegments, sender)) shouldBe state
      self.expectNoMessage(1.second)
      val response = sender.expectMessage[PushSegmentsResponse]()
      response.request.segments shouldHaveSameInOrderedIds testSegments
      response.result.isFailure shouldBe true
    }
  }

  "LevelActor.doPushResponse" should {
    "dispatch pull to upper level after successful Push to lower level" in {
      val sender = TestActor[LevelCommand]()

      implicit val level = mock[LevelActorAPI]
      implicit val state = Pushing(List.empty, false, None, Some(sender))
      implicit val self = TestActor[LevelCommand]()

      val testSegments = Seq(TestSegment().assertGet, TestSegment().assertGet)

      val request = PushSegments(testSegments, sender)

      level.removeSegments _ expects * onCall {
        segments: Iterable[Segment] =>
          segments shouldHaveSameInOrderedIds testSegments
          Success(testSegments.size)
      }

      level.nextPushDelay _ expects() returns 1.second

      LevelActor.doPushResponse(PushSegmentsResponse(request, TryUtil.successUnit)) shouldBe(Sleeping(false, None), Some(PushTask(1.second, Push)))
      self.expectNoMessage()
      sender.expectMessage[Pull]()
    }

    "submit a Pull request if Push response was ContainsOverlappingBusySegments" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Pushing(List.empty, true, None, None)
      implicit val self = TestActor[LevelCommand]()

      val testSegments = Seq(TestSegment().assertGet, TestSegment().assertGet)

      val sender = TestActor[LevelCommand]()
      val request = PushSegments(testSegments, sender)

      level.push _ expects PullRequest(self) returning()

      LevelActor.doPushResponse(PushSegmentsResponse(request, Failure(ContainsOverlappingBusySegments))) shouldBe(WaitingPull(true, None), None)
      self.expectNoMessage()
    }

    "for all Push failures other then ContainsBusySegments, next push should be scheduled within the next 1.second" in {
      implicit val level = mock[LevelActorAPI]
      implicit val state = Pushing(List.empty, true, None, None)
      implicit val self = TestActor[LevelCommand]()

      val testSegments = Seq(TestSegment().assertGet, TestSegment().assertGet)

      val sender = TestActor[LevelCommand]()
      val request = PushSegments(testSegments, sender)

      LevelActor.doPushResponse(PushSegmentsResponse(request, Failure(farOut))) shouldBe(Sleeping(true, None), Some(PushTask(LevelActor.unexpectedFailureRetry, Push)))
      self.expectNoMessage()
    }
  }
}