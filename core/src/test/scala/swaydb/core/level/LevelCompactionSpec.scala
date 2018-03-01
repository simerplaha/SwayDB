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

import java.util.concurrent.atomic.AtomicInteger

import org.scalamock.scalatest.MockFactory
import swaydb.core.{TestBase, data}
import swaydb.core.actor.TestActor
import swaydb.core.data.{KeyValue, Transient}
import swaydb.core.level.LevelException.ContainsOverlappingBusySegments
import swaydb.core.level.actor.LevelAPI
import swaydb.core.level.actor.LevelCommand.{Pull, PullRequest, PushSegments, PushSegmentsResponse}
import swaydb.core.util.Delay
import swaydb.data.compaction.Throttle
import swaydb.data.slice.Slice

import scala.concurrent.duration._
import scala.util.{Failure, Success}
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder
import swaydb.serializers.Default._
import swaydb.serializers._


class LevelCompactionMemorySpec extends LevelCompactionSpec {
  override def inMemoryStorage = true
}

class LevelCompactionSpec extends TestBase with MockFactory {

  import TestLevel._

  implicit val ordering = KeyOrder.default

  "Level" should {
    "merge all it's Segments to lower level" in {
      val testSegments = (1 to 10) map { index => TestSegment(Slice(Transient.Put(index, index))).assertGet }

      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times
      //there are no busy segments in lower level
      nextLevel.getBusySegments _ expects() returns List.empty

      //expect lower level to receive a Push command with all the 10 Segments since the batch size is 10
      (nextLevel ! _) expects * onCall {
        command: LevelAPI =>
          command match {
            case request @ PushSegments(segments, replyTo) =>
              segments should have size 10
              testSegments shouldHaveSameKeyValuesAs segments
              //return successful response and expect upper level to have deleted the Segments
              replyTo ! PushSegmentsResponse(request, Success())
          }
      }

      val level =
        TestLevel(
          segmentSize = 1.byte, //1.byte so that small segments do not get collapsed since the test data is very small.
          nextLevel = nextLevel,
          throttle = _ => Throttle(1.second, 10)
        ).addSegments(testSegments)

      eventual(2.second) {
        level.isEmpty shouldBe true
        level.isSleeping shouldBe true
      }
    }

    "merge all 50 Segments to lower level in batches of 10" in {
      //total 50 segments, lower level should get 5 requests with 10 segments in each.
      val batchSize = 10
      val testSegments = (1 to 50) map { index => TestSegment(Slice(Transient.Put(index, index))).assertGet }

      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times

      nextLevel.getBusySegments _ expects() returns List.empty repeat 5 times

      val requestsCount = new AtomicInteger(0)

      (nextLevel ! _) expects * onCall {
        command: LevelAPI =>
          command match {
            case request @ PushSegments(segments, replyTo) =>
              val expectedBatch = testSegments.drop(requestsCount.get() * batchSize).take(10)
              segments shouldHaveSameKeyValuesAs expectedBatch
              requestsCount.incrementAndGet()
              replyTo ! PushSegmentsResponse(request, Success())
          }
      } repeat 5.times

      val level =
        TestLevel(
          segmentSize = 1.byte, //1.byte, so that small segments do not get collapsed since the test data is very small.
          nextLevel = nextLevel,
          throttle = _ => Throttle(100.millisecond, batchSize)
        ).addSegments(testSegments)

      eventual(2.seconds) {
        level.isEmpty shouldBe true
        level.isSleeping shouldBe true
      }

    }

    "collapse all existing small Segments to a single segment and then Push the single large segments to lower level" in {
      //100 small segments.
      val testSegments = (1 to 100) map { index => TestSegment(Slice(Transient.Put(index, index))).assertGet }

      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times
      nextLevel.getBusySegments _ expects() returns List.empty

      (nextLevel ! _) expects * onCall {
        command: LevelAPI =>
          command match {
            case request @ PushSegments(segments, replyTo) =>
              //All the segments are collapsed into 1 Segment before lower level gets a job.
              segments should have size 1
              testSegments.head.minKey shouldBe segments.head.minKey
              testSegments.last.maxKey shouldBe segments.head.maxKey
              //collapsed Segment should contain all KeyValues of test segments
              testSegments shouldHaveSameKeyValuesAs Seq(segments.head)
              replyTo ! PushSegmentsResponse(request, Success())
          }
      }

      val level =
        TestLevel(
          //10.mb, test Segments are small and will be collapsed. Lower level is expected to get only 1 Segment (Collapsed from 100 segments)
          segmentSize = 10.mb,
          nextLevel = nextLevel,
          throttle = _ => Throttle(1.second, 100)
        ).addSegments(testSegments) // add 100 small segments to upper level.

      eventual(15.second) {
        level.isEmpty shouldBe true
        level.isSleeping shouldBe true
      }
    }

    "not push Segments that have overlapping KeyValues with lower level's " +
      "current busy segments (Segments that lower level is pushing down)" +
      "but re-send those Segments on after the first push of non overlapping Segments are dispatch" in {
      val testSegments = (1 to 10) map { index => TestSegment(Slice(Transient.Put(index, index))).assertGet }

      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times

      //first push - lower level's busy Segments are upper level's first 5 busy Segments, so 2nd half of the testSegments
      // is expected to be pushed to lower level.
      nextLevel.getBusySegments _ expects() returns testSegments.take(5).toList
      (nextLevel ! _) expects * onCall {
        command: LevelAPI =>
          command match {
            case request @ PushSegments(segments, replyTo) =>
              //first 5 test segments have overlapping key values, they are expected to not get delivered to lower level.
              //as they get ignored by the upper level before a Push to lower level is executed
              //first request segments at indexes 5 to 10 are expected as the 0 to 4 are set to busy is lower level
              segments should have size 5
              testSegments.drop(5) shouldHaveSameKeyValuesAs segments
              replyTo ! PushSegmentsResponse(request, Success())
          }
      }


      //second busy, there are no busy segments, so upper level should send the first 5 segments again that were busy in the previous Push
      nextLevel.getBusySegments _ expects() returns List.empty //release busy segments, Lower level should get remaining other Segments
      (nextLevel ! _) expects * onCall {
        command: LevelAPI =>
          command match {
            case request @ PushSegments(segments, replyTo) =>
              //second request - after successful first request, second request results in no busy segments and now the previously ignored
              //segments are re-dispatched to lower level.
              segments should have size 5
              testSegments.take(5) shouldHaveSameKeyValuesAs segments
              replyTo ! PushSegmentsResponse(request, Success())
          }
      }

      val level =
        TestLevel(
          segmentSize = 1.bytes, //disable collapsing of smaller segments since test data set is small
          nextLevel = nextLevel,
          throttle = _ => Throttle(500.millisecond, 10)
        ).addSegments(testSegments)

      eventual(2.second) {
        level.isEmpty shouldBe true
        level.isSleeping shouldBe true
      }
    }

    "not push Segments that have overlapping KeyValues with lower level's " +
      "current busy segments and wait for lower level to trigger pull" in {
      val testSegments = (1 to 10) map { index => TestSegment(Slice(Transient.Put(index, index))).assertGet }

      val nextLevel = mock[LevelRef]
      nextLevel.isTrash _ expects() returning false repeat 2.times
      //on first getBusySegments check, lower level's busy Segments are upper level's first 5 busy Segments, so 2nd half
      // of the testSegments is expected to be pushed to lower level.
      nextLevel.getBusySegments _ expects() returns testSegments.take(5).toList
      (nextLevel ! _) expects * onCall {
        command: LevelAPI =>
          command match {
            case request @ PushSegments(segments, replyTo) =>
              //Lower level has the first five segments set as busy, expect the 2nd half to receive for merging
              //first request segments at indexes 5 to 10 are expected.
              segments should have size 5
              testSegments.drop(5) shouldHaveSameKeyValuesAs segments
              //for the first request respond back with Busy segments for all the segments
              replyTo ! PushSegmentsResponse(request, Failure(ContainsOverlappingBusySegments))
          }
      }

      //upper level sends a PullRequest to lower level
      (nextLevel ! _) expects * onCall {
        command: LevelAPI =>
          command match {
            case PullRequest(pullFrom) =>
              pullFrom ! Pull
          }
      }

      // on third getBusySegments check (After the Pull is sent to upper level), there are no busy Segments for lower level,
      // upper level will try again.
      nextLevel.getBusySegments _ expects() returns List.empty
      (nextLevel ! _) expects * onCall {
        command: LevelAPI =>
          command match {
            case request @ PushSegments(segments, replyTo) =>
              segments should have size 10
              //all key values are expected
              testSegments shouldHaveSameKeyValuesAs segments
              //respond back with success and expect upper Level to have delete all it's Segments.
              replyTo ! PushSegmentsResponse(request, Success())
          }
      }

      val level =
        TestLevel(
          segmentSize = 1.byte, //disable collapsing of smaller segments since test data set is small
          nextLevel = nextLevel,
          throttle = _ => Throttle(0.micro, 10)
        ).addSegments(testSegments)

      eventual(2.second) {
        level.isEmpty shouldBe true
        level.isSleeping shouldBe true
      }
    }
  }
}