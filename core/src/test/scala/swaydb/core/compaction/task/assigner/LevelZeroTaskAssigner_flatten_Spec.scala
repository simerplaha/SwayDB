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

package swaydb.core.compaction.task.assigner

import org.scalamock.scalatest.MockFactory
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core._
import swaydb.core.level.ALevelSpec
import swaydb.core.log.ALogSpec
import swaydb.core.segment.Segment
import swaydb.core.segment.data.{merge => _}
import swaydb.serializers.Default._
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.slice.{MaxKey, Slice}
import swaydb.testkit.RunThis._

import scala.concurrent.duration.DurationInt
import swaydb.testkit.TestKit._

class LevelZeroTaskAssigner_flatten_Spec extends ALevelSpec with ALogSpec with MockFactory {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
  implicit val ec = TestExecutionContext.executionContext

  /**
   * TEST STEPS
   * 1. random generates multiple LevelZero maps
   * 2. performs concurrent flattening on them
   * 3. asserts it's final merge state by merging the maps sequentially (no concurrency) into a Level.
   */
  "random generation" in {
    runThis(5.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          //LevelZero map key-values.
          val keyValues =
            (0 to randomIntMax(20)) map {
              _ =>
                randomizedKeyValues(randomIntMax(100) max 1, startId = Some(0))
            }

          //build maps
          val logs = keyValues.map(TestLog(_))
          //execute the flatten function on the map
          val flattenedMaps = LevelZeroTaskAssigner.flatten(logs.iterator).awaitInf

          //Assert: all maps are in order and there are no overlaps
          flattenedMaps.drop(1).foldLeft(flattenedMaps.head) {
            case (previousCollection, nextCollection) =>
              previousCollection.maxKey match {
                case MaxKey.Fixed(maxKey) =>
                  nextCollection.key.readInt() should be > maxKey.readInt()
                  nextCollection

                case MaxKey.Range(_, maxKey) =>
                  nextCollection.key.readInt() should be >= maxKey.readInt()
                  nextCollection
              }
          }

          //given the key-values assert it's state if it was merged sequentially into a level
          def buildExpectedKeyValues() = {
            val level = TestLevel()

            keyValues.reverse foreach {
              keyValues =>
                level.put(keyValues = keyValues, removeDeletes = true).get
            }

            level.segments().flatMap(_.iterator(randomBoolean())).map(_.toMemory()).toSlice
          }

          //get the actual merged key-values and write it to a Level to final cleanup merges
          def buildActualKeyValues() = {
            val level = TestLevel()
            val flattenedKeyValues = flattenedMaps.flatMap(_.iterator(randomBoolean())).map(_.toMemory())
            level.put(keyValues = flattenedKeyValues, removeDeletes = true)
            level.segments().flatMap(_.iterator(randomBoolean())).map(_.toMemory()).toSlice
          }

          //get all expected key-values
          val expected = buildExpectedKeyValues()
          //get all actual key-values
          val actual = buildActualKeyValues()
          //this test's timeout
          var testTimeout = 2.minutes.fromNow
          //run this test with multiple attempts until timeout
          while (testTimeout.hasTimeLeft())
            try {
              //Retry merge if there is a failure in assertion. This would happen for cases
              //where flattening merge (concurrent) resulted in a deadline to expire a key-value immediately
              //and sequential merge resulted in allowing the key-value's deadline to be extended or vice-versa.
              //This happens when the order of merge is not the same and only effects key-values with deadline.
              //Documentation - http://swaydb.io/implementation/compaction/increasing-expiration-time/?language=java
              val expectedMerged = merge(expected, Slice.empty, true)
              val actualMerged = merge(actual, Slice.empty, true)
              actualMerged shouldBe expectedMerged
              testTimeout = 0.seconds.fromNow
            } catch {
              case exception: Throwable =>
                //sleep for some-time
                sleep(2.second)
                //For better print find the furthest deadline to show how long will this test run for
                val furthestDeadline = CoreTestData.furthestDeadline(expected ++ actual)
                println(s"Test TimeLeft: ${testTimeout.timeLeft.asString}: ${exception.getMessage}. furthestDeadline timeLeft: ${furthestDeadline.map(_.timeLeft.asString)}")
                if (testTimeout.isOverdue())
                  fail(exception)
            }
      }
    }
  }
}
