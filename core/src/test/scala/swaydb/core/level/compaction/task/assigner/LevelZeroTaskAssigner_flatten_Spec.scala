/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.level.compaction.task.assigner

import org.scalamock.scalatest.MockFactory
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.segment.Segment
import swaydb.core.{merge => _, _}
import swaydb.data.MaxKey
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.testkit.RunThis._

import scala.concurrent.duration.DurationInt

class LevelZeroTaskAssigner_flatten_Spec extends TestBase with MockFactory {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder = TimeOrder.long
  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
  implicit val ec = TestExecutionContext.executionContext
  implicit val compactionParallelism: CompactionParallelism = CompactionParallelism.availableProcessors()

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
          //LevelZero map key-values.
          val keyValues =
            (0 to randomIntMax(20)) map {
              _ =>
                randomizedKeyValues(randomIntMax(100) max 1, startId = Some(0))
            }

          //build maps
          val maps = keyValues.map(TestMap(_))
          //execute the flatten function on the map
          val flattenedMaps = LevelZeroTaskAssigner.flatten(maps.iterator).awaitInf

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
                val furthestDeadline = TestData.furthestDeadline(expected ++ actual)
                println(s"Test TimeLeft: ${testTimeout.timeLeft.asString}: ${exception.getMessage}. furthestDeadline timeLeft: ${furthestDeadline.map(_.timeLeft.asString)}")
                if (testTimeout.isOverdue())
                  fail(exception)
            }
      }
    }
  }
}
