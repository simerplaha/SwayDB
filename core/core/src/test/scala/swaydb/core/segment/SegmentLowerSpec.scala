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

package swaydb.core.segment

import org.scalatest.OptionValues._
import org.scalatest.PrivateMethodTester
import org.scalatest.concurrent.ScalaFutures
import swaydb.config.MMAP
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.{ACoreSpec, TestSweeper, TestForceSave}
import swaydb.core.level.ALevelSpec
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem

class SegmentLowerSpec0 extends SegmentLowerSpec {
  val keyValuesCount = 100
}

class SegmentLowerSpec1 extends SegmentLowerSpec {
  val keyValuesCount = 100
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
}

class SegmentLowerSpec2 extends SegmentLowerSpec {
  val keyValuesCount = 100
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
}

class SegmentLowerSpec3 extends SegmentLowerSpec {
  val keyValuesCount = 1000

  override def isMemorySpec = true
}

sealed trait SegmentLowerSpec extends ALevelSpec with ScalaFutures with PrivateMethodTester {

  implicit val keyOrder = KeyOrder.default

  def keyValuesCount: Int

  "Segment.lower" should {
    "value the lower key from the segment that has only 1 fixed key-value" in {
      TestSweeper {
        implicit sweeper =>
          assertSegment(
            keyValues =
              Slice(randomFixedKeyValue(1)),

            assert =
              (keyValues, segment) => {
                val readState = ThreadReadState.random
                segment.lower(0, readState).toOption shouldBe empty
                segment.lower(1, readState).toOption shouldBe empty
                segment.lower(2, readState).toOption.value shouldBe keyValues.head
              }
          )
      }
    }

    "value the lower from the segment when there are no Range key-values" in {
      TestSweeper {
        implicit sweeper =>
          //1, 2, 3
          assertSegment(
            keyValues =
              Slice(randomFixedKeyValue(1), randomFixedKeyValue(2), randomFixedKeyValue(3)),

            assert =
              (keyValues, segment) => {
                val readState = ThreadReadState.random

                segment.lower(0, readState).toOption shouldBe empty //smallest key in this segment is 1
                segment.lower(1, readState).toOption shouldBe empty

                segment.lower(2, readState).toOption.value shouldBe keyValues.head
                segment.lower(3, readState).toOption.value shouldBe keyValues(1)
                (4 to 10) foreach {
                  i =>
                    segment.lower(i, readState).toOption.value shouldBe keyValues(2)
                }
              }
          )
      }
    }

    "value the lower from the segment when there are Range key-values" in {
      //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
      runThis(100.times, log = true) {
        TestSweeper {
          implicit sweeper =>

            assertSegment(
              keyValues = Slice(
                randomFixedKeyValue(1),
                randomRangeKeyValue(2, 5),
                randomFixedKeyValue(10),
                randomRangeKeyValue(11, 20),
                randomRangeKeyValue(20, 30)
              ),

              assert =
                (keyValues, segment) => {
                  val readState = ThreadReadState.random

                  //0
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(0, readState).toOption shouldBe empty
                  //1
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(1, readState).toOption shouldBe empty
                  //    2
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(2, readState).getUnsafe shouldBe keyValues(0)
                  //     3
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(3, readState).getUnsafe shouldBe keyValues(1)
                  //       4
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(4, readState).getUnsafe shouldBe keyValues(1)
                  //        5
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(5, readState).getUnsafe shouldBe keyValues(1)
                  //          6
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(6, readState).getUnsafe shouldBe keyValues(1)
                  //            10
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(10, readState).getUnsafe shouldBe keyValues(1)
                  //                 11
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(11, readState).getUnsafe shouldBe keyValues(2)
                  //                   12
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(12, readState).getUnsafe shouldBe keyValues(3)
                  //                    19
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(19, readState).getUnsafe shouldBe keyValues(3)
                  //                      20
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(20, readState).getUnsafe shouldBe keyValues(3)
                  //                              21
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(21, readState).getUnsafe shouldBe keyValues(4)
                  //                                29
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(29, readState).getUnsafe shouldBe keyValues(4)
                  //                                 30
                  //  1, (2 - 5), 10, (11 - 20), (20 - 30) (30), (40 - 50)
                  segment.lower(30, readState).getUnsafe shouldBe keyValues(4)
                }
            )
        }
      }
    }

    "random" in {
      TestSweeper {
        implicit sweeper =>
          assertSegment(
            keyValues = randomizedKeyValues(keyValuesCount, addUpdates = true),
            assert = assertLower(_, _)
          )
      }
    }
  }
}
