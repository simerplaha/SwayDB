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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.assigner

import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.segment.Segment
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.util.PipeOps._
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave, TestTimer}
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem

import scala.collection.mutable.ListBuffer

class SegmentAssigner_AssignSegment_Spec0 extends SegmentAssigner_AssignSegment_Spec {
  val keyValueCount = 100
}

class SegmentAssigner_AssignSegment_Spec1 extends SegmentAssigner_AssignSegment_Spec {
  val keyValueCount = 100

  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class SegmentAssigner_AssignSegment_Spec2 extends SegmentAssigner_AssignSegment_Spec {
  val keyValueCount = 100

  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class SegmentAssigner_AssignSegment_Spec3 extends SegmentAssigner_AssignSegment_Spec {
  val keyValueCount = 1000
  override def inMemoryStorage = true
}

sealed trait SegmentAssigner_AssignSegment_Spec extends TestBase {
  implicit val keyOrder = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit def segmentIO: SegmentReadIO = SegmentReadIO.random

  def keyValueCount: Int

  "assign new Segment to Segment" when {
    "both have the same key values" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val segment = TestSegment()

            //assign the Segment's key-values to itself.
            /**
             * Test with no gaps
             */
            val noGaps = SegmentAssigner.assignUnsafeNoGaps(Slice(segment), Slice(segment))
            noGaps should have size 1
            var assignedSegments = noGaps.head.midOverlap.expectSegments()
            assignedSegments should have size 1
            assignedSegments.head shouldBe segment
            //just to reduce GC workload. Do not create gap instances when it's noGap is true.
            //even without null the output is still Nothing which is useless.
            noGaps.head.headGap shouldBe null
            noGaps.head.tailGap shouldBe null

            /**
             * Test with gaps
             */
            val gaps = SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable]](Slice(segment), Slice(segment))
            gaps should have size 1
            assignedSegments = noGaps.head.midOverlap.expectSegments()
            assignedSegments.head shouldBe segment

            gaps.head.headGap.result shouldBe empty
            gaps.head.tailGap.result shouldBe empty
        }
      }
    }

    "there two Segments and all have same key-values" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val keyValues = randomKeyValues(count = 1000, startId = Some(1)).groupedSlice(2)
            keyValues should have size 2

            val segment1 = TestSegment(keyValues.head)
            val segment2 = TestSegment(keyValues.last)

            //assign the Segment's key-values to itself.
            /**
             * Test with no gaps
             */
            SegmentAssigner.assignUnsafeNoGaps(Slice(segment1, segment2), Slice(segment1, segment2)) ==> {
              noGaps =>
                //got two assignments
                noGaps should have size 2

                //first segment is segment1
                val assignment1 = noGaps.head
                val assignedSegmentsHead = assignment1.midOverlap.expectSegments()
                assignedSegmentsHead should have size 1
                assignedSegmentsHead.head shouldBe segment1
                assignment1.segment shouldBe segment1
                assignment1.headGap shouldBe null
                assignment1.tailGap shouldBe null

                val assignment2 = noGaps.last
                val assignedSegmentsLast = assignment2.midOverlap.expectSegments()
                assignedSegmentsLast should have size 1
                assignedSegmentsLast.head shouldBe segment2
                assignment2.segment shouldBe segment2
                assignment2.headGap shouldBe null
                assignment2.tailGap shouldBe null
            }

            /**
             * Test with gaps
             */
            SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable]](Slice(segment1, segment2), Slice(segment1, segment2)) ==> {
              gaps =>
                gaps should have size 2

                //first segment is segment1
                val assignment1 = gaps.head
                val assignedSegmentsHead = assignment1.midOverlap.expectSegments()
                assignedSegmentsHead should have size 1
                assignedSegmentsHead.head shouldBe segment1
                assignment1.segment shouldBe segment1
                assignment1.headGap.result shouldBe empty
                assignment1.tailGap.result shouldBe empty

                val assignment2 = gaps.last
                val assignedSegmentsLast = assignment2.midOverlap.expectSegments()
                assignedSegmentsLast should have size 1
                assignedSegmentsLast.head shouldBe segment2
                assignment2.segment shouldBe segment2
                assignment2.headGap.result shouldBe empty
                assignment2.tailGap.result shouldBe empty
            }
        }
      }
    }

    "input Segment expands to all Segment" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val keyValues = randomKeyValues(count = 1000, startId = Some(1))
            val keyValuesGrouped = keyValues.groupedSlice(2)
            keyValuesGrouped should have size 2

            val inputSegment = TestSegment(keyValues)

            val segment1 = TestSegment(keyValuesGrouped.head)
            val segment2 = TestSegment(keyValuesGrouped.last)

            //assign the Segment's key-values to itself.
            /**
             * Test with no gaps
             */
            SegmentAssigner.assignUnsafeNoGaps(Slice(inputSegment), Slice(segment1, segment2)) ==> {
              noGaps =>
                //got two assignments
                noGaps should have size 2

                //first segment is segment1
                val assignment1 = noGaps.head
                assignment1.midOverlap.expectKeyValues() shouldBe keyValuesGrouped.head
                assignment1.segment shouldBe segment1
                assignment1.headGap shouldBe null
                assignment1.tailGap shouldBe null

                val assignment2 = noGaps.last
                assignment2.midOverlap.expectKeyValues() shouldBe keyValuesGrouped.last
                assignment2.segment shouldBe segment2
                assignment2.headGap shouldBe null
                assignment2.tailGap shouldBe null
            }

            /**
             * Test with gaps
             */
            SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable]](Slice(inputSegment), Slice(segment1, segment2)) ==> {
              gaps =>
                gaps should have size 2

                //first segment is segment1
                val assignment1 = gaps.head
                assignment1.midOverlap.expectKeyValues() shouldBe keyValuesGrouped.head
                assignment1.segment shouldBe segment1
                assignment1.headGap.result shouldBe empty
                assignment1.tailGap.result shouldBe empty

                val assignment2 = gaps.last
                assignment2.midOverlap.expectKeyValues() shouldBe keyValuesGrouped.last
                assignment2.segment shouldBe segment2
                assignment2.headGap.result shouldBe empty
                assignment2.tailGap.result shouldBe empty
            }
        }
      }
    }

    "Segment belongs to next Segment" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val keyValues = randomKeyValues(count = 1000, startId = Some(1)).groupedSlice(2)
            keyValues should have size 2

            val inputSegment = TestSegment(keyValues.last)

            val segment1 = TestSegment(keyValues.head)
            val segment2 = TestSegment(keyValues.last)

            //assign the Segment's key-values to itself.
            /**
             * Test with no gaps
             */
            SegmentAssigner.assignUnsafeNoGaps(Slice(inputSegment), Slice(segment1, segment2)) ==> {
              noGaps =>
                noGaps should have size 1

                val assignment = noGaps.head
                val assignedSegments = assignment.midOverlap.expectSegments()
                assignedSegments should have size 1
                assignedSegments.head shouldBe inputSegment
                assignment.segment shouldBe segment2
                assignment.headGap shouldBe null
                assignment.tailGap shouldBe null
            }

            /**
             * Test with gaps
             */
            SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable]](Slice(inputSegment), Slice(segment1, segment2)) ==> {
              gaps =>
                gaps should have size 1

                val assignment = gaps.head
                val assignedSegments = assignment.midOverlap.expectSegments()
                assignedSegments should have size 1
                assignedSegments.head shouldBe inputSegment
                assignment.segment shouldBe segment2
                assignment.headGap.result shouldBe empty
                assignment.tailGap.result shouldBe empty
            }
        }
      }
    }

    "multiple input Segments have gaps" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val keyValues = randomKeyValues(count = 1000, startId = Some(1)).groupedSlice(10)

            //create key-values with head and tail gap to be of 5
            val expectedAssignments =
              keyValues map {
                keyValues =>
                  val dropCount = 5
                  val headGap = keyValues.take(dropCount)
                  val tailGap = keyValues.takeRight(dropCount)
                  val mid = keyValues.drop(dropCount).dropRight(dropCount)
                  (headGap, mid, tailGap)
              }

            //create Segments with only mid key-values
            val segments =
              expectedAssignments map {
                case (_, midKeyValues, _) =>
                  TestSegment(midKeyValues)
              }

            val inputSegments =
              keyValues map (TestSegment(_))

            //Expect full Segments to get assigned without expanding or assigning gaps.

            /**
             * Test with no gaps
             */
            SegmentAssigner.assignUnsafeNoGaps(inputSegments, segments) ==> {
              assignments =>
                assignments should have size segments.size

                assignments.zipWithIndex foreach {
                  case (assignment, index) =>
                    assignment.segment shouldBe segments.get(index)
                    val assignedSegments = assignment.midOverlap.expectSegments()
                    assignedSegments should have size 1
                    assignedSegments.head shouldBe inputSegments.get(index)
                    assignment.headGap shouldBe null
                    assignment.tailGap shouldBe null
                }
            }

            /**
             * Test with gaps
             */
            SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable]](inputSegments, segments) ==> {
              assignments =>
                assignments should have size segments.size

                assignments.zipWithIndex foreach {
                  case (assignment, index) =>
                    assignment.segment shouldBe segments.get(index)
                    val assignedSegments = assignment.midOverlap.expectSegments()
                    assignedSegments should have size 1
                    assignedSegments.head shouldBe inputSegments.get(index)
                    assignment.headGap.result shouldBe empty
                    assignment.tailGap.result shouldBe empty
                }
            }
        }
      }
    }

    "multiple input Segments have gaps Segments" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val keyValues = randomKeyValues(count = 100, startId = Some(1)).groupedSlice(10)

            //create key-values with head and tail gap to be of 5
            val expectedAssignments =
              keyValues map {
                keyValues =>
                  val dropCount = 2
                  val headGap = keyValues.take(dropCount)
                  val tailGap = keyValues.takeRight(dropCount)
                  val mid = keyValues.drop(dropCount).dropRight(dropCount)
                  (headGap, mid, tailGap)
              }

            val gapedSegment: Slice[(Segment, Segment, Segment)] =
              expectedAssignments map {
                case (headGap, midKeyValues, tailGap) =>
                  (TestSegment(headGap), TestSegment(midKeyValues), TestSegment(tailGap))
              }

            //input all Segments and gap Segments flattened
            val inputSegments =
              gapedSegment flatMap {
                case (headGap, midKeyValues, tailGap) =>
                  Slice(headGap, midKeyValues, tailGap)
              }

            //existing Segments are just the midKeyValues
            val existingSegments =
              expectedAssignments map {
                case (_, midKeyValues, _) =>
                  TestSegment(midKeyValues)
              }

            /**
             * Test with no gaps
             */
            SegmentAssigner.assignUnsafeNoGaps(inputSegments, existingSegments) ==> {
              assignments =>
                assignments should have size existingSegments.size

                assignments.zipWithIndex foreach {
                  case (assignment, index) =>
                    assignment.segment shouldBe existingSegments.get(index)
                    val assignedSegments = assignment.midOverlap.expectSegments()

                    if (index == 0) //first will get 2 assigned since tail gap get assigned to next Segment
                      assignedSegments should have size 2
                    else if (index == existingSegments.size - 1)
                      assignedSegments should have size 4 //last will have 4 assigned
                    else
                      assignedSegments should have size 3 //mid will have 3 segments assigned

                    assignment.headGap shouldBe null
                    assignment.tailGap shouldBe null
                }
            }

            /**
             * Test with gaps
             */
            SegmentAssigner.assignUnsafeGaps[ListBuffer[Assignable]](inputSegments, existingSegments) ==> {
              assignments =>
                assignments should have size existingSegments.size

                assignments.zipWithIndex foreach {
                  case (assignment, index) =>
                    assignment.segment shouldBe existingSegments.get(index)
                    val assignedSegments = assignment.midOverlap.expectSegments()

                    assignedSegments should have size 1

                    (assignment.headGap.result ++ assignment.tailGap.result) should not be empty
                }
            }
        }
      }
    }
  }
}
