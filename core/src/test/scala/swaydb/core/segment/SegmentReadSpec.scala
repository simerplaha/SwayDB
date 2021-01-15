/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.segment

import java.nio.file.NoSuchFileException
import org.scalatest.OptionValues._
import org.scalatest.concurrent.ScalaFutures
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.io.file.Effect
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.Segment
import swaydb.core._
import swaydb.core.segment.io.SegmentReadIO
import swaydb.data.MaxKey
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._
import scala.util.Random

class SegmentReadSpec0 extends SegmentReadSpec {
  val keyValuesCount = 100
}

class SegmentReadSpec1 extends SegmentReadSpec {
  val keyValuesCount = 100
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class SegmentReadSpec2 extends SegmentReadSpec {
  val keyValuesCount = 100
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class SegmentReadSpec3 extends SegmentReadSpec {
  val keyValuesCount = 100
  override def inMemoryStorage = true
}

sealed trait SegmentReadSpec extends TestBase with ScalaFutures {

  implicit val keyOrder = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.random

  def keyValuesCount: Int

  implicit val segmentIO = SegmentReadIO.random

  "belongsTo" should {
    "return true if the input key-value belong to the Segment else false when the Segment contains no Range key-value" in {
      TestCaseSweeper {
        implicit sweeper =>
          val segment = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(5)))

          runThis(10.times) {
            Segment.keyOverlaps(randomFixedKeyValue(0), segment) shouldBe false

            //inner
            (1 to 5) foreach {
              i =>
                randomizedKeyValues(10, startId = Some(i)) foreach {
                  keyValue =>
                    if (keyValue.key.readInt() <= 5)
                      Segment.keyOverlaps(keyValue, segment) shouldBe true
                }
            }

            //outer
            (6 to 20) foreach {
              i =>
                randomizedKeyValues(10, startId = Some(i)) foreach {
                  keyValue =>
                    Segment.keyOverlaps(keyValue, segment) shouldBe false
                }
            }
          }
      }
    }

    "return true if the input key-value belong to the Segment else false when the Segment's max key is a Range key-value" in {
      TestCaseSweeper {
        implicit sweeper =>
          val segment = TestSegment(Slice(randomFixedKeyValue(1), randomRangeKeyValue(5, 10)))

          runThis(10.times) {
            Segment.keyOverlaps(randomFixedKeyValue(0), segment) shouldBe false

            //inner
            (1 to 9) foreach {
              i =>
                randomizedKeyValues(10, startId = Some(i)) foreach {
                  keyValue =>
                    if (keyValue.key.readInt() < 10)
                      Segment.keyOverlaps(keyValue, segment) shouldBe true
                }
            }

            //outer
            (10 to 20) foreach {
              i =>
                randomizedKeyValues(10, startId = Some(i)) foreach {
                  keyValue =>
                    Segment.keyOverlaps(keyValue, segment) shouldBe false
                }
            }
          }
      }
    }

    "return true if the input key-value belong to the Segment else false when the Segment's min key is a Range key-value" in {
      TestCaseSweeper {
        implicit sweeper =>
          val segment = TestSegment(Slice(randomRangeKeyValue(1, 10), randomFixedKeyValue(11)))

          runThis(10.times) {
            Segment.keyOverlaps(randomFixedKeyValue(0), segment) shouldBe false

            //inner
            (1 to 11) foreach {
              i =>
                randomizedKeyValues(10, startId = Some(i)) foreach {
                  keyValue =>
                    if (keyValue.key.readInt() < 10)
                      Segment.keyOverlaps(keyValue, segment) shouldBe true
                }
            }

            //outer
            (12 to 20) foreach {
              i =>
                randomizedKeyValues(10, startId = Some(i)) foreach {
                  keyValue =>
                    Segment.keyOverlaps(keyValue, segment) shouldBe false
                }
            }
          }
      }
    }

    "for randomizedKeyValues" in {
      TestCaseSweeper {
        implicit sweeper =>
          val keyValues = randomizedKeyValues(keyValuesCount, startId = Some(keyValuesCount + 1000))
          val segment = TestSegment(keyValues)

          val minKeyInt = keyValues.head.key.readInt()
          val maxKey = getMaxKey(keyValues.last)
          val maxKeyInt = maxKey.maxKey.readInt()

          val leftOutKeysRange = minKeyInt - keyValuesCount until minKeyInt
          val innerKeyRange = if (maxKey.inclusive) minKeyInt to maxKeyInt else minKeyInt until maxKeyInt
          val rightOutKeyRange = if (maxKey.inclusive) maxKeyInt + 1 to maxKeyInt + keyValuesCount else maxKeyInt to maxKeyInt + keyValuesCount

          runThis(10.times) {
            leftOutKeysRange foreach {
              key =>
                Segment.keyOverlaps(randomFixedKeyValue(key), segment) shouldBe false
            }

            //inner
            innerKeyRange foreach {
              i =>
                val keyValues = randomizedKeyValues(keyValuesCount, startId = Some(i))
                keyValues foreach {
                  keyValue =>
                    if (keyValue.key.readInt() <= innerKeyRange.last)
                      Segment.keyOverlaps(keyValue, segment) shouldBe true
                }
            }

            //outer
            rightOutKeyRange foreach {
              i =>
                val keyValues = randomizedKeyValues(keyValuesCount, startId = Some(i))
                keyValues foreach {
                  keyValue =>
                    Segment.keyOverlaps(keyValue, segment) shouldBe false
                }
            }
          }
      }
    }
  }

  "rangeBelongsTo" should {
    "return true for overlapping KeyValues else false for Segments if the Segment's last key-value is not a Range" in {
      TestCaseSweeper {
        implicit sweeper =>
          val segment = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(5)))

          //0 - 0
          //      1 - 5
          Segment.overlaps(0, 0, true, segment) shouldBe false
          Segment.overlaps(0, 0, false, segment) shouldBe false
          //  0 - 1
          //      1 - 5
          Segment.overlaps(0, 1, true, segment) shouldBe true
          Segment.overlaps(0, 1, false, segment) shouldBe false
          //    0 - 2
          //      1 - 5
          Segment.overlaps(0, 2, true, segment) shouldBe true
          Segment.overlaps(0, 2, false, segment) shouldBe true
          //    0   - 5
          //      1 - 5
          Segment.overlaps(0, 5, true, segment) shouldBe true
          Segment.overlaps(0, 5, false, segment) shouldBe true
          //    0   -   6
          //      1 - 5
          Segment.overlaps(0, 6, true, segment) shouldBe true
          Segment.overlaps(0, 6, false, segment) shouldBe true


          //      1-2
          //      1 - 5
          Segment.overlaps(1, 2, true, segment) shouldBe true
          Segment.overlaps(1, 2, false, segment) shouldBe true
          //      1-4
          //      1 - 5
          Segment.overlaps(1, 4, true, segment) shouldBe true
          Segment.overlaps(1, 4, false, segment) shouldBe true
          //      1 - 5
          //      1 - 5
          Segment.overlaps(1, 5, true, segment) shouldBe true
          Segment.overlaps(1, 5, false, segment) shouldBe true
          //      1 -  6
          //      1 - 5
          Segment.overlaps(1, 6, true, segment) shouldBe true
          Segment.overlaps(1, 6, false, segment) shouldBe true


          //       2-4
          //      1 - 5
          Segment.overlaps(2, 4, true, segment) shouldBe true
          Segment.overlaps(2, 4, false, segment) shouldBe true
          //       2- 5
          //      1 - 5
          Segment.overlaps(2, 5, true, segment) shouldBe true
          Segment.overlaps(2, 5, false, segment) shouldBe true
          //        2 - 6
          //      1 - 5
          Segment.overlaps(2, 6, true, segment) shouldBe true
          Segment.overlaps(2, 6, false, segment) shouldBe true
          //          5 - 6
          //      1 - 5
          Segment.overlaps(5, 6, true, segment) shouldBe true
          Segment.overlaps(5, 6, false, segment) shouldBe true
          //            6 - 7
          //      1 - 5
          Segment.overlaps(6, 7, true, segment) shouldBe false
          Segment.overlaps(6, 7, false, segment) shouldBe false

          //wide outer overlap
          //    0   -   6
          //      1 - 5
          Segment.overlaps(0, 6, true, segment) shouldBe true
          Segment.overlaps(0, 6, false, segment) shouldBe true
      }
    }

    "return true for overlapping KeyValues else false for Segments if the Segment's last key-value is a Range" in {
      TestCaseSweeper {
        implicit sweeper =>
          val segment = TestSegment(Slice(randomFixedKeyValue(1), randomRangeKeyValue(5, 10)))


          //0 - 0
          //      1 - (5 - 10(EX))
          Segment.overlaps(0, 0, true, segment) shouldBe false
          Segment.overlaps(0, 0, false, segment) shouldBe false
          //  0 - 1
          //      1 - (5 - 10(EX))
          Segment.overlaps(0, 1, true, segment) shouldBe true
          Segment.overlaps(0, 1, false, segment) shouldBe false
          //    0 - 2
          //      1 - (5 - 10(EX))
          Segment.overlaps(0, 2, true, segment) shouldBe true
          Segment.overlaps(0, 2, false, segment) shouldBe true
          //    0 -    5
          //      1 - (5 - 10(EX))
          Segment.overlaps(0, 5, true, segment) shouldBe true
          Segment.overlaps(0, 5, false, segment) shouldBe true
          //    0   -    7
          //      1 - (5 - 10(EX))
          Segment.overlaps(0, 7, true, segment) shouldBe true
          Segment.overlaps(0, 7, false, segment) shouldBe true
          //    0     -    10
          //      1 - (5 - 10(EX))
          Segment.overlaps(0, 10, true, segment) shouldBe true
          Segment.overlaps(0, 10, false, segment) shouldBe true
          //    0      -      11
          //      1 - (5 - 10(EX))
          Segment.overlaps(0, 11, true, segment) shouldBe true
          Segment.overlaps(0, 11, false, segment) shouldBe true

          //      1 - 5
          //      1 - (5 - 10(EX))
          Segment.overlaps(1, 5, true, segment) shouldBe true
          Segment.overlaps(1, 5, false, segment) shouldBe true
          //      1 -   6
          //      1 - (5 - 10(EX))
          Segment.overlaps(1, 6, true, segment) shouldBe true
          Segment.overlaps(1, 6, false, segment) shouldBe true
          //      1 -      10
          //      1 - (5 - 10(EX))
          Segment.overlaps(1, 10, true, segment) shouldBe true
          Segment.overlaps(1, 10, false, segment) shouldBe true
          //      1 -          11
          //      1 - (5 - 10(EX))
          Segment.overlaps(1, 11, true, segment) shouldBe true
          Segment.overlaps(1, 11, false, segment) shouldBe true

          //       2-4
          //      1 - (5 - 10(EX))
          Segment.overlaps(2, 4, true, segment) shouldBe true
          Segment.overlaps(2, 4, false, segment) shouldBe true
          //       2 - 5
          //      1 - (5 - 10(EX))
          Segment.overlaps(2, 5, true, segment) shouldBe true
          Segment.overlaps(2, 5, false, segment) shouldBe true
          //       2 -   6
          //      1 - (5 - 10(EX))
          Segment.overlaps(2, 6, true, segment) shouldBe true
          Segment.overlaps(2, 6, false, segment) shouldBe true
          //       2   -    10
          //      1 - (5 - 10(EX))
          Segment.overlaps(2, 10, true, segment) shouldBe true
          Segment.overlaps(2, 10, false, segment) shouldBe true
          //       2     -    11
          //      1 - (5 - 10(EX))
          Segment.overlaps(2, 11, true, segment) shouldBe true
          Segment.overlaps(2, 11, false, segment) shouldBe true


          //          5 - 6
          //      1 - (5 - 10(EX))
          Segment.overlaps(5, 6, true, segment) shouldBe true
          Segment.overlaps(5, 6, false, segment) shouldBe true
          //          5 -  10
          //      1 - (5 - 10(EX))
          Segment.overlaps(5, 10, true, segment) shouldBe true
          Segment.overlaps(5, 10, false, segment) shouldBe true
          //          5   -   11
          //      1 - (5 - 10(EX))
          Segment.overlaps(5, 11, true, segment) shouldBe true
          Segment.overlaps(5, 11, false, segment) shouldBe true
          //            6 - 7
          //      1 - (5 - 10(EX))
          Segment.overlaps(6, 7, true, segment) shouldBe true
          Segment.overlaps(6, 7, false, segment) shouldBe true
          //             8 - 9
          //      1 - (5   -   10(EX))
          Segment.overlaps(8, 9, true, segment) shouldBe true
          Segment.overlaps(8, 9, false, segment) shouldBe true
          //             8   - 10
          //      1 - (5   -   10(EX))
          Segment.overlaps(8, 10, true, segment) shouldBe true
          Segment.overlaps(8, 10, false, segment) shouldBe true
          //               9 - 10
          //      1 - (5   -   10(EX))
          Segment.overlaps(9, 10, true, segment) shouldBe true
          Segment.overlaps(9, 10, false, segment) shouldBe true
          //               9 -   11
          //      1 - (5   -   10(EX))
          Segment.overlaps(9, 11, true, segment) shouldBe true
          Segment.overlaps(9, 11, false, segment) shouldBe true
          //                   10  -   11
          //      1 - (5   -   10(EX))
          Segment.overlaps(10, 11, true, segment) shouldBe false
          Segment.overlaps(10, 11, false, segment) shouldBe false

          //                      11  -   11
          //      1 - (5   -   10(EX))
          Segment.overlaps(11, 11, true, segment) shouldBe false
          Segment.overlaps(11, 11, false, segment) shouldBe false

          //wide outer overlap
          //    0   -   6
          //      1 - (5 - 10(EX))
          Segment.overlaps(0, 6, true, segment) shouldBe true
      }
    }
  }

  "partitionOverlapping" should {
    "partition overlapping and non-overlapping Segments" in {
      TestCaseSweeper {
        implicit sweeper =>
          //0-1, 2-3
          //         4-5, 6-7
          var segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1))), TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3))))
          var segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))))
          Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq.empty, segments1)

          //0-1,   3-4
          //         4-5, 6-7
          segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1))), TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4))))
          segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))))
          Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq(segments1.last), Seq(segments1.head))

          //0-1,   3 - 5
          //         4-5, 6-7
          segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1))), TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(5))))
          segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))))
          Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq(segments1.last), Seq(segments1.head))


          //0-1,      6-8
          //      4-5,    10-20
          segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(8))))
          segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(20))))
          Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq.empty, segments1)

          //0-1,             20 - 21
          //      4-5,    10-20
          segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1))), TestSegment(Slice(randomFixedKeyValue(20), randomFixedKeyValue(21))))
          segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(20))))
          Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq(segments1.last), Seq(segments1.head))

          //0-1,               21 - 22
          //      4-5,    10-20
          segments1 = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1))), TestSegment(Slice(randomFixedKeyValue(21), randomFixedKeyValue(22))))
          segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(20))))
          Segment.partitionOverlapping(segments1, segments2) shouldBe(Seq.empty, segments1)

          //0          -          22
          //      4-5,    10-20
          segments1 = Seq(TestSegment(Slice(randomRangeKeyValue(0, 22))))
          segments2 = Seq(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(20))))
          Segment.partitionOverlapping(segments1, segments2) shouldBe(segments1, Seq.empty)
      }
    }
  }

  "overlaps" should {
    "return true for overlapping Segments else false for Segments without Ranges" in {
      TestCaseSweeper {
        implicit sweeper =>
          //0 1
          //    2 3
          var segment1 = TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)))
          var segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)))
          Segment.overlaps(segment1, segment2) shouldBe false
          Segment.overlaps(segment2, segment1) shouldBe false

          //1 2
          //  2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)))
          segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true

          //2 3
          //2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)))
          segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true

          //  3 4
          //2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4)))
          segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true

          //    4 5
          //2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)))
          segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)))
          Segment.overlaps(segment1, segment2) shouldBe false
          Segment.overlaps(segment2, segment1) shouldBe false

          //0       10
          //   2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(10)))
          segment2 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true

          //   2 3
          //0       10
          segment1 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)))
          segment2 = TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(10)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true
      }
    }

    "return true for overlapping Segments if the target Segment's maxKey is a Range key" in {
      TestCaseSweeper {
        implicit sweeper =>
          //0 1
          //    2 3
          var segment1 = TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)))
          var segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)))
          Segment.overlaps(segment1, segment2) shouldBe false
          Segment.overlaps(segment2, segment1) shouldBe false
          //range over range
          segment1 = TestSegment(Slice(randomRangeKeyValue(0, 1)))
          Segment.overlaps(segment1, segment2) shouldBe false
          Segment.overlaps(segment2, segment1) shouldBe false

          //1 2
          //  2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)))
          segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true
          segment1 = TestSegment(Slice(randomRangeKeyValue(1, 2)))
          Segment.overlaps(segment1, segment2) shouldBe false
          Segment.overlaps(segment2, segment1) shouldBe false

          //1   3
          //  2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(3)))
          segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true
          segment1 = TestSegment(Slice(randomRangeKeyValue(1, 3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true

          //2 3
          //2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)))
          segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true
          segment1 = TestSegment(Slice(randomRangeKeyValue(2, 3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true

          //  3 4
          //2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4)))
          segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)))
          Segment.overlaps(segment1, segment2) shouldBe false
          Segment.overlaps(segment2, segment1) shouldBe false
          segment1 = TestSegment(Slice(randomRangeKeyValue(3, 4)))
          Segment.overlaps(segment1, segment2) shouldBe false
          Segment.overlaps(segment2, segment1) shouldBe false

          //    4 5
          //2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5)))
          segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)))
          Segment.overlaps(segment1, segment2) shouldBe false
          Segment.overlaps(segment2, segment1) shouldBe false
          segment1 = TestSegment(Slice(randomRangeKeyValue(4, 5)))
          Segment.overlaps(segment1, segment2) shouldBe false
          Segment.overlaps(segment2, segment1) shouldBe false

          //0       10
          //   2 3
          segment1 = TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(10)))
          segment2 = TestSegment(Slice(randomRangeKeyValue(2, 3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true
          segment1 = TestSegment(Slice(randomRangeKeyValue(0, 10)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true

          //   2 3
          //0       10
          segment1 = TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3)))
          segment2 = TestSegment(Slice(randomRangeKeyValue(0, 10)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true
          segment1 = TestSegment(Slice(randomRangeKeyValue(2, 3)))
          Segment.overlaps(segment1, segment2) shouldBe true
          Segment.overlaps(segment2, segment1) shouldBe true
      }
    }
  }

  "nonOverlapping and overlapping" should {
    "return non overlapping Segments" in {
      TestCaseSweeper {
        implicit sweeper =>
          //0-1, 2-3
          //         4-5, 6-7
          var segments1 = List(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1))), TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3))))
          var segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))))
          Segment.nonOverlapping(segments1, segments2).map(_.path) shouldBe segments1.map(_.path)
          Segment.nonOverlapping(segments2, segments1).map(_.path) shouldBe segments2.map(_.path)
          Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
          Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty


          //2-3, 4-5
          //     4-5, 6-7
          segments1 = List(TestSegment(Slice(randomFixedKeyValue(2), randomFixedKeyValue(3))), TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))))
          segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))))
          Segment.nonOverlapping(segments1, segments2).map(_.path) should contain only segments1.head.path
          Segment.nonOverlapping(segments2, segments1).map(_.path) should contain only segments2.last.path
          Segment.overlaps(segments1, segments2).map(_.path) should contain only segments1.last.path
          Segment.overlaps(segments2, segments1).map(_.path) should contain only segments2.head.path

          //4-5, 6-7
          //4-5, 6-7
          segments1 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))))
          segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))))
          Segment.nonOverlapping(segments1, segments2).map(_.path) shouldBe empty
          Segment.nonOverlapping(segments2, segments1).map(_.path) shouldBe empty
          Segment.overlaps(segments1, segments2).map(_.path) shouldBe segments1.map(_.path)
          Segment.overlaps(segments2, segments1).map(_.path) shouldBe segments2.map(_.path)

          //     6-7, 8-9
          //4-5, 6-7
          segments1 = List(TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))), TestSegment(Slice(randomFixedKeyValue(8), randomFixedKeyValue(9))))
          segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))))
          Segment.nonOverlapping(segments1, segments2).map(_.path) should contain only segments1.last.path
          Segment.nonOverlapping(segments2, segments1).map(_.path) should contain only segments2.head.path
          Segment.overlaps(segments1, segments2).map(_.path) should contain only segments1.head.path
          Segment.overlaps(segments2, segments1).map(_.path) should contain only segments2.last.path

          //         8-9, 10-11
          //4-5, 6-7
          segments1 = List(TestSegment(Slice(randomFixedKeyValue(8), randomFixedKeyValue(9))), TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(11))))
          segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))))
          Segment.nonOverlapping(segments1, segments2).map(_.path) should contain allElementsOf segments1.map(_.path)
          Segment.nonOverlapping(segments2, segments1).map(_.path) should contain allElementsOf segments2.map(_.path)
          Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
          Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty

          //1-2            10-11
          //     4-5, 6-7
          segments1 = List(TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2))), TestSegment(Slice(randomFixedKeyValue(10), randomFixedKeyValue(11))))
          segments2 = List(TestSegment(Slice(randomFixedKeyValue(4), randomFixedKeyValue(5))), TestSegment(Slice(randomFixedKeyValue(6), randomFixedKeyValue(7))))
          Segment.nonOverlapping(segments1, segments2).map(_.path) should contain allElementsOf segments1.map(_.path)
          Segment.nonOverlapping(segments2, segments1).map(_.path) should contain allElementsOf segments2.map(_.path)
          Segment.overlaps(segments1, segments2).map(_.path) shouldBe empty
          Segment.overlaps(segments2, segments1).map(_.path) shouldBe empty
      }
    }
  }

  "tempMinMaxKeyValues" should {
    "return key-values with Segments min and max keys only" in {
      TestCaseSweeper {
        implicit sweeper =>
          implicit def testTimer: TestTimer = TestTimer.Empty

          val segment1 = TestSegment(randomizedKeyValues(keyValuesCount))
          val segment2 = TestSegment(randomizedKeyValues(keyValuesCount, startId = Some(segment1.maxKey.maxKey.read[Int] + 1)))
          val segment3 = TestSegment(randomizedKeyValues(keyValuesCount, startId = Some(segment2.maxKey.maxKey.read[Int] + 1)))
          val segment4 = TestSegment(randomizedKeyValues(keyValuesCount, startId = Some(segment3.maxKey.maxKey.read[Int] + 1)))

          val segments = Seq(segment1, segment2, segment3, segment4)

          val expectedTempKeyValues: Seq[Memory] =
            segments flatMap {
              segment =>
                segment.maxKey match {
                  case MaxKey.Fixed(maxKey) =>
                    Seq(Memory.put(segment.minKey), Memory.put(maxKey))
                  case MaxKey.Range(fromKey, maxKey) =>
                    Seq(Memory.put(segment.minKey), Memory.Range(fromKey, maxKey, Value.FromValue.Null, Value.update(maxKey)))
                }
            }

          Segment.tempMinMaxKeyValues(segments) shouldBe expectedTempKeyValues
      }
    }
  }

  "overlapsWithBusySegments" should {
    "return true or false if input Segments overlap or do not overlap with busy Segments respectively" in {
      TestCaseSweeper {
        implicit sweeper =>

          val targetSegments =
            TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2))) ::
              TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4))) ::
              TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8))) ::
              TestSegment(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10))) ::
              Nil

          //0-1
          //          3-4       7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          var inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1))))
          var busySegments = Seq(TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4))), TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8))))
          Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe false

          //     1-2
          //          3-4       7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2))))
          Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe false

          //          3-4
          //          3-4       7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(2))))
          Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe true

          //               5-6
          //          3-4       7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(5), randomFixedKeyValue(6))))
          Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe true

          //                         9-10
          //          3-4       7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10))))
          Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe false

          //               5-6
          //     1-2            7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          inputSegments = Seq(TestSegment(Slice(randomFixedKeyValue(5), randomFixedKeyValue(6))))
          busySegments = {
            TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2))) ::
              TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8))) ::
              Nil
          }
          Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe true

          //               5-6
          //     1-2                 9-10
          //     1-2, 3-4, ---, 7-8, 9-10
          busySegments = Seq(TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2))), TestSegment(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10))))
          Segment.overlapsWithBusySegments(inputSegments, busySegments, targetSegments) shouldBe false
      }
    }

    "return true or false if input map overlap or do not overlap with busy Segments respectively" in {
      TestCaseSweeper {
        implicit sweeper =>

          val targetSegments = {
            TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2))) ::
              TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4))) ::
              TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8))) ::
              TestSegment(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10))) ::
              Nil
          }

          //0-1
          //          3-4       7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          var inputMap = TestMap(Slice(randomFixedKeyValue(0), randomFixedKeyValue(1)))
          var busySegments = Seq(TestSegment(Slice(randomFixedKeyValue(3), randomFixedKeyValue(4))), TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8))))
          Segment.overlapsWithBusySegments(inputMap.cache.skipList, busySegments, targetSegments) shouldBe false

          //     1-2
          //          3-4       7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          inputMap = TestMap(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2)))
          Segment.overlapsWithBusySegments(inputMap.cache.skipList, busySegments, targetSegments) shouldBe false

          //          3-4
          //          3-4       7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          inputMap = TestMap(Slice(randomFixedKeyValue(3), randomFixedKeyValue(2)))
          Segment.overlapsWithBusySegments(inputMap.cache.skipList, busySegments, targetSegments) shouldBe true

          //               5-6
          //          3-4       7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          inputMap = TestMap(Slice(randomFixedKeyValue(5), randomFixedKeyValue(6)))
          Segment.overlapsWithBusySegments(inputMap.cache.skipList, busySegments, targetSegments) shouldBe true

          //                         9-10
          //          3-4       7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          inputMap = TestMap(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10)))
          Segment.overlapsWithBusySegments(inputMap.cache.skipList, busySegments, targetSegments) shouldBe false

          //               5-6
          //     1-2            7-8
          //     1-2, 3-4, ---, 7-8, 9-10
          inputMap = TestMap(Slice(randomFixedKeyValue(5), randomFixedKeyValue(6)))
          busySegments = {
            TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2))) ::
              TestSegment(Slice(randomFixedKeyValue(7), randomFixedKeyValue(8))) ::
              Nil
          }
          Segment.overlapsWithBusySegments(inputMap.cache.skipList, busySegments, targetSegments) shouldBe true

          //               5-6
          //     1-2                 9-10
          //     1-2, 3-4, ---, 7-8, 9-10
          busySegments = Seq(TestSegment(Slice(randomFixedKeyValue(1), randomFixedKeyValue(2))), TestSegment(Slice(randomFixedKeyValue(9), randomFixedKeyValue(10))))
          Segment.overlapsWithBusySegments(inputMap.cache.skipList, busySegments, targetSegments) shouldBe false
      }
    }
  }

  "getAllKeyValues" should {
    "value KeyValues from multiple Segments" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            val keyValues1 = randomizedKeyValues(keyValuesCount)
            val keyValues2 = randomizedKeyValues(keyValuesCount)
            val keyValues3 = randomizedKeyValues(keyValuesCount)

            val segment1 = TestSegment(keyValues1)
            val segment2 = TestSegment(keyValues2)
            val segment3 = TestSegment(keyValues3)

            val all = Slice((keyValues1 ++ keyValues2 ++ keyValues3).toArray)

            val mergedSegment = TestSegment(all)
            mergedSegment.nearestPutDeadline shouldBe nearestPutDeadline(all)

            val readKeyValues = Seq(segment1, segment2, segment3).flatMap(_.iterator())

            readKeyValues shouldBe all
        }
      }
    }

    "fail read if reading any one Segment fails for persistent Segments" in {
      TestCaseSweeper {
        implicit sweeper =>
          val keyValues1 = randomizedKeyValues(keyValuesCount)
          val keyValues2 = randomizedKeyValues(keyValuesCount)
          val keyValues3 = randomizedKeyValues(keyValuesCount)

          val segment1 = TestSegment(keyValues1)
          val segment2 = TestSegment(keyValues2)
          val segment3 = TestSegment(keyValues3)

          segment3.delete //delete a segment so that there is a failure.

          if (isWindowsAndMMAPSegments()) {
            sweeper.receiveAll() //execute all messages so that delete occurs.
            //ensure that file does not exist because reading otherwise on windows it will reopen the the ByteBuffer
            //which leads to AccessDenied.
            eventual(1.minute)(Effect.notExists(segment3.path) shouldBe true)
          }

          IO(Seq(segment1, segment2, segment3).flatMap(_.iterator())).left.value.exception shouldBe a[NoSuchFileException]
      }
    }

    "fail read if reading any one Segment file is corrupted" in {
      //Ignoring for windows MMAP because even after closing the files Windows not allow altering the unmapped file's Bytes.
      //Exception - The requested operation cannot be performed on a file with a user-mapped section open.
      if (persistent)
        if (isWindowsAndMMAPSegments())
          cancel("Test does not apply to Windows with MMAP files.")
        else
          TestCaseSweeper {
            implicit sweeper =>
              runThis(100.times, log = true) {
                val keyValues1 = randomizedKeyValues(keyValuesCount)
                val keyValues2 = randomizedKeyValues(keyValuesCount)
                val keyValues3 = randomizedKeyValues(keyValuesCount)

                val segment1 = TestSegment(keyValues1)
                val segment2 = TestSegment(keyValues2)
                val segment3 = TestSegment(keyValues3)

                def clearAll() = {
                  segment1.clearAllCaches()
                  segment2.clearAllCaches()
                  segment3.clearAllCaches()
                }

                val bytes = Effect.readAllBytes(segment2.path)

                //FIXME this should result in DataAccess

                Effect.overwrite(segment2.path, bytes.drop(1))
                clearAll()
                IO(Seq(segment1, segment2, segment3).flatMap(_.iterator())).left.runRandomIO.get shouldBe a[swaydb.Error]

                Effect.overwrite(segment2.path, bytes.dropRight(1))
                clearAll()
                IO(segment2.iterator()).left.runRandomIO.get shouldBe a[swaydb.Error]

                Effect.overwrite(segment2.path, bytes.drop(10))
                clearAll()
                IO(Seq(segment1, segment2, segment3).flatMap(_.iterator())).left.runRandomIO.get shouldBe a[swaydb.Error]

                Effect.overwrite(segment2.path, bytes.dropRight(1))
                clearAll()
                IO(Seq(segment1, segment2, segment3).flatMap(_.iterator())).left.runRandomIO.get shouldBe a[swaydb.Error]
              }
          }
      //memory files do not require this test
    }
  }

  "getAll" should {
    "read full index" in {
      if (persistent)
        TestCaseSweeper {
          implicit sweeper =>
            runThis(10.times) {
              //ensure groups are not added because ones read their values are populated in memory
              val keyValues = randomizedKeyValues(keyValuesCount)
              val segment = TestSegment(keyValues)

              if (persistent) segment.isKeyValueCacheEmpty shouldBe true

              val segmentKeyValues = segment.iterator().toList.toSlice

              (0 until keyValues.size).foreach {
                index =>
                  val actualKeyValue = keyValues(index)
                  val segmentKeyValue = segmentKeyValues(index)

                  if (persistent) {
                    //ensure that indexEntry's values are not already read as they are lazily fetched from the file.
                    //values with Length 0 and non Range key-values always have isValueDefined set to true as they do not required disk seek.
                    segmentKeyValue match {
                      case persistent: Persistent.Remove =>
                        persistent.isValueCached shouldBe true

                      case persistent: Persistent =>
                        persistent.isValueCached shouldBe false
                    }

                    actualKeyValue shouldBe segmentKeyValue //after comparison values should be populated.

                    segmentKeyValue match {
                      case persistent: Persistent.Remove =>
                        persistent.isValueCached shouldBe true

                      case persistent: Persistent =>
                        persistent.isValueCached shouldBe true
                    }
                  }
              }
            }
        }
    }
  }

  "getNearestDeadline" should {
    "return earliest deadline from key-values" in {
      runThis(10.times) {
        val deadlines = (1 to 8).map(_.seconds.fromNow)

        val shuffledDeadlines = Random.shuffle(deadlines)

        //populated deadlines from shuffled deadlines.
        val keyValues =
          Slice(
            Memory.remove(1, shuffledDeadlines(0)),
            Memory.put(2, 1, shuffledDeadlines(1)),
            Memory.update(3, 10, shuffledDeadlines(2)),
            Memory.Range(4, 10, Value.FromValue.Null, Value.remove(shuffledDeadlines(3))),
            Memory.Range(5, 10, Value.put(10, shuffledDeadlines(4)), Value.remove(shuffledDeadlines(5))),
            Memory.Range(6, 10, Value.put(10, shuffledDeadlines(6)), Value.update(Slice.Null, Some(shuffledDeadlines(7))))
          )

        Segment.getNearestDeadline(keyValues).value shouldBe nearestPutDeadline(keyValues).value
      }
    }
  }

  "getNearestDeadlineSegment" should {
    "return None deadline if non of the key-values in the Segments contains deadline" in {
      TestCaseSweeper {
        implicit sweeper =>
          def segmentConfig(keyValuesCount: Int) =
            if (persistent)
              SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = randomIntMax(keyValuesCount * 2), mmap = mmapSegments)
            else
              SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = Int.MaxValue, mmap = mmapSegments)

          runThis(100.times) {
            val keyValues1 = randomizedKeyValues(keyValuesCount, addPutDeadlines = false, addRemoveDeadlines = false, addUpdateDeadlines = false)
            val segment1 = TestSegment(keyValues1, segmentConfig = segmentConfig(keyValues1.size))

            val keyValues2 = randomizedKeyValues(keyValuesCount, addPutDeadlines = false, addRemoveDeadlines = false, addUpdateDeadlines = false)
            val segment2 = TestSegment(keyValues2, segmentConfig = segmentConfig(keyValues2.size))

            Segment.getNearestDeadlineSegment(segment1, segment2).toOptionS shouldBe empty

            segment1.close
            segment2.close
          }
      }
    }

    "return deadline if one of the Segments contains deadline" in {
      implicit val ec = TestExecutionContext.executionContext

      runThisParallel(10.times) {
        TestCaseSweeper {
          implicit sweeper =>
            val keyValues = randomizedKeyValues(keyValuesCount, addPutDeadlines = false, addRemoveDeadlines = false, addUpdateDeadlines = false)

            //only a single key-value with a deadline.
            val deadline = 100.seconds.fromNow
            val keyValueWithDeadline =
              eitherOne(
                left = Memory.remove(keyValues.last.key.readInt() + 10000, deadline),
                mid =
                  eitherOne(
                    Memory.put(keyValues.last.key.readInt() + 10000, randomStringOption, deadline),
                    randomRangeKeyValueForDeadline(keyValues.last.key.readInt() + 10000, keyValues.last.key.readInt() + 20000, deadline = deadline)
                  ),
                right =
                  eitherOne(
                    Memory.update(keyValues.last.key.readInt() + 10000, randomStringOption, deadline),
                    Memory.PendingApply(keyValues.last.key.readInt() + 10000, randomAppliesWithDeadline(deadline = deadline))
                  )
              )

            val keyValuesWithDeadline = keyValues ++ Array(keyValueWithDeadline: Memory)
            val keyValuesNoDeadline = randomizedKeyValues(keyValuesCount, addPutDeadlines = false, addRemoveDeadlines = false, addUpdateDeadlines = false)

            val deadlineToExpect = nearestPutDeadline(keyValuesWithDeadline)

            val segment1 = TestSegment(keyValuesWithDeadline)
            val segment2 = TestSegment(keyValuesNoDeadline)

            Segment.getNearestDeadlineSegment(segment1, segment2).flatMapOptionS(_.nearestPutDeadline) shouldBe deadlineToExpect
            Segment.getNearestDeadlineSegment(segment2, segment1).flatMapOptionS(_.nearestPutDeadline) shouldBe deadlineToExpect
            Segment.getNearestDeadlineSegment(Random.shuffle(Seq(segment2, segment1, segment2, segment2))).flatMapOptionS(_.nearestPutDeadline) shouldBe deadlineToExpect
            Segment.getNearestDeadlineSegment(Random.shuffle(Seq(segment1, segment2, segment2, segment2))).flatMapOptionS(_.nearestPutDeadline) shouldBe deadlineToExpect
        }
      }
    }

    "return deadline" in {
      implicit val ec = TestExecutionContext.executionContext

      runThisParallel(10.times) {
        TestCaseSweeper {
          implicit sweeper =>
            val keyValues1 = randomizedKeyValues(1000)
            val keyValues2 = randomizedKeyValues(1000)

            val segmentConfig =
              if (persistent)
                SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = randomIntMax(keyValues1.size * 2), mmap = mmapSegments)
              else
                SegmentBlock.Config.random(minSegmentSize = Int.MaxValue, maxKeyValuesPerSegment = Int.MaxValue, mmap = mmapSegments)

            val segment1 = TestSegment(keyValues1, segmentConfig = segmentConfig)
            val segment2 = TestSegment(keyValues2, segmentConfig = segmentConfig)

            val deadline = nearestPutDeadline(keyValues1 ++ keyValues2)

            Segment.getNearestDeadlineSegment(segment1, segment2).flatMapOptionS(_.nearestPutDeadline) shouldBe deadline
            Segment.getNearestDeadlineSegment(segment2, segment1).flatMapOptionS(_.nearestPutDeadline) shouldBe deadline
            Segment.getNearestDeadlineSegment(segment1 :: segment2 :: Nil).flatMapOptionS(_.nearestPutDeadline) shouldBe deadline
        }
      }
    }
  }
}
