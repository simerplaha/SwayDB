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

package swaydb.core.level

import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.level.zero.LevelZeroMapCache
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.{PersistentSegment, Segment, ThreadReadState}
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestForceSave, TestTimer}
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._

import scala.collection.mutable.ListBuffer
import scala.concurrent.duration._

class LevelCollapseSpec0 extends LevelCollapseSpec

class LevelCollapseSpec1 extends LevelCollapseSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelCollapseSpec2 extends LevelCollapseSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelCollapseSpec3 extends LevelCollapseSpec {
  override def inMemoryStorage = true
}

sealed trait LevelCollapseSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val ec = TestExecutionContext.executionContext
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean =
  //    false

  "collapse" should {
    "collapse small Segments to 50% of the size when the Segment's size was reduced by deleting 50% of it's key-values" in {
      TestCaseSweeper {
        implicit sweeper =>
          //disable throttling so that it does not automatically collapse small Segments
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))
          val keyValues = randomPutKeyValues(1000, addPutDeadlines = false, startId = Some(0))(TestTimer.Empty)
          level.putKeyValuesTest(keyValues).runRandomIO.right.value

          val segmentCountBeforeDelete = level.segmentsCount()
          segmentCountBeforeDelete > 1 shouldBe true

          assertAllSegmentsCreatedInLevel(level)

          val keyValuesNoDeleted = ListBuffer.empty[KeyValue]
          val deleteEverySecond =
            keyValues.zipWithIndex flatMap {
              case (keyValue, index) =>
                if (index % 2 == 0)
                  Some(Memory.Remove(keyValue.key, None, Time.empty))
                else {
                  keyValuesNoDeleted += keyValue
                  None
                }
            }
          //delete half of the key values which will create small Segments
          level.putKeyValuesTest(Slice(deleteEverySecond.toArray)).runRandomIO.right.value
          level.collapse(level.segmentsInLevel(), randomMaxParallelism()).right.right.value.right.value
          //since every second key-value was delete, the number of Segments is reduced to half
          level.segmentFilesInAppendix shouldBe <=((segmentCountBeforeDelete / 2) + 1) //+1 for odd number of key-values
          assertReads(Slice(keyValuesNoDeleted.toArray), level)

      }
    }

    "collapse all small Segments into one of the existing small Segments, if the Segment was reopened with a larger segment size" in {
      if (memory) {
        //memory Level cannot be reopened.
      } else {
        runThis(1.times) {
          TestCaseSweeper {
            implicit sweeper =>
              import sweeper._

              //          implicit val compressionType: Option[KeyValueCompressionType] = randomCompressionTypeOption(keyValuesCount)
              //disable throttling so that it does not automatically collapse small Segments
              val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, pushForward = false, deleteEventually = false, mmap = mmapSegments))

              assertAllSegmentsCreatedInLevel(level)

              val keyValues = randomPutKeyValues(1000, addPutDeadlines = false)(TestTimer.Empty)
              level.putKeyValuesTest(keyValues).runRandomIO.right.value
              //dispatch another push to trigger split
              level.putKeyValuesTest(Slice(keyValues.head)).runRandomIO.right.value

              level.segmentsCount() > 1 shouldBe true
              level.closeNoSweep().runRandomIO.right.value

              //reopening the Level will make the Segments unreadable.
              //reopen the Segments
              val segments =
              level.segmentsInLevel().map {
                case segment: PersistentSegment =>
                  Segment(
                    path = segment.path,
                    mmap = mmapSegments,
                    checkExists = true
                  )

                case _ =>
                  fail("Expected PersistentSegment")
              }

              //reopen the Level with larger min segment size
              val reopenLevel = level.reopen(segmentSize = 20.mb)
              reopenLevel.collapse(segments, randomMaxParallelism()).right.right.value.right.value

              //resulting segments is 1
              eventually {
                level.segmentFilesOnDisk should have size 1
              }
              //can still read Segments
              assertReads(keyValues, reopenLevel)
              val reopen2 = reopenLevel.reopen
              eventual(assertReads(keyValues, reopen2))

          }
        }
      }
    }

    "clear expired key-values" in {
      //this test is similar as the above collapsing small Segment test.
      //Remove or expiring key-values should have the same result
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))
          val expiryAt = 5.seconds.fromNow
          val keyValues = randomPutKeyValues(1000, valueSize = 0, startId = Some(0), addPutDeadlines = false)(TestTimer.Empty)
          level.putKeyValuesTest(keyValues).runRandomIO.right.value
          val segmentCountBeforeDelete = level.segmentsCount()
          segmentCountBeforeDelete > 1 shouldBe true

          val keyValuesNotExpired = ListBuffer.empty[KeyValue]
          val expireEverySecond =
            keyValues.zipWithIndex flatMap {
              case (keyValue, index) =>
                if (index % 2 == 0)
                  Some(Memory.Remove(keyValue.key, Some(expiryAt + index.millisecond), Time.empty))
                else {
                  keyValuesNotExpired += keyValue
                  None
                }
            }

          //delete half of the key values which will create small Segments
          level.putKeyValuesTest(Slice(expireEverySecond.toArray)).runRandomIO.right.value
          keyValues.zipWithIndex foreach {
            case (keyValue, index) =>
              if (index % 2 == 0)
                level.get(keyValue.key, ThreadReadState.random).runRandomIO.right.value.toOptionPut.value.deadline should contain(expiryAt + index.millisecond)
          }

          sleep(20.seconds)
          level.collapse(level.segmentsInLevel(), randomMaxParallelism()).right.right.value.right.value
          level.segmentFilesInAppendix should be <= ((segmentCountBeforeDelete / 2) + 1)

          assertReads(Slice(keyValuesNotExpired.toArray), level)
      }
    }
  }

  "update createdInLevel" in {
    TestCaseSweeper {
      implicit sweeper =>
        val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))

        val keyValues = randomPutKeyValues(keyValuesCount, addExpiredPutDeadlines = false)
        val maps = TestMap(keyValues)
        level.put(maps, randomMaxParallelism()).right.right.value.right.value

        val nextLevel = TestLevel()
        nextLevel.put(level.segmentsInLevel(), randomMaxParallelism()).right.right.value.right.value

        if (persistent) nextLevel.segmentsInLevel() foreach (_.createdInLevel shouldBe level.levelNumber)
        nextLevel.collapse(nextLevel.segmentsInLevel(), randomMaxParallelism()).right.right.value.right.value
        nextLevel.segmentsInLevel() foreach (_.createdInLevel shouldBe nextLevel.levelNumber)
    }
  }
}
