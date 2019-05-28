/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.tool

import java.nio.file.NoSuchFileException
import scala.concurrent.duration.Duration
import scala.util.Random
import swaydb.core.CommonAssertions._
import swaydb.core.IOAssert._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.io.file.IOEffect
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.Segment
import swaydb.core.io.file.IOEffect._
import swaydb.core.{TestBase, TestData, TestLimitQueues}
import swaydb.data.compaction.Throttle
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.repairAppendix.{AppendixRepairStrategy, OverlappingSegmentsException}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class AppendixRepairerSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter

  "AppendixRepair" should {
    "fail if the input path does not exist" in {
      AppendixRepairer(nextLevelPath, AppendixRepairStrategy.ReportFailure).failed.assertGet.exception shouldBe a[NoSuchFileException]
    }

    "create new appendix file if all the Segments in the Level are non-overlapping Segments" in {
      val level = TestLevel(segmentSize = 1.kb)
      level.putKeyValuesTest(randomizedKeyValues(10000).toMemory).assertGet

      level.segmentsCount() should be > 2
      val segmentsBeforeRepair = level.segmentsInLevel()

      //repair appendix
      AppendixRepairer(level.rootPath, AppendixRepairStrategy.ReportFailure).assertGet
      level.appendixPath.exists shouldBe true //appendix is created

      //reopen level and it should contain all the Segment
      val reopenedLevel = level.reopen
      reopenedLevel.segmentsInLevel().map(_.path) shouldBe segmentsBeforeRepair.map(_.path)
      reopenedLevel.close.assertGet
    }

    "create empty appendix file if the Level is empty" in {
      //create empty Level
      val level = TestLevel(segmentSize = 1.kb)

      //delete appendix
      IOEffect.walkDelete(level.appendixPath).assertGet
      level.appendixPath.exists shouldBe false

      //repair appendix
      AppendixRepairer(level.rootPath, AppendixRepairStrategy.ReportFailure).assertGet
      level.appendixPath.exists shouldBe true //appendix is created

      //reopen level, the Level is empty
      val reopenedLevel = level.reopen
      reopenedLevel.isEmpty shouldBe true
      reopenedLevel.close.assertGet
    }

    "report duplicate Segments" in {
      //create a Level with a sub-level and disable throttling so that compaction does not delete expired key-values
      val level = TestLevel(segmentSize = 1.kb, nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomizedKeyValues(1000).toMemory
      level.putKeyValuesTest(keyValues).assertGet

      level.segmentsCount() should be > 2
      val segmentsBeforeRepair = level.segmentsInLevel()
      level.segmentsInLevel().foldLeft(segmentsBeforeRepair.last.path.fileId.assertGet._1 + 1) {
        case (segmentId, segment) =>
          //create a duplicate Segment
          val duplicateSegment = segment.path.getParent.resolve(segmentId.toSegmentFileId)
          segment.copyTo(duplicateSegment).assertGet
          //perform repair
          AppendixRepairer(level.rootPath, AppendixRepairStrategy.ReportFailure).failed.assertGet.exception shouldBe a[OverlappingSegmentsException]
          //perform repair with DeleteNext. This will delete the newest duplicate Segment.
          AppendixRepairer(level.rootPath, AppendixRepairStrategy.KeepOld).assertGet
          //newer duplicate Segment is deleted
          duplicateSegment.exists shouldBe false

          //copy again
          segment.copyTo(duplicateSegment).assertGet
          //now use delete previous instead
          AppendixRepairer(level.rootPath, AppendixRepairStrategy.KeepNew).assertGet
          //newer duplicate Segment exists
          duplicateSegment.exists shouldBe true
          //older duplicate Segment is deleted
          segment.existsOnDisk shouldBe false
          segmentId + 1
      }
      //level still contains the same key-values
      val reopenedLevel = level.reopen
      Segment.getAllKeyValues(TestData.falsePositiveRate, reopenedLevel.segmentsInLevel()).assertGet shouldBe keyValues
      reopenedLevel.close.assertGet
    }

    "report overlapping min & max key Segments & delete newer overlapping Segment if KeepOld is selected" in {
      //create empty Level
      val level = TestLevel(segmentSize = 1.kb, nextLevel = Some(TestLevel()), throttle = (_) => Throttle(Duration.Zero, 0))

      val keyValues = randomizedKeyValues(10000).toMemory
      level.putKeyValuesTest(keyValues).assertGet

      level.segmentsCount() should be > 2
      val segmentsBeforeRepair = level.segmentsInLevel()
      level.segmentsInLevel().foldLeft(segmentsBeforeRepair.last.path.fileId.assertGet._1 + 1) {
        case (overlappingSegmentId, segment) =>
          val overlappingLevelSegmentPath = level.rootPath.resolve(overlappingSegmentId.toSegmentFileId)

          def createOverlappingSegment() = {
            val numberOfKeyValuesToOverlap = randomNextInt(3) max 1
            val keyValuesToOverlap = Random.shuffle(segment.getAll().assertGet.toList).take(numberOfKeyValuesToOverlap)
            //create overlapping Segment
            val overlappingSegment = TestSegment(keyValuesToOverlap.toTransient).assertGet
            overlappingSegment.copyTo(overlappingLevelSegmentPath).assertGet
            overlappingSegment.close.assertGet //gotta close the new segment create after it's copied over.
          }

          createOverlappingSegment()
          //perform repair with Report
          AppendixRepairer(level.rootPath, AppendixRepairStrategy.ReportFailure).failed.assertGet.exception shouldBe a[OverlappingSegmentsException]
          //perform repair with DeleteNext. This will delete the newest overlapping Segment.
          AppendixRepairer(level.rootPath, AppendixRepairStrategy.KeepOld).assertGet
          //overlapping Segment does not exist.
          overlappingLevelSegmentPath.exists shouldBe false

          //create overlapping Segment again but this time do DeletePrevious
          createOverlappingSegment()
          AppendixRepairer(level.rootPath, AppendixRepairStrategy.KeepNew).assertGet
          //newer overlapping Segment exists
          overlappingLevelSegmentPath.exists shouldBe true
          //older overlapping Segment is deleted
          segment.existsOnDisk shouldBe false

          overlappingSegmentId + 1
      }
      val reopenedLevel = level.reopen
      reopenedLevel.segmentsCount() shouldBe segmentsBeforeRepair.size
      reopenedLevel.close.assertGet
    }
  }

}
