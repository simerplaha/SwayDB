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

package swaydb.core.level.tool

import java.nio.file.NoSuchFileException

import swaydb.Bag
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.io.file.Effect
import swaydb.core.io.file.Effect._
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestForceSave}
import swaydb.data.RunThis._
import swaydb.data.compaction.Throttle
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.repairAppendix.{AppendixRepairStrategy, OverlappingSegmentsException}
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.Random

class AppendixRepairerSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val ec = TestExecutionContext.executionContext

  "AppendixRepair" should {
    "fail if the input path does not exist" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          AppendixRepairer(nextLevelPath, AppendixRepairStrategy.ReportFailure).left.value.exception shouldBe a[NoSuchFileException]
      }
    }

    "create new appendix file if all the Segments in the Level are non-overlapping Segments" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, deleteEventually = false, pushForward = false, mmap = MMAP.Off(TestForceSave.channel())))
            level.putKeyValuesTest(randomizedKeyValues(10000)).value

            if (level.hasMMAP && OperatingSystem.isWindows)
              level.close[Bag.Glass]()

            level.segmentsCount() should be > 2
            val segmentsBeforeRepair = level.segmentsInLevel()

            //repair appendix
            AppendixRepairer(level.rootPath, AppendixRepairStrategy.ReportFailure).value
            level.appendixPath.exists shouldBe true //appendix is created

            //reopen level and it should contain all the Segment
            val reopenedLevel = level.reopen
            reopenedLevel.segmentsInLevel().map(_.path) shouldBe segmentsBeforeRepair.map(_.path)
        }
      }
    }

    "create empty appendix file if the Level is empty" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            //create empty Level
            val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, deleteEventually = false, pushForward = false, mmap = MMAP.Off(TestForceSave.channel())))

            if (level.hasMMAP && OperatingSystem.isWindows) {
              level.close[Bag.Glass]()
              eventual(10.seconds) {
                sweeper.receiveAll()
                Effect.walkDelete(level.appendixPath)
              }
            } else {
              Effect.walkDelete(level.appendixPath)
            }

            //delete appendix
            level.appendixPath.exists shouldBe false

            //repair appendix
            AppendixRepairer(level.rootPath, AppendixRepairStrategy.ReportFailure).value
            level.appendixPath.exists shouldBe true //appendix is created

            //reopen level, the Level is empty
            val reopenedLevel = level.reopen
            reopenedLevel.isEmpty shouldBe true
            reopenedLevel.closeNoSweep().value
        }
      }
    }

    "report duplicate Segments" in {
      runThis(1.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            //create a Level with a sub-level and disable throttling so that compaction does not delete expired key-values
            val level = TestLevel(
              segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, deleteEventually = false, pushForward = false, mmap = MMAP.Off(TestForceSave.channel())),
              nextLevel = Some(TestLevel()),
              throttle = (_) => Throttle(Duration.Zero, 0)
            )

            val keyValues = randomizedKeyValues(1000)
            level.putKeyValuesTest(keyValues).value

            if (level.hasMMAP && OperatingSystem.isWindows)
              level.close[Bag.Glass]()

            level.segmentsCount() should be > 2
            val segmentsBeforeRepair = level.segmentsInLevel()

            level.segmentsInLevel().foldLeft(segmentsBeforeRepair.last.path.fileId._1 + 1) {
              case (segmentId, segment) =>
                //create a duplicate Segment
                val duplicateSegment = segment.path.getParent.resolve(segmentId.toSegmentFileId)
                Effect.copy(segment.path, duplicateSegment)
                //perform repair
                AppendixRepairer(level.rootPath, AppendixRepairStrategy.ReportFailure).left.value.exception shouldBe a[OverlappingSegmentsException]
                //perform repair with DeleteNext. This will delete the newest duplicate Segment.
                AppendixRepairer(level.rootPath, AppendixRepairStrategy.KeepOld).value
                //newer duplicate Segment is deleted
                duplicateSegment.exists shouldBe false

                //copy again
                Effect.copy(segment.path, duplicateSegment)
                //now use delete previous instead
                AppendixRepairer(level.rootPath, AppendixRepairStrategy.KeepNew).value
                //newer duplicate Segment exists
                duplicateSegment.exists shouldBe true
                //older duplicate Segment is deleted
                segment.existsOnDisk shouldBe false
                segmentId + 1
            }

            //level still contains the same key-values
            val reopenedLevel = level.reopen
            Segment.getAllKeyValues(reopenedLevel.segmentsInLevel()).runRandomIO.value shouldBe keyValues
            reopenedLevel.closeNoSweep().value
        }
      }
    }

    "report overlapping min & max key Segments & delete newer overlapping Segment if KeepOld is selected" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._
          //create empty Level
          val keyValues = randomizedKeyValues(1000)

          val level = TestLevel(
            segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, deleteEventually = false, pushForward = false, mmap = MMAP.Off(TestForceSave.channel())),
            nextLevel = Some(TestLevel()),
            throttle = (_) => Throttle(Duration.Zero, 0)
          )

          level.putKeyValuesTest(keyValues).value

          level.segmentsCount() should be > 2
          val segmentsBeforeRepair = level.segmentsInLevel()

          if (level.hasMMAP && OperatingSystem.isWindows) {
            level.close[Bag.Glass]()
            sweeper.receiveAll()
          }

          level.segmentsInLevel().foldLeft(segmentsBeforeRepair.last.path.fileId._1 + 1) {
            case (overlappingSegmentId, segment) =>
              val overlappingLevelSegmentPath = level.rootPath.resolve(overlappingSegmentId.toSegmentFileId)

              def createOverlappingSegment() = {
                val numberOfKeyValuesToOverlap = randomNextInt(3) max 1
                val keyValuesToOverlap = Random.shuffle(segment.toSlice().runRandomIO.value.toList).take(numberOfKeyValuesToOverlap).map(_.toMemory).toSlice
                //create overlapping Segment
                val overlappingSegment = TestSegment(keyValuesToOverlap, segmentConfig = SegmentBlock.Config.random(mmap = MMAP.Off(TestForceSave.channel())))
                Effect.copy(overlappingSegment.path, overlappingLevelSegmentPath)
                overlappingSegment.close //gotta close the new segment create after it's copied over.
                if (level.hasMMAP && OperatingSystem.isWindows)
                  sweeper.receiveAll()
              }

              createOverlappingSegment()
              //perform repair with Report
              AppendixRepairer(level.rootPath, AppendixRepairStrategy.ReportFailure).left.value.exception shouldBe a[OverlappingSegmentsException]
              //perform repair with DeleteNext. This will delete the newest overlapping Segment.
              AppendixRepairer(level.rootPath, AppendixRepairStrategy.KeepOld).value
              //overlapping Segment does not exist.
              overlappingLevelSegmentPath.exists shouldBe false

              //create overlapping Segment again but this time do DeletePrevious
              createOverlappingSegment()
              AppendixRepairer(level.rootPath, AppendixRepairStrategy.KeepNew).value
              //newer overlapping Segment exists
              overlappingLevelSegmentPath.exists shouldBe true
              //older overlapping Segment is deleted
              segment.existsOnDisk shouldBe false

              overlappingSegmentId + 1
          }

          level.reopen.segmentsCount() shouldBe segmentsBeforeRepair.size
      }
    }
  }
}
