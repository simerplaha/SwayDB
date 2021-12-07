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

package swaydb.core.tool

import swaydb.Glass
import swaydb.IOValues._
import swaydb.config.MMAP
import swaydb.config.compaction.LevelThrottle
import swaydb.config.repairAppendix.{AppendixRepairStrategy, OverlappingSegmentsException}
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.{CoreTestBase, TestCaseSweeper, TestExecutionContext, TestForceSave}
import swaydb.effect.Effect
import swaydb.effect.Effect._
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._

import java.nio.file.NoSuchFileException
import scala.concurrent.duration.{Duration, DurationInt}
import scala.util.Random

class AppendixRepairerSpec extends CoreTestBase {

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
            val level = TestLevel(segmentConfig = SegmentBlockConfig.random(minSegmentSize = 1.kb, deleteDelay = Duration.Zero, mmap = MMAP.Off(TestForceSave.channel())))
            level.put(randomizedKeyValues(10000)).value

            if (level.hasMMAP && OperatingSystem.isWindows)
              level.close[Glass]()

            level.segmentsCount() should be > 2
            val segmentsBeforeRepair = level.segments()

            //repair appendix
            AppendixRepairer(level.rootPath, AppendixRepairStrategy.ReportFailure).value
            level.appendixPath.exists shouldBe true //appendix is created

            //reopen level and it should contain all the Segment
            val reopenedLevel = level.reopen
            reopenedLevel.segments().map(_.path) shouldBe segmentsBeforeRepair.map(_.path)
        }
      }
    }

    "create empty appendix file if the Level is empty" in {
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            //create empty Level
            val level = TestLevel(segmentConfig = SegmentBlockConfig.random(minSegmentSize = 1.kb, deleteDelay = Duration.Zero, mmap = MMAP.Off(TestForceSave.channel())))

            if (level.hasMMAP && OperatingSystem.isWindows) {
              level.close[Glass]()
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
      runThis(10.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            //create a Level with a sub-level and disable throttling so that compaction does not delete expired key-values
            val level = TestLevel(
              segmentConfig = SegmentBlockConfig.random(minSegmentSize = 1.kb, deleteDelay = Duration.Zero, mmap = MMAP.Off(TestForceSave.channel())),
              nextLevel = Some(TestLevel()),
              throttle = _ => LevelThrottle(Duration.Zero, 0)
            )

            val keyValues = randomizedKeyValues(1000)
            level.put(keyValues).value

            if (level.hasMMAP && OperatingSystem.isWindows)
              level.close[Glass]()

            level.segmentsCount() should be > 2
            val segmentsBeforeRepair = level.segments()

            level.segments().foldLeft(segmentsBeforeRepair.last.path.fileId._1 + 1) {
              case (segmentNumber, segment) =>
                //create a duplicate Segment
                val duplicateSegment = segment.path.getParent.resolve(segmentNumber.toSegmentFileId)
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
                segment.existsOnDisk() shouldBe false
                segmentNumber + 1
            }

            //level still contains the same key-values
            val reopenedLevel = level.reopen
            reopenedLevel.segments().flatMap(_.iterator(randomBoolean())).runRandomIO.value shouldBe keyValues
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
            segmentConfig = SegmentBlockConfig.random(minSegmentSize = 1.kb, deleteDelay = Duration.Zero, mmap = MMAP.Off(TestForceSave.channel())),
            nextLevel = Some(TestLevel()),
            throttle = (_) => LevelThrottle(Duration.Zero, 0)
          )

          level.put(keyValues).value

          level.segmentsCount() should be > 2
          val segmentsBeforeRepair = level.segments()

          if (level.hasMMAP && OperatingSystem.isWindows) {
            level.close[Glass]()
            sweeper.receiveAll()
          }

          level.segments().foldLeft(segmentsBeforeRepair.last.path.fileId._1 + 1) {
            case (overlappingSegmentId, segment) =>
              val overlappingLevelSegmentPath = level.rootPath.resolve(overlappingSegmentId.toSegmentFileId)

              def createOverlappingSegment() = {
                val numberOfKeyValuesToOverlap = randomNextInt(3) max 1
                val keyValuesToOverlap = Random.shuffle(segment.iterator(randomBoolean()).runRandomIO.value.toList).take(numberOfKeyValuesToOverlap).map(_.toMemory()).toSlice
                //create overlapping Segment
                val overlappingSegment = TestSegment(keyValuesToOverlap, segmentConfig = SegmentBlockConfig.random(mmap = MMAP.Off(TestForceSave.channel())))
                Effect.copy(overlappingSegment.path, overlappingLevelSegmentPath)
                overlappingSegment.close() //gotta close the new segment create after it's copied over.
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
              segment.existsOnDisk() shouldBe false

              overlappingSegmentId + 1
          }

          level.reopen.segmentsCount() shouldBe segmentsBeforeRepair.size
      }
    }
  }
}
