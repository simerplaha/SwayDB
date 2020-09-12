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

import java.nio.channels.OverlappingFileLockException

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core.TestCaseSweeper._
import swaydb.core.data._
import swaydb.core.io.file.Effect
import swaydb.core.io.file.Effect._
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.map.MapEntry
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.util.{Extension, ReserveRange}
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestForceSave, TestTimer}
import swaydb.data.config.{Dir, ForceSave, MMAP}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice._
import swaydb.data.storage.LevelStorage
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.data.RunThis._

import scala.concurrent.Promise
import scala.concurrent.duration.DurationInt
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice.Slice

class LevelSpec0 extends LevelSpec

class LevelSpec1 extends LevelSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.Enabled(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelSpec2 extends LevelSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Disabled(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Disabled(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Disabled(forceSave = TestForceSave.channel())
}

class LevelSpec3 extends LevelSpec {
  override def inMemoryStorage = true
}

sealed trait LevelSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val ec = TestExecutionContext.executionContext
  val keyValuesCount = 100

  //    override def deleteFiles: Boolean =
  //      false

  implicit val skipListMerger = LevelZeroSkipListMerger

  "acquireLock" should {
    "create a lock file for only the root directory and not allow more locks" in {
      //memory databases do not perform locks
      if (persistent) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._
            val otherDirs = (0 to randomIntMax(5)) map (_ => Dir(randomDir, 1))
            val storage =
              LevelStorage.Persistent(
                dir = randomDir,
                otherDirs = otherDirs
              )

            val lock = Level.acquireLock(storage).runRandomIO.right.value
            lock shouldBe defined
            //other directories do not have locks.
            storage.otherDirs foreach {
              dir =>
                Effect.exists(dir.path.resolve("LOCK")) shouldBe false
            }

            //trying to lock again should fail
            Level.acquireLock(storage).left.runRandomIO.right.value.exception shouldBe a[OverlappingFileLockException]

            //closing the lock should allow re-locking
            Effect.release(lock)
            Level.acquireLock(storage).runRandomIO.right.value shouldBe defined
        }
      }
    }
  }

  "apply" should {
    "create level" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          if (memory) {
            //memory level always have one folder
            level.dirs should have size 1
            level.existsOnDisk shouldBe false
            level.inMemory shouldBe true
            //        level.valuesConfig.compressDuplicateValues shouldBe true
          } else {
            level.existsOnDisk shouldBe true
            level.inMemory shouldBe false

            //there shouldBe at least one path
            level.dirs should not be empty

            //appendix path gets added to the head path
            val appendixPath = level.pathDistributor.headPath.resolve("appendix")
            appendixPath.exists shouldBe true
            appendixPath.resolve("0.log").exists shouldBe true

            //all paths should exists
            level.dirs.foreach(_.path.exists shouldBe true)
          }

          level.segmentsInLevel() shouldBe empty
          level.removeDeletedRecords shouldBe true
      }
    }

    "report error if appendix file and folder does not exists" in {
      if (persistent) {
        TestCaseSweeper {
          implicit sweeper =>
            //create a non empty level
            val level = TestLevel()

            val segment = TestSegment(randomKeyValues(keyValuesCount))

            level.put(segment).value.value should not be empty

            if (segment.isMMAP && OperatingSystem.isWindows) {
              level.close().await(10.seconds)
              sweeper.receiveAll()
            }

            //delete the appendix file
            level.pathDistributor.headPath.resolve("appendix").files(Extension.Log) map Effect.delete
            //expect failure when file does not exists
            level.tryReopen.left.get.exception shouldBe a[IllegalStateException]

            //delete folder
            Effect.delete(level.pathDistributor.headPath.resolve("appendix")).runRandomIO.right.value
            //expect failure when folder does not exist
            level.tryReopen.left.get.exception shouldBe a[IllegalStateException]
        }
      }
    }
  }

  "deleteUncommittedSegments" should {
    "delete segments that are not in the appendix" in {
      if (memory) {
        // memory Level do not have uncommitted Segments
      } else {
        TestCaseSweeper {
          implicit sweeper =>
            val level = TestLevel()

            val keyValues = randomPutKeyValues()
            level.putKeyValuesTest(keyValues).value

            if (isWindowsAndMMAPSegments())
              sweeper.receiveAll()

            val segmentsIdsBeforeInvalidSegments = level.segmentFilesOnDisk
            segmentsIdsBeforeInvalidSegments should have size 1

            val currentSegmentId = segmentsIdsBeforeInvalidSegments.head.fileId.runRandomIO.right.value._1

            //create 3 invalid segments in all the paths of the Level
            level.dirs.foldLeft(currentSegmentId) {
              case (currentSegmentId, dir) =>
                //deleteUncommittedSegments will also be invoked on Levels with cleared and closed Segments there will never be
                //memory-mapped. So disable mmap in this test specially for windows which does not allow deleting memory-mapped files without
                //clearing the MappedByteBuffer.
                TestSegment(path = dir.path.resolve((currentSegmentId + 1).toSegmentFileId), segmentConfig = SegmentBlock.Config.random(mmap = mmapSegments).copy(mmap = MMAP.Disabled(TestForceSave.channel())))
                TestSegment(path = dir.path.resolve((currentSegmentId + 2).toSegmentFileId), segmentConfig = SegmentBlock.Config.random(mmap = mmapSegments).copy(mmap = MMAP.Disabled(TestForceSave.channel())))
                TestSegment(path = dir.path.resolve((currentSegmentId + 3).toSegmentFileId), segmentConfig = SegmentBlock.Config.random(mmap = mmapSegments).copy(mmap = MMAP.Disabled(TestForceSave.channel())))
                currentSegmentId + 3
            }
            //every level folder has 3 uncommitted Segments plus 1 valid Segment
            level.segmentFilesOnDisk should have size (level.dirs.size * 3) + 1

            Level.deleteUncommittedSegments(level.dirs, level.segmentsInLevel()).runRandomIO.right.value

            level.segmentFilesOnDisk should have size 1
            level.segmentFilesOnDisk should contain only segmentsIdsBeforeInvalidSegments.head
            level.reopen.segmentFilesOnDisk should contain only segmentsIdsBeforeInvalidSegments.head
        }
      }
    }
  }

  "largestSegmentId" should {
    "value the largest segment in the Level when the Level is not empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))
          level.putKeyValuesTest(randomizedKeyValues(2000)).runRandomIO.right.value

          val largeSegmentId = Level.largestSegmentId(level.segmentsInLevel())
          largeSegmentId shouldBe level.segmentsInLevel().map(_.path.fileId.runRandomIO.right.value._1).max
      }
    }

    "return 0 when the Level is empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))

          Level.largestSegmentId(level.segmentsInLevel()) shouldBe 0
      }
    }
  }

  "optimalSegmentsToPushForward" should {
    "return empty if there Levels are empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          val nextLevel = TestLevel()
          val level = TestLevel()
          implicit val reserve = ReserveRange.create[Unit]()

          Level.optimalSegmentsToPushForward(
            level = level,
            nextLevel = nextLevel,
            take = 10
          ) shouldBe Level.emptySegmentsToPush
      }
    }

    "return all Segments to copy if next Level is empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          val nextLevel = TestLevel()
          val level = TestLevel(keyValues = randomizedKeyValues(count = 10000, startId = Some(1)), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))
          //      level.segmentsCount() should be >= 2

          implicit val reserve = ReserveRange.create[Unit]()

          val (toCopy, toMerge) =
            Level.optimalSegmentsToPushForward(
              level = level,
              nextLevel = nextLevel,
              take = 10
            )

          toMerge shouldBe empty
          toCopy.map(_.path) shouldBe level.segmentsInLevel().take(10).map(_.path)
      }
    }

    "return all unreserved Segments to copy if next Level is empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          val nextLevel = TestLevel()
          val level = TestLevel(keyValues = randomizedKeyValues(count = 10000, startId = Some(1)), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))

          level.segmentsCount() should be >= 2

          implicit val reserve = ReserveRange.create[Unit]()
          val firstSegment = level.segmentsInLevel().head

          ReserveRange.reserveOrGet(firstSegment.minKey, firstSegment.maxKey.maxKey, firstSegment.maxKey.inclusive, ()) shouldBe empty //reserve first segment

          val (toCopy, toMerge) =
            Level.optimalSegmentsToPushForward(
              level = level,
              nextLevel = nextLevel,
              take = 10
            )

          toMerge shouldBe empty
          toCopy.map(_.path) shouldBe level.segmentsInLevel().drop(1).take(10).map(_.path)
      }
    }
  }

  "optimalSegmentsToCollapse" should {
    "return empty if there Levels are empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          implicit val reserve = ReserveRange.create[Unit]()

          Level.optimalSegmentsToCollapse(
            level = level,
            take = 10
          ) shouldBe empty
      }
    }

    "return empty if all segments were reserved" in {
      TestCaseSweeper {
        implicit sweeper =>
          val keyValues = randomizedKeyValues(count = 10000, startId = Some(1))
          val level = TestLevel(keyValues = keyValues, segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))

          level.segmentsCount() should be >= 2

          implicit val reserve = ReserveRange.create[Unit]()

          val minKey = keyValues.head.key
          val maxKey = Segment.minMaxKey(level.segmentsInLevel()).get
          ReserveRange.reserveOrGet(minKey, maxKey._2, maxKey._3, ()) shouldBe empty

          Level.optimalSegmentsToCollapse(
            level = level,
            take = 10
          ) shouldBe empty
      }
    }
  }

  "reserve" should {
    "reserve keys for compaction where Level is empty" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          val keyValues = randomizedKeyValues(keyValuesCount).groupedSlice(2)
          val segment1 = TestSegment(keyValues.head).runRandomIO.right.value
          val segment2 = TestSegment(keyValues.last).runRandomIO.right.value
          level.reserve(Seq(segment1, segment2)).get shouldBe IO.Right[Promise[Unit], Slice[Byte]](keyValues.head.head.key)(IO.ExceptionHandler.PromiseUnit)

          //cannot reserve again
          level.reserve(Seq(segment1, segment2)).get shouldBe a[IO.Left[_, _]]
          level.reserve(Seq(segment1)).get shouldBe a[IO.Left[_, _]]
          level.reserve(Seq(segment2)).get shouldBe a[IO.Left[_, _]]
      }
    }

    "return completed Future for empty Segments" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          level.reserve(Seq.empty).get.left.get.isCompleted shouldBe true
      }
    }

    "reserve min and max keys" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          val keyValues = randomizedKeyValues(keyValuesCount).groupedSlice(2)
          val segments =
            Seq(
              TestSegment(keyValues.head).runRandomIO.right.value,
              TestSegment(keyValues.last).runRandomIO.right.value
            )
          level.put(segments).right.right.value.right.value
      }
    }
  }

  "buildNewMapEntry" should {
    import swaydb.core.map.serializer.AppendixMapEntryWriter._

    "build MapEntry.Put map for the first created Segment" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          val segments = TestSegment(Slice(Memory.put(1, "value1"), Memory.put(2, "value2"))).runRandomIO.right.value
          val actualMapEntry = level.buildNewMapEntry(Slice(segments), originalSegmentMayBe = Segment.Null, initialMapEntry = None).runRandomIO.right.value
          val expectedMapEntry = MapEntry.Put[Slice[Byte], Segment](segments.minKey, segments)

          actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
            expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])
      }
    }

    "build MapEntry.Put map for the newly merged Segments and not add MapEntry.Remove map " +
      "for original Segment as it's minKey is replace by one of the new Segment" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          val originalSegment = TestSegment(Slice(Memory.put(1, "value"), Memory.put(5, "value"))).runRandomIO.right.value
          val mergedSegment1 = TestSegment(Slice(Memory.put(1, "value"), Memory.put(5, "value"))).runRandomIO.right.value
          val mergedSegment2 = TestSegment(Slice(Memory.put(6, "value"), Memory.put(10, "value"))).runRandomIO.right.value
          val mergedSegment3 = TestSegment(Slice(Memory.put(11, "value"), Memory.put(15, "value"))).runRandomIO.right.value

          val actualMapEntry = level.buildNewMapEntry(Slice(mergedSegment1, mergedSegment2, mergedSegment3), originalSegment, initialMapEntry = None).runRandomIO.right.value

          val expectedMapEntry =
            MapEntry.Put[Slice[Byte], Segment](1, mergedSegment1) ++
              MapEntry.Put[Slice[Byte], Segment](6, mergedSegment2) ++
              MapEntry.Put[Slice[Byte], Segment](11, mergedSegment3)

          actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
            expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])
      }
    }

    "build MapEntry.Put map for the newly merged Segments and also add Remove map entry for original map when all minKeys are unique" in {
      TestCaseSweeper {
        implicit sweeper =>
          val level = TestLevel()

          val originalSegment = TestSegment(Slice(Memory.put(0, "value"), Memory.put(5, "value"))).runRandomIO.right.value
          val mergedSegment1 = TestSegment(Slice(Memory.put(1, "value"), Memory.put(5, "value"))).runRandomIO.right.value
          val mergedSegment2 = TestSegment(Slice(Memory.put(6, "value"), Memory.put(10, "value"))).runRandomIO.right.value
          val mergedSegment3 = TestSegment(Slice(Memory.put(11, "value"), Memory.put(15, "value"))).runRandomIO.right.value

          val expectedMapEntry =
            MapEntry.Put[Slice[Byte], Segment](1, mergedSegment1) ++
              MapEntry.Put[Slice[Byte], Segment](6, mergedSegment2) ++
              MapEntry.Put[Slice[Byte], Segment](11, mergedSegment3) ++
              MapEntry.Remove[Slice[Byte]](0)

          val actualMapEntry = level.buildNewMapEntry(Slice(mergedSegment1, mergedSegment2, mergedSegment3), originalSegment, initialMapEntry = None).runRandomIO.right.value

          actualMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
            expectedMapEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])
      }
    }
  }
}
