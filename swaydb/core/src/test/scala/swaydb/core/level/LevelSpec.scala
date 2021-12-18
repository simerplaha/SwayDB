///*
// * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// * http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package swaydb.core.level
//
//import org.scalamock.scalatest.MockFactory
//import org.scalatest.PrivateMethodTester
//import swaydb.Glass
//import swaydb.effect.IOValues._
//import swaydb.config.MMAP
//import swaydb.config.storage.LevelStorage
//import swaydb.core._
//import swaydb.core.CoreTestData._
//import swaydb.core.file.CoreFileTestKit._
//import swaydb.core.log.LogEntry
//import swaydb.core.segment.Segment
//import swaydb.core.segment.block.segment.SegmentBlockConfig
//import swaydb.core.segment.data._
//import swaydb.effect.{Dir, Effect}
//import swaydb.effect.Effect._
//import swaydb.serializers._
//import swaydb.serializers.Default._
//import swaydb.slice.Slice
//import swaydb.slice.order.{KeyOrder, TimeOrder}
//import swaydb.testkit.TestKit._
//import swaydb.utils.{Extension, OperatingSystem}
//import swaydb.utils.StorageUnits._
//
//import java.nio.channels.OverlappingFileLockException
//
//class LevelSpec0 extends LevelSpec
//
//class LevelSpec1 extends LevelSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//}
//
//class LevelSpec2 extends LevelSpec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
//  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
//  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
//}
//
//class LevelSpec3 extends LevelSpec {
//  override def isMemorySpec = true
//}
//
//sealed trait LevelSpec extends AnyWordSpec  {
//
//  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
//  implicit val testTimer: TestTimer = TestTimer.Empty
//  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
//  implicit val ec = TestExecutionContext.executionContext
//  val keyValuesCount = 100
//
//  //    override def deleteFiles: Boolean =
//  //      false
//
//  "acquireLock" should {
//    "create a lock file for only the root directory and not allow more locks" in {
//      //memory databases do not perform locks
//      if (isPersistent) {
//        CoreTestSweeper {
//          implicit sweeper =>
//
//            val otherDirs = (0 to randomIntMax(5)) map (_ => Dir(randomDir(), 1))
//            val storage =
//              LevelStorage.Persistent(
//                dir = randomDir(),
//                otherDirs = otherDirs,
//                appendixMMAP = MMAP.randomForLog(),
//                appendixFlushCheckpointSize = 4.mb
//              )
//
//            val lock = Level.acquireLock(storage).runRandomIO.get
//            lock shouldBe defined
//            //other directories do not have locks.
//            storage.otherDirs foreach {
//              dir =>
//                Effect.exists(dir.path.resolve("LOCK")) shouldBe false
//            }
//
//            //trying to lock again should fail
//            Level.acquireLock(storage).left.runRandomIO.get.exception shouldBe a[OverlappingFileLockException]
//
//            //closing the lock should allow re-locking
//            Effect.release(lock)
//            Level.acquireLock(storage).runRandomIO.get shouldBe defined
//        }
//      }
//    }
//  }
//
//  "apply" should {
//    "create level" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val level = TestLevel()
//
//          if (isMemorySpec) {
//            //memory level always have one folder
//            level.dirs should have size 1
//            level.existsOnDisk() shouldBe false
//            level.inMemory shouldBe true
//            //        level.valuesConfig.compressDuplicateValues shouldBe true
//          } else {
//            level.existsOnDisk() shouldBe true
//            level.inMemory shouldBe false
//
//            //there shouldBe at least one path
//            level.dirs should not be empty
//
//            //appendix path gets added to the head path
//            val appendixPath = level.pathDistributor.headPath.resolve("appendix")
//            appendixPath.exists shouldBe true
//            appendixPath.resolve("0.log").exists shouldBe true
//
//            //all paths should exists
//            level.dirs.foreach(_.path.exists shouldBe true)
//          }
//
//          level.segments() shouldBe empty
//      }
//    }
//
//    "report error if appendix file and folder does not exists" in {
//      if (isPersistent) {
//        CoreTestSweeper {
//          implicit sweeper =>
//            import sweeper._
//
//            //create a non empty level
//            val level = TestLevel()
//
//            val segment = TestSegment(randomKeyValues(keyValuesCount))
//
//            level.put(segment).value
//
//            if (segment.isMMAP && OperatingSystem.isWindows()) {
//              level.close[Glass]()
//              sweeper.receiveAll()
//            }
//
//            //delete the appendix file
//            level.pathDistributor.headPath.resolve("appendix").files(Extension.Log) foreach Effect.delete
//            //expect failure when file does not exists
//            level.tryReopen.left.get.exception shouldBe a[IllegalStateException]
//
//            //delete folder
//            Effect.delete(level.pathDistributor.headPath.resolve("appendix")).runRandomIO.get
//            //expect failure when folder does not exist
//            level.tryReopen.left.get.exception shouldBe a[IllegalStateException]
//        }
//      }
//    }
//  }
//
//  "deleteUncommittedSegments" should {
//    "delete segments that are not in the appendix" in {
//      if (isMemorySpec) {
//        // memory Level do not have uncommitted Segments
//      } else {
//        CoreTestSweeper {
//          implicit sweeper =>
//            import sweeper._
//
//            val level = TestLevel()
//
//            val keyValues = randomPutKeyValues()
//            level.put(keyValues).value
//
//            if (isWindowsAndMMAPSegments())
//              sweeper.receiveAll()
//
//            val segmentsIdsBeforeInvalidSegments = level.segmentFilesOnDisk()
//            segmentsIdsBeforeInvalidSegments should have size 1
//
//            val currentSegmentId = segmentsIdsBeforeInvalidSegments.head.fileId.runRandomIO.get._1
//
//            //create 3 invalid segments in all the paths of the Level
//            level.dirs.foldLeft(currentSegmentId) {
//              case (currentSegmentId, dir) =>
//                //deleteUncommittedSegments will also be invoked on Levels with cleared and closed Segments there will never be
//                //memory-mapped. So disable mmap in this test specially for windows which does not allow deleting memory-mapped files without
//                //clearing the MappedByteBuffer.
//                TestSegment(path = dir.path.resolve((currentSegmentId + 1).toSegmentFileId), segmentConfig = SegmentBlockConfig.random(mmap = mmapSegments).copy(mmap = MMAP.Off(TestForceSave.standard())))
//                TestSegment(path = dir.path.resolve((currentSegmentId + 2).toSegmentFileId), segmentConfig = SegmentBlockConfig.random(mmap = mmapSegments).copy(mmap = MMAP.Off(TestForceSave.standard())))
//                TestSegment(path = dir.path.resolve((currentSegmentId + 3).toSegmentFileId), segmentConfig = SegmentBlockConfig.random(mmap = mmapSegments).copy(mmap = MMAP.Off(TestForceSave.standard())))
//                currentSegmentId + 3
//            }
//            //every level folder has 3 uncommitted Segments plus 1 valid Segment
//            level.segmentFilesOnDisk() should have size (level.dirs.size * 3) + 1
//
//            Level.deleteUncommittedSegments(level.dirs, level.segments()).runRandomIO.get
//
//            level.segmentFilesOnDisk() should have size 1
//            level.segmentFilesOnDisk() should contain only segmentsIdsBeforeInvalidSegments.head
//            level.reopen.segmentFilesOnDisk() should contain only segmentsIdsBeforeInvalidSegments.head
//        }
//      }
//    }
//  }
//
//  "largestSegmentId" should {
//    "value the largest segment in the Level when the Level is not empty" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          import sweeper._
//
//          val level = TestLevel(segmentConfig = SegmentBlockConfig.random(minSegmentSize = 1.kb, mmap = mmapSegments))
//          level.put(randomizedKeyValues(2000)).runRandomIO.get
//
//          val largeSegmentId = Level.largestSegmentId(level.segments())
//          largeSegmentId shouldBe level.segments().map(_.path.fileId.runRandomIO.get._1).max
//      }
//    }
//
//    "return 0 when the Level is empty" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val level = TestLevel(segmentConfig = SegmentBlockConfig.random(minSegmentSize = 1.kb, mmap = mmapSegments))
//
//          Level.largestSegmentId(level.segments()) shouldBe 0
//      }
//    }
//  }
//
//  "buildNewLogEntry" should {
//    import swaydb.core.log.serialiser.AppendixLogEntryWriter._
//
//    "build LogEntry.Put map for the first created Segment" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val level = TestLevel()
//
//          val segments = TestSegment(Slice(Memory.put(1, "value1"), Memory.put(2, "value2"))).runRandomIO.get
//          val actualLogEntry = level.buildNewLogEntry(Slice(segments), originalSegmentMayBe = Segment.Null, initialLogEntry = None).runRandomIO.get
//          val expectedLogEntry = LogEntry.Put[Slice[Byte], Segment](segments.minKey, segments)
//
//          actualLogEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
//            expectedLogEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])
//      }
//    }
//
//    "build LogEntry.Put map for the newly merged Segments and not add LogEntry.Remove map " +
//      "for original Segment as it's minKey is replace by one of the new Segment" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val level = TestLevel()
//
//          val originalSegment = TestSegment(Slice(Memory.put(1, "value"), Memory.put(5, "value"))).runRandomIO.get
//          val mergedSegment1 = TestSegment(Slice(Memory.put(1, "value"), Memory.put(5, "value"))).runRandomIO.get
//          val mergedSegment2 = TestSegment(Slice(Memory.put(6, "value"), Memory.put(10, "value"))).runRandomIO.get
//          val mergedSegment3 = TestSegment(Slice(Memory.put(11, "value"), Memory.put(15, "value"))).runRandomIO.get
//
//          val actualLogEntry = level.buildNewLogEntry(Slice(mergedSegment1, mergedSegment2, mergedSegment3), originalSegment, initialLogEntry = None).runRandomIO.get
//
//          val expectedLogEntry =
//            LogEntry.Put[Slice[Byte], Segment](1, mergedSegment1) ++
//              LogEntry.Put[Slice[Byte], Segment](6, mergedSegment2) ++
//              LogEntry.Put[Slice[Byte], Segment](11, mergedSegment3)
//
//          actualLogEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
//            expectedLogEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])
//      }
//    }
//
//    "build LogEntry.Put map for the newly merged Segments and also add Remove map entry for original map when all minKeys are unique" in {
//      CoreTestSweeper {
//        implicit sweeper =>
//          val level = TestLevel()
//
//          val originalSegment = TestSegment(Slice(Memory.put(0, "value"), Memory.put(5, "value"))).runRandomIO.get
//          val mergedSegment1 = TestSegment(Slice(Memory.put(1, "value"), Memory.put(5, "value"))).runRandomIO.get
//          val mergedSegment2 = TestSegment(Slice(Memory.put(6, "value"), Memory.put(10, "value"))).runRandomIO.get
//          val mergedSegment3 = TestSegment(Slice(Memory.put(11, "value"), Memory.put(15, "value"))).runRandomIO.get
//
//          val expectedLogEntry =
//            LogEntry.Put[Slice[Byte], Segment](1, mergedSegment1) ++
//              LogEntry.Put[Slice[Byte], Segment](6, mergedSegment2) ++
//              LogEntry.Put[Slice[Byte], Segment](11, mergedSegment3) ++
//              LogEntry.Remove[Slice[Byte]](0)
//
//          val actualLogEntry = level.buildNewLogEntry(Slice(mergedSegment1, mergedSegment2, mergedSegment3), originalSegment, initialLogEntry = None).runRandomIO.get
//
//          actualLogEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int]) shouldBe
//            expectedLogEntry.asString(_.read[Int].toString, segment => segment.path.toString + segment.maxKey.maxKey.read[Int])
//      }
//    }
//  }
//}
