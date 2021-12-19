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
////
////package swaydb.core.compaction.throttle.behaviour
////
////import org.scalamock.scalatest.MockFactory
////import swaydb.Error.Segment.ExceptionHandler
////import swaydb.IO
////import swaydb.effect.IOValues._
////import swaydb.core.CommonAssertions._
////import swaydb.core.TestData._
////import swaydb.core.segment.data.Memory
////import swaydb.core.level.NextLevel
////import swaydb.core.segment.Segment
////import swaydb.core.segment.block.segment.SegmentBlock
////import swaydb.core._
////import swaydb.testkit.RunThis._
////import swaydb.config.compaction.ParallelMerge
////import swaydb.config.{MMAP, PushForwardStrategy}
////import swaydb.slice.order.{KeyOrder, TimeOrder}
////import swaydb.slice.Slice
////import swaydb.config.util.OperatingSystem
////import swaydb.config.util.StorageUnits._
////import swaydb.serializers.Default._
////import swaydb.serializers._
////
////import scala.collection.compat._
////import scala.collection.mutable.ListBuffer
////import scala.concurrent.ExecutionContext
////
////class CompactionSpec0 extends CompactionSpec
////
////class CompactionSpec1 extends CompactionSpec {
////  override def levelFoldersCount = 10
////  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
////  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
////  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
////}
////
////class CompactionSpec2 extends CompactionSpec {
////  override def levelFoldersCount = 10
////  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.standard())
////  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.standard())
////  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.standard())
////}
////
////class CompactionSpec3 extends CompactionSpec {
////  override def inMemoryStorage = true
////}
////
////sealed trait CompactionSpec extends TestBase  {
////
////  val keyValueCount = 1000
////
////  implicit val ec = TestExecutionContext.executionContext
////  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
////  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
////  implicit val timer = TestTimer.Empty
////
////  "putForward" should {
////    "return zero" when {
////      "input Segments are empty" in {
////        //levels are never invoked
////        val thisLevel = mock[NextLevel]("thisLevel")
////        val nextLevel = mock[NextLevel]("nextLevel")
////
////        ThrottleCompaction.putForward(
////          segments = Iterable.empty,
////          thisLevel = thisLevel,
////          nextLevel = nextLevel,
////          parallelMerge = randomParallelMerge()
////        ).right.right.value shouldBe IO.zero
////      }
////    }
////
////    "remove Segments" when {
////      "Segments from upper Level are merged into lower level" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            val thisLevel = mock[NextLevel]("thisLevel")
////            val nextLevel = mock[NextLevel]("nextLevel")
////
////            val keyValues = randomPutKeyValues(keyValueCount).groupedSlice(2)
////            val segments = Seq(GenSegment(keyValues(0)), GenSegment(keyValues(1)))
////
////            //next level should value a put for all the input Segments
////            (nextLevel.put(_: Iterable[Segment], _: ParallelMerge)(_: ExecutionContext)) expects(*, *, *) onCall {
////              (putSegments: Iterable[Segment], _: ParallelMerge, _: ExecutionContext) =>
////                putSegments.map(_.path) shouldBe segments.map(_.path)
////                implicit val nothingExceptionHandler = IO.ExceptionHandler.Nothing
////                IO.Right[Nothing, IO[Nothing, Set[Int]]](IO.Right[Nothing, Set[Int]](Set(Int.MaxValue)))
////            }
////
////            //segments value removed
////            (thisLevel.removeSegments(_: Iterable[Segment])) expects * onCall {
////              putSegments: Iterable[Segment] =>
////                putSegments.map(_.path) shouldBe segments.map(_.path)
////                IO.Right(segments.size)
////            }
////
////            ThrottleCompaction.putForward(
////              segments = segments,
////              thisLevel = thisLevel,
////              nextLevel = nextLevel,
////              parallelMerge = randomParallelMerge()
////            ).right.right.value.right.value shouldBe segments.size
////        }
////      }
////    }
////
////    "return success" when {
////      "it fails to remove Segments" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            val thisLevel = mock[NextLevel]("thisLevel")
////            val nextLevel = mock[NextLevel]("nextLevel")
////
////            val keyValues = randomPutKeyValues(keyValueCount).groupedSlice(2)
////            val segments = Seq(GenSegment(keyValues(0)), GenSegment(keyValues(1)))
////
////            //next level should value a put for all the input Segments
////            (nextLevel.put(_: Iterable[Segment], _: ParallelMerge)(_: ExecutionContext)) expects(*, *, *) onCall {
////              (putSegments: Iterable[Segment], _: ParallelMerge, _: ExecutionContext) =>
////                putSegments.map(_.path) shouldBe segments.map(_.path)
////                implicit val nothingExceptionHandler = IO.ExceptionHandler.Nothing
////                IO.Right[Nothing, IO[Nothing, Set[Int]]](IO.Right[Nothing, Set[Int]](Set(Int.MaxValue)))
////            }
////
////
////            //segments value removed
////            (thisLevel.removeSegments(_: Iterable[Segment])) expects * onCall {
////              putSegments: Iterable[Segment] =>
////                putSegments.map(_.path) shouldBe segments.map(_.path)
////                IO.failed("Failed!")
////            }
////
////            ThrottleCompaction.putForward(
////              segments = segments,
////              thisLevel = thisLevel,
////              nextLevel = nextLevel,
////              parallelMerge = randomParallelMerge()
////            ).right.right.value.right.value shouldBe segments.size
////        }
////      }
////    }
////  }
////
////  "copyForwardForEach" should {
////    "not copy" when {
////      "it's the last Level and is empty" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            ThrottleCompaction.copyForwardForEach(Slice(TestLevel())) shouldBe 0
////        }
////      }
////
////      "it's the last Level and is non empty" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            val keyValues = randomPutKeyValues(keyValueCount)
////            val level = TestLevel(keyValues = keyValues)
////            level.isEmpty shouldBe false
////
////            ThrottleCompaction.copyForwardForEach(
////              levels = level.reverseLevels.toSlice,
////              parallelMerge = randomParallelMerge()
////            ) shouldBe 0
////
////            if (persistent)
////              assertGet(keyValues, level.reopen)
////        }
////      }
////    }
////
////    "copy all Segments to last level" when {
////      "no Segments overlap" in {
////        runThis(1.times, log = true) {
////          CoreTestSweeper {
////            implicit sweeper =>
////              val allKeyValues = randomPutKeyValues(keyValueCount, startId = Some(1))
////              val keyValues = allKeyValues.groupedSlice(5)
////
////              val level5 = TestLevel(keyValues = keyValues(4), segmentConfig = SegmentBlockConfig.random(minSegmentSize = 1.kb, pushForward = PushForwardStrategy.On, mmap = mmapSegments))
////              val level4 = TestLevel(nextLevel = Some(level5), keyValues = keyValues(3), segmentConfig = SegmentBlockConfig.random(minSegmentSize = 10.bytes, pushForward = PushForwardStrategy.On, mmap = mmapSegments))
////              val level3 = TestLevel(nextLevel = Some(level4), keyValues = keyValues(2), segmentConfig = SegmentBlockConfig.random(minSegmentSize = 10.bytes, pushForward = PushForwardStrategy.On, mmap = mmapSegments))
////              val level2 = TestLevel(nextLevel = Some(level3), keyValues = keyValues(1), segmentConfig = SegmentBlockConfig.random(minSegmentSize = 10.bytes, pushForward = PushForwardStrategy.On, mmap = mmapSegments))
////              val level1 = TestLevel(nextLevel = Some(level2), keyValues = keyValues(0), segmentConfig = SegmentBlockConfig.random(minSegmentSize = 10.bytes, pushForward = PushForwardStrategy.On, mmap = mmapSegments))
////
////              //        level1.foreachLevel(_.segmentsCount() should be > 1)
////
////              val expectedCopiedSegments = level1.foldLeftLevels(0)(_ + _.segmentsCount()) - level5.segmentsCount()
////
////              val actualCopied =
////                ThrottleCompaction.copyForwardForEach(
////                  levels = level1.reverseLevels.toSlice,
////                  parallelMerge = randomParallelMerge()
////                )
////
////              actualCopied shouldBe expectedCopiedSegments
////              //all top levels shouldBe empty
////              level1.mapLevels(level => level).dropRight(1).foreach(_.isEmpty shouldBe true)
////
////              assertReads(allKeyValues, level1)
////              assertReads(allKeyValues, level2)
////              assertReads(allKeyValues, level3)
////              assertReads(allKeyValues, level4)
////              assertReads(allKeyValues, level5)
////
////              assertGet(allKeyValues, level1.reopen)
////          }
////        }
////      }
////    }
////
////    "copy Segments to last level" when {
////      "some Segments overlap" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            val allKeyValues = randomPutKeyValues(keyValueCount, addPutDeadlines = false, startId = Some(1))
////
////            val keyValues = allKeyValues.groupedSlice(5)
////
////            val level5 = TestLevel(keyValues = Slice(keyValues(3).last) ++ keyValues(4), segmentConfig = SegmentBlockConfig.random(minSegmentSize = 2.kb, pushForward = PushForwardStrategy.On, mmap = mmapSegments))
////            val level4 = TestLevel(nextLevel = Some(level5), keyValues = Slice(keyValues(2).last) ++ keyValues(3), segmentConfig = SegmentBlockConfig.random(minSegmentSize = 2.kb, pushForward = PushForwardStrategy.On, mmap = mmapSegments))
////            val level3 = TestLevel(nextLevel = Some(level4), keyValues = keyValues(2), segmentConfig = SegmentBlockConfig.random(minSegmentSize = 2.kb, pushForward = PushForwardStrategy.On, mmap = mmapSegments))
////            val level2 = TestLevel(nextLevel = Some(level3), keyValues = keyValues(1), segmentConfig = SegmentBlockConfig.random(minSegmentSize = 2.kb, pushForward = PushForwardStrategy.On, mmap = mmapSegments))
////            val level1 = TestLevel(nextLevel = Some(level2), keyValues = keyValues(0), segmentConfig = SegmentBlockConfig.random(minSegmentSize = 2.kb, pushForward = PushForwardStrategy.On, mmap = mmapSegments))
////
////            ThrottleCompaction.copyForwardForEach(
////              levels = level1.reverseLevels.toSlice,
////              parallelMerge = randomParallelMerge()
////            )
////
////            //top levels are level, second last level value all overlapping Segments, last Level gets the rest.
////            level1.isEmpty shouldBe true
////            level2.isEmpty shouldBe true
////            level3.isEmpty shouldBe true
////            level4.isEmpty shouldBe false
////            level5.isEmpty shouldBe false
////
////            assertReads(allKeyValues, level1)
////            assertGet(allKeyValues, level1.reopen)
////        }
////      }
////    }
////  }
////
////  "runLastLevelCompaction" should {
////    "not run compaction" when {
////      "level is not the last Level" in {
////        val level = mock[NextLevel]("level")
////        (level.hasNextLevel _).expects() returns true repeat 20.times
////
////        runThis(20.times) {
////          ThrottleCompaction.runLastLevelCompaction(
////            level = level,
////            checkExpired = randomBoolean(),
////            remainingCompactions = randomIntMax(10),
////            segmentsCompacted = 0,
////            parallelMerge = randomParallelMerge()
////          ) shouldBe IO.zero
////        }
////      }
////
////      "remaining compactions are 0" in {
////        val level = mock[NextLevel]("level")
////        (level.hasNextLevel _).expects() returns true repeat 20.times
////
////        runThis(20.times) {
////          ThrottleCompaction.runLastLevelCompaction(
////            level = level,
////            checkExpired = randomBoolean(),
////            remainingCompactions = randomIntMax(10),
////            segmentsCompacted = 10,
////            parallelMerge = randomParallelMerge()
////          ) shouldBe IO.Right(10)
////        }
////      }
////    }
////
////    "keep invoking refresh" when {
////      "remaining compactions are non zero" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            val segments: ListBuffer[Segment] =
////              (1 to 10).flatMap({
////                i =>
////                  if (i % 2 == 0)
////                    Some(
////                      GenSegment(
////                        Slice(
////                          Memory.put(i, i, Some(expiredDeadline())),
////                          Memory.put(i + 1, i + 1, Some(expiredDeadline()))
////                        )
////                      )
////                    )
////                  else
////                    None
////              }).to(ListBuffer)
////
////            val level = mock[NextLevel]("level")
////            (level.hasNextLevel _).expects() returns false repeat 6.times
////            (level.segments _).expects() returning segments repeat 5.times
////
////            (level.refresh(_: Segment)) expects * onCall {
////              segment: Segment =>
////                segments find (_.path == segment.path) shouldBe defined
////                segments -= segment
////                IO.Right(IO(segment.delete))(IO.ExceptionHandler.PromiseUnit)
////            } repeat 5.times
////
////            ThrottleCompaction.runLastLevelCompaction(
////              level = level,
////              checkExpired = true,
////              remainingCompactions = 5,
////              segmentsCompacted = 0,
////              parallelMerge = randomParallelMerge()
////            ) shouldBe IO.Right(5)
////        }
////      }
////    }
////
////    "invoke collapse" when {
////      "checkExpired is false" in {
////        CoreTestSweeper {
////          implicit sweeper =>
////            val segments: ListBuffer[Segment] =
////              (1 to 10).flatMap({
////                i =>
////                  if (i % 2 == 0)
////                    Some(
////                      GenSegment(
////                        Slice(
////                          Memory.put(i, i, Some(expiredDeadline())),
////                          Memory.put(i + 1, i + 1, Some(expiredDeadline()))
////                        )
////                      )
////                    )
////                  else
////                    None
////              }).to(ListBuffer)
////
////            val level = mock[NextLevel]("level")
////            (level.hasNextLevel _).expects() returns false repeated 2.times
////
////            (level.optimalSegmentsToCollapse _).expects(*) onCall {
////              count: Int =>
////                segments.take(count)
////            }
////
////            (level.collapse(_: Iterable[Segment], _: ParallelMerge)(_: ExecutionContext)) expects(*, *, *) onCall {
////              (segmentsToCollapse: Iterable[Segment], _: ParallelMerge, _: ExecutionContext) =>
////                segmentsToCollapse foreach (segment => segments find (_.path == segment.path) shouldBe defined)
////                segments --= segmentsToCollapse
////                IO.Right(IO(segmentsToCollapse.size))(IO.ExceptionHandler.PromiseUnit)
////            }
////
////            //        level.levelNumber _ expects() returns 1 repeat 3.times
////
////            ThrottleCompaction.runLastLevelCompaction(
////              level = level,
////              checkExpired = false,
////              remainingCompactions = 5,
////              segmentsCompacted = 0,
////              parallelMerge = randomParallelMerge()
////            ) shouldBe IO.Right(5)
////        }
////      }
////    }
////  }
////
////  //  "pushForward" when {
////  //    "NextLevel" should {
////  //      "copy segments first" in {
////  //        val segments: ListBuffer[Segment] =
////  //          (1 to 10).flatMap({
////  //            i =>
////  //              if (i % 2 == 0)
////  //                Some(
////  //                  GenSegment(
////  //                    Slice(
////  //                      Memory.put(i, i, Some(expiredDeadline())),
////  //                      Memory.put(i + 1, i + 1, Some(expiredDeadline()))
////  //                    ).toTransient
////  //                  ).get
////  //                )
////  //              else
////  //                None
////  //          })(collection.breakOut)
////  //
////  //        val lowerLevel = mock[NextLevel]("level")
////  //        lowerLevel.hasNextLevel _ expects() returns false repeated 2.times
////  //
////  //        val throttleFunction = mockFunction[LevelMeter, Throttle]("throttleFunction")
////  //
////  //        val upperLevel = TestLevel(nextLevel = Some(lowerLevel), throttle = throttleFunction)
////  //        upperLevel.put(segments).right.value.right.value
////  //
////  //        lowerLevel.optimalSegmentsToCollapse _ expects * onCall {
////  //          count: Int =>
////  //            segments.take(count)
////  //        }
////  //
////  //        (lowerLevel.collapse(_: Iterable[Segment])) expects(*, *) onCall {
////  //          (segmentsToCollapse: Iterable[Segment], _) =>
////  //            segmentsToCollapse foreach (segment => segments find (_.path == segment.path) shouldBe defined)
////  //            segments --= segmentsToCollapse
////  //            IO.Right(IO(segmentsToCollapse.size))(IO.ExceptionHandler.PromiseUnit)
////  //        }
////  //
////  //        //        level.levelNumber _ expects() returns 1 repeat 3.times
////  //
////  //        DefaultCompaction.pushForward(
////  //          lowerLevel = lowerLevel,
////  //          checkExpired = false,
////  //          remainingCompactions = 5,
////  //          segmentsCompacted = 0
////  //        ) shouldBe IO.Right(5)
////  //
////  //
////  //      }
////  //    }
////  //  }
////}
