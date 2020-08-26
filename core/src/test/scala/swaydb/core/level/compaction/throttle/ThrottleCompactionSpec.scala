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

package swaydb.core.level.compaction.throttle

import org.scalamock.scalatest.MockFactory
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.data.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.Memory
import swaydb.core.level.NextLevel
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.{TestBase, TestCaseSweeper, TestExecutionContext, TestSweeper, TestTimer}
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.mutable.ListBuffer
import scala.collection.compat._

class CompactionSpec0 extends CompactionSpec

class CompactionSpec1 extends CompactionSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Enabled(OperatingSystem.isWindows)
  override def level0MMAP = MMAP.Enabled(OperatingSystem.isWindows)
  override def appendixStorageMMAP = MMAP.Enabled(OperatingSystem.isWindows)
}

class CompactionSpec2 extends CompactionSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Disabled
  override def level0MMAP = MMAP.Disabled
  override def appendixStorageMMAP = MMAP.Disabled
}

class CompactionSpec3 extends CompactionSpec {
  override def inMemoryStorage = true
}

sealed trait CompactionSpec extends TestBase with MockFactory {

  val keyValueCount = 1000

  implicit val ec = TestExecutionContext.executionContext
  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val timer = TestTimer.Empty

  "putForward" should {
    "return zero" when {
      "input Segments are empty" in {
        //levels are never invoked
        val thisLevel = mock[NextLevel]("thisLevel")
        val nextLevel = mock[NextLevel]("nextLevel")

        ThrottleCompaction.putForward(Iterable.empty, thisLevel, nextLevel).right.right.value shouldBe IO.zero
      }
    }

    "remove Segments" when {
      "Segments from upper Level are merged into lower level" in {
        TestCaseSweeper {
          implicit sweeper =>
            val thisLevel = mock[NextLevel]("thisLevel")
            val nextLevel = mock[NextLevel]("nextLevel")

            val keyValues = randomPutKeyValues(keyValueCount).groupedSlice(2)
            val segments = Seq(TestSegment(keyValues(0)), TestSegment(keyValues(1)))

            //next level should value a put for all the input Segments
            (nextLevel.put(_: Iterable[Segment])) expects * onCall {
              putSegments: Iterable[Segment] =>
                putSegments.map(_.path) shouldBe segments.map(_.path)
                implicit val nothingExceptionHandler = IO.ExceptionHandler.Nothing
                IO.Right[Nothing, IO[Nothing, Set[Int]]](IO.Right[Nothing, Set[Int]](Set(Int.MaxValue)))
            }

            //segments value removed
            (thisLevel.removeSegments(_: Iterable[Segment])) expects * onCall {
              putSegments: Iterable[Segment] =>
                putSegments.map(_.path) shouldBe segments.map(_.path)
                IO.Right(segments.size)
            }

            ThrottleCompaction.putForward(segments, thisLevel, nextLevel).right.right.value.right.value shouldBe segments.size
        }
      }
    }

    "return success" when {
      "it fails to remove Segments" in {
        TestCaseSweeper {
          implicit sweeper =>
            val thisLevel = mock[NextLevel]("thisLevel")
            val nextLevel = mock[NextLevel]("nextLevel")

            val keyValues = randomPutKeyValues(keyValueCount).groupedSlice(2)
            val segments = Seq(TestSegment(keyValues(0)), TestSegment(keyValues(1)))

            //next level should value a put for all the input Segments
            (nextLevel.put(_: Iterable[Segment])) expects * onCall {
              putSegments: Iterable[Segment] =>
                putSegments.map(_.path) shouldBe segments.map(_.path)
                implicit val nothingExceptionHandler = IO.ExceptionHandler.Nothing
                IO.Right[Nothing, IO[Nothing, Set[Int]]](IO.Right[Nothing, Set[Int]](Set(Int.MaxValue)))
            }

            //segments value removed
            (thisLevel.removeSegments(_: Iterable[Segment])) expects * onCall {
              putSegments: Iterable[Segment] =>
                putSegments.map(_.path) shouldBe segments.map(_.path)
                IO.failed("Failed!")
            }

            ThrottleCompaction.putForward(segments, thisLevel, nextLevel).right.right.value.right.value shouldBe segments.size
        }
      }
    }
  }

  "copyForwardForEach" should {
    "not copy" when {
      "it's the last Level and is empty" in {
        TestCaseSweeper {
          implicit sweeper =>
            ThrottleCompaction.copyForwardForEach(Slice(TestLevel())) shouldBe 0
        }
      }

      "it's the last Level and is non empty" in {
        TestCaseSweeper {
          implicit sweeper =>
            val keyValues = randomPutKeyValues(keyValueCount)
            val level = TestLevel(keyValues = keyValues)
            level.isEmpty shouldBe false
            ThrottleCompaction.copyForwardForEach(level.reverseLevels.toSlice) shouldBe 0
            if (persistent)
              assertGet(keyValues, level.reopen)
        }
      }
    }

    "copy all Segments to last level" when {
      "no Segments overlap" in {
        /**
         * @note Copying memory-mapped files on Windows is slow.
         */
        if (OperatingSystem.isWindows && mmapSegments.hasMMAP)
          cancel()
        else
          runThis(1.times, log = true) {
            TestCaseSweeper {
              implicit sweeper =>
                val allKeyValues = randomPutKeyValues(keyValueCount, startId = Some(1))
                val keyValues = allKeyValues.groupedSlice(5)

                val level5 = TestLevel(keyValues = keyValues(4), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, pushForward = true, mmap = mmapSegments))
                val level4 = TestLevel(nextLevel = Some(level5), keyValues = keyValues(3), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 10.bytes, pushForward = true, mmap = mmapSegments))
                val level3 = TestLevel(nextLevel = Some(level4), keyValues = keyValues(2), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 10.bytes, pushForward = true, mmap = mmapSegments))
                val level2 = TestLevel(nextLevel = Some(level3), keyValues = keyValues(1), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 10.bytes, pushForward = true, mmap = mmapSegments))
                val level1 = TestLevel(nextLevel = Some(level2), keyValues = keyValues(0), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 10.bytes, pushForward = true, mmap = mmapSegments))

                //        level1.foreachLevel(_.segmentsCount() should be > 1)

                val expectedCopiedSegments = level1.foldLeftLevels(0)(_ + _.segmentsCount()) - level5.segmentsCount()
                val actualCopied = ThrottleCompaction.copyForwardForEach(level1.reverseLevels.toSlice)
                actualCopied shouldBe expectedCopiedSegments
                //all top levels shouldBe empty
                level1.mapLevels(level => level).dropRight(1).foreach(_.isEmpty shouldBe true)

                assertReads(allKeyValues, level1)
                assertReads(allKeyValues, level2)
                assertReads(allKeyValues, level3)
                assertReads(allKeyValues, level4)
                assertReads(allKeyValues, level5)

                assertGet(allKeyValues, level1.reopen)
            }
          }
      }
    }

    "copy Segments to last level" when {
      "some Segments overlap" in {
        if (OperatingSystem.isWindows && mmapSegments.hasMMAP)
          cancel()
        else
          TestCaseSweeper {
            implicit sweeper =>
              val allKeyValues = randomPutKeyValues(keyValueCount, addPutDeadlines = false, startId = Some(1))

              val keyValues = allKeyValues.groupedSlice(5)

              val level5 = TestLevel(keyValues = Slice(keyValues(3).last) ++ keyValues(4), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 2.kb, pushForward = true, mmap = mmapSegments))
              val level4 = TestLevel(nextLevel = Some(level5), keyValues = Slice(keyValues(2).last) ++ keyValues(3), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 2.kb, pushForward = true, mmap = mmapSegments))
              val level3 = TestLevel(nextLevel = Some(level4), keyValues = keyValues(2), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 2.kb, pushForward = true, mmap = mmapSegments))
              val level2 = TestLevel(nextLevel = Some(level3), keyValues = keyValues(1), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 2.kb, pushForward = true, mmap = mmapSegments))
              val level1 = TestLevel(nextLevel = Some(level2), keyValues = keyValues(0), segmentConfig = SegmentBlock.Config.random(minSegmentSize = 2.kb, pushForward = true, mmap = mmapSegments))

              ThrottleCompaction.copyForwardForEach(level1.reverseLevels.toSlice)

              //top levels are level, second last level value all overlapping Segments, last Level gets the rest.
              level1.isEmpty shouldBe true
              level2.isEmpty shouldBe true
              level3.isEmpty shouldBe true
              level4.isEmpty shouldBe false
              level5.isEmpty shouldBe false

              assertReads(allKeyValues, level1)
              assertGet(allKeyValues, level1.reopen)
          }
      }
    }
  }

  "runLastLevelCompaction" should {
    "not run compaction" when {
      "level is not the last Level" in {
        val level = mock[NextLevel]("level")
        (level.hasNextLevel _).expects() returns true repeat 20.times

        runThis(20.times) {
          ThrottleCompaction.runLastLevelCompaction(
            level = level,
            checkExpired = randomBoolean(),
            remainingCompactions = randomIntMax(10),
            segmentsCompacted = 0
          ) shouldBe IO.zero
        }
      }

      "remaining compactions are 0" in {
        val level = mock[NextLevel]("level")
        (level.hasNextLevel _).expects() returns true repeat 20.times

        runThis(20.times) {
          ThrottleCompaction.runLastLevelCompaction(
            level = level,
            checkExpired = randomBoolean(),
            remainingCompactions = randomIntMax(10),
            segmentsCompacted = 10
          ) shouldBe IO.Right(10)
        }
      }
    }

    "keep invoking refresh" when {
      "remaining compactions are non zero" in {
        TestCaseSweeper {
          implicit sweeper =>
            val segments: ListBuffer[Segment] =
              (1 to 10).flatMap({
                i =>
                  if (i % 2 == 0)
                    Some(
                      TestSegment(
                        Slice(
                          Memory.put(i, i, Some(expiredDeadline())),
                          Memory.put(i + 1, i + 1, Some(expiredDeadline()))
                        )
                      )
                    )
                  else
                    None
              }).to(ListBuffer)

            val level = mock[NextLevel]("level")
            (level.hasNextLevel _).expects() returns false repeat 6.times
            (level.segmentsInLevel _).expects() returning segments repeat 5.times

            (level.refresh(_: Segment)) expects * onCall {
              segment: Segment =>
                segments find (_.path == segment.path) shouldBe defined
                segments -= segment
                IO.Right(IO(segment.delete))(IO.ExceptionHandler.PromiseUnit)
            } repeat 5.times

            ThrottleCompaction.runLastLevelCompaction(
              level = level,
              checkExpired = true,
              remainingCompactions = 5,
              segmentsCompacted = 0
            ) shouldBe IO.Right(5)
        }
      }
    }

    "invoke collapse" when {
      "checkExpired is false" in {
        TestCaseSweeper {
          implicit sweeper =>
            val segments: ListBuffer[Segment] =
              (1 to 10).flatMap({
                i =>
                  if (i % 2 == 0)
                    Some(
                      TestSegment(
                        Slice(
                          Memory.put(i, i, Some(expiredDeadline())),
                          Memory.put(i + 1, i + 1, Some(expiredDeadline()))
                        )
                      )
                    )
                  else
                    None
              }).to(ListBuffer)

            val level = mock[NextLevel]("level")
            (level.hasNextLevel _).expects() returns false repeated 2.times

            (level.optimalSegmentsToCollapse _).expects(*) onCall {
              count: Int =>
                segments.take(count)
            }

            (level.collapse(_: Iterable[Segment])) expects * onCall {
              segmentsToCollapse: Iterable[Segment] =>
                segmentsToCollapse foreach (segment => segments find (_.path == segment.path) shouldBe defined)
                segments --= segmentsToCollapse
                IO.Right(IO(segmentsToCollapse.size))(IO.ExceptionHandler.PromiseUnit)
            }

            //        level.levelNumber _ expects() returns 1 repeat 3.times

            ThrottleCompaction.runLastLevelCompaction(
              level = level,
              checkExpired = false,
              remainingCompactions = 5,
              segmentsCompacted = 0
            ) shouldBe IO.Right(5)
        }
      }
    }
  }

  //  "pushForward" when {
  //    "NextLevel" should {
  //      "copy segments first" in {
  //        val segments: ListBuffer[Segment] =
  //          (1 to 10).flatMap({
  //            i =>
  //              if (i % 2 == 0)
  //                Some(
  //                  TestSegment(
  //                    Slice(
  //                      Memory.put(i, i, Some(expiredDeadline())),
  //                      Memory.put(i + 1, i + 1, Some(expiredDeadline()))
  //                    ).toTransient
  //                  ).get
  //                )
  //              else
  //                None
  //          })(collection.breakOut)
  //
  //        val lowerLevel = mock[NextLevel]("level")
  //        lowerLevel.hasNextLevel _ expects() returns false repeated 2.times
  //
  //        val throttleFunction = mockFunction[LevelMeter, Throttle]("throttleFunction")
  //
  //        val upperLevel = TestLevel(nextLevel = Some(lowerLevel), throttle = throttleFunction)
  //        upperLevel.put(segments).right.value.right.value
  //
  //        lowerLevel.optimalSegmentsToCollapse _ expects * onCall {
  //          count: Int =>
  //            segments.take(count)
  //        }
  //
  //        (lowerLevel.collapse(_: Iterable[Segment])) expects(*, *) onCall {
  //          (segmentsToCollapse: Iterable[Segment], _) =>
  //            segmentsToCollapse foreach (segment => segments find (_.path == segment.path) shouldBe defined)
  //            segments --= segmentsToCollapse
  //            IO.Right(IO(segmentsToCollapse.size))(IO.ExceptionHandler.PromiseUnit)
  //        }
  //
  //        //        level.levelNumber _ expects() returns 1 repeat 3.times
  //
  //        DefaultCompaction.pushForward(
  //          lowerLevel = lowerLevel,
  //          checkExpired = false,
  //          remainingCompactions = 5,
  //          segmentsCompacted = 0
  //        ) shouldBe IO.Right(5)
  //
  //
  //      }
  //    }
  //  }
}
