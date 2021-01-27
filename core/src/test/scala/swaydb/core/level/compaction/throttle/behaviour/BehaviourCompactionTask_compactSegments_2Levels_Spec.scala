/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.level.compaction.throttle.behaviour

import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core._
import swaydb.core.data.Memory
import swaydb.core.level.Level
import swaydb.core.level.compaction.task.CompactionTask
import swaydb.core.segment.Segment
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.collection.SortedSet

class BehaviourCompactionTask_compactSegments_2Levels_Spec0 extends BehaviourCompactionTask_compactSegments_2Levels_Spec

class BehaviourCompactionTask_compactSegments_2Levels_Spec1 extends BehaviourCompactionTask_compactSegments_2Levels_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class BehaviourCompactionTask_compactSegments_2Levels_Spec2 extends BehaviourCompactionTask_compactSegments_2Levels_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class BehaviourCompactionTask_compactSegments_2Levels_Spec3 extends BehaviourCompactionTask_compactSegments_2Levels_Spec {
  override def inMemoryStorage = true
}

sealed trait BehaviourCompactionTask_compactSegments_2Levels_Spec extends TestBase {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
  implicit val ec = TestExecutionContext.executionContext

  "compactSegments" when {
    "there are 2 levels" should {
      "write data to lower level" when {
        "target Level is empty" in {
          TestCaseSweeper {
            implicit sweeper =>
              import sweeper._

              //LEVEL1 - [1], [2]
              //LEVEL2 - EMPTY
              val sourceLevel = TestLevel()
              val targetLevel = TestLevel()

              val segments = SortedSet(TestSegment(Slice(Memory.put(1))), TestSegment(Slice(Memory.put(2))))
              val keyValues = segments.iterator.flatMap(_.iterator()).toSlice

              sourceLevel.putSegments(segments) shouldBe IO.unit

              sourceLevel.isEmpty shouldBe false
              targetLevel.isEmpty shouldBe true

              assertReads(keyValues, sourceLevel)
              assertEmpty(keyValues, targetLevel)

              val task = CompactionTask.CompactSegments(sourceLevel, Seq(CompactionTask.Task(targetLevel, segments)))
              BehaviourCompactionTask.compactSegments(task, targetLevel).awaitInf shouldBe unit

              sourceLevel.isEmpty shouldBe true
              targetLevel.isEmpty shouldBe false

              assertEmpty(keyValues, sourceLevel)
              assertReads(keyValues, targetLevel)

              if (persistent) {
                val reopenSource = sourceLevel.reopen
                val reopenTarget = targetLevel.reopen

                assertEmpty(keyValues, reopenSource)
                assertReads(keyValues, reopenTarget)
              }
          }
        }

        "target Level is non empty" in {
          TestCaseSweeper {
            implicit sweeper =>
              import sweeper._

              //LEVEL1 - [1], [2]
              //LEVEL2 - {[1], [2]} || {1, 2}
              val sourceLevel = TestLevel()
              val targetLevel = TestLevel()

              val segments = SortedSet(TestSegment(Slice(Memory.put(1))), TestSegment(Slice(Memory.put(2))))
              val keyValues = segments.iterator.flatMap(_.iterator()).toSlice

              sourceLevel.putSegments(segments) shouldBe IO.unit
              //write data to Level as a single Segment or multiple Segments
              eitherOne(
                targetLevel.putSegments(segments) shouldBe IO.unit,
                targetLevel.put(keyValues.map(_.toMemory())) shouldBe IO.unit
              )

              sourceLevel.isEmpty shouldBe false
              targetLevel.isEmpty shouldBe false

              assertReads(keyValues, sourceLevel)
              assertReads(keyValues, targetLevel)

              val task = CompactionTask.CompactSegments(sourceLevel, Seq(CompactionTask.Task(targetLevel, segments)))
              BehaviourCompactionTask.compactSegments(task, targetLevel).awaitInf shouldBe unit

              sourceLevel.isEmpty shouldBe true
              targetLevel.isEmpty shouldBe false

              assertEmpty(keyValues, sourceLevel)
              assertReads(keyValues, targetLevel)

              if (persistent) {
                val reopenSource = sourceLevel.reopen
                val reopenTarget = targetLevel.reopen

                assertEmpty(keyValues, reopenSource)
                assertReads(keyValues, reopenTarget)
              }
          }
        }
      }

      "expire data" when {
        "target Level is empty" in {
          runThis(10.times, log = true) {
            TestCaseSweeper {
              implicit sweeper =>
                import sweeper._

                //LEVEL1 - [1], [2]
                //LEVEL2 - EMPTY
                val sourceLevel = TestLevel()
                val targetLevel = TestLevel()

                val segments =
                  SortedSet(
                    TestSegment(Slice(randomUpdateKeyValue(1), randomExpiredPutKeyValue(2))),
                    TestSegment(Slice(randomUpdateKeyValue(3)))
                  )

                val keyValues = segments.iterator.flatMap(_.iterator()).toSlice

                sourceLevel.putSegments(segments) shouldBe IO.unit

                sourceLevel.isEmpty shouldBe false
                targetLevel.isEmpty shouldBe true

                assertReads(keyValues, sourceLevel)
                assertEmpty(keyValues, targetLevel)

                val task = CompactionTask.CompactSegments(sourceLevel, Seq(CompactionTask.Task(targetLevel, segments)))
                BehaviourCompactionTask.compactSegments(task, targetLevel).awaitInf shouldBe unit

                sourceLevel.isEmpty shouldBe true
                targetLevel.isEmpty shouldBe true

                assertEmpty(keyValues, sourceLevel)
                assertEmpty(keyValues, targetLevel)

                if (persistent) {
                  val reopenSource = sourceLevel.reopen
                  val reopenTarget = targetLevel.reopen

                  assertEmpty(keyValues, reopenSource)
                  assertEmpty(keyValues, reopenTarget)
                }
            }
          }
        }

        "target Level is non empty" in {
          runThis(20.times, log = true) {
            TestCaseSweeper {
              implicit sweeper =>
                import sweeper._

                val sourceLevel = TestLevel()
                val targetLevel = TestLevel()

                //all source key-values are expired or update
                val sourceLevelSegment =
                  SortedSet(
                    TestSegment(Slice(randomUpdateKeyValue(1), randomExpiredPutKeyValue(2))),
                    TestSegment(Slice(randomUpdateKeyValue(3, deadline = randomExpiredDeadlineOption())))
                  )

                val sourceLevelKeyValues = sourceLevelSegment.iterator.flatMap(_.iterator()).toSlice

                //all target key-values are expired or update
                val targetLevelSegment =
                  SortedSet(
                    TestSegment(Slice(randomUpdateKeyValue(1), randomExpiredPutKeyValue(3))),
                    TestSegment(Slice(randomUpdateKeyValue(4), randomUpdateKeyValue(5)))
                  )

                val targetLevelKeyValues = targetLevelSegment.iterator.flatMap(_.iterator()).toSlice

                sourceLevel.putSegments(sourceLevelSegment) shouldBe IO.unit
                //write data to Level as a single Segment or multiple Segments
                eitherOne(
                  targetLevel.putSegments(targetLevelSegment) shouldBe IO.unit,
                  targetLevel.put(targetLevelKeyValues.map(_.toMemory())) shouldBe IO.unit
                )

                unexpiredPuts(sourceLevelKeyValues) should have size 0
                unexpiredPuts(targetLevelKeyValues) should have size 0

                sourceLevel.isEmpty shouldBe false
                targetLevel.isEmpty shouldBe false

                assertReads(sourceLevelKeyValues, sourceLevel)
                assertReads(targetLevelKeyValues, targetLevel)

                val task = CompactionTask.CompactSegments(sourceLevel, Seq(CompactionTask.Task(targetLevel, sourceLevelSegment)))
                BehaviourCompactionTask.compactSegments(task, targetLevel).awaitInf shouldBe unit

                sourceLevel.isEmpty shouldBe true
                targetLevel.isEmpty shouldBe true

                if (persistent) {
                  val reopenSource = sourceLevel.reopen
                  val reopenTarget = targetLevel.reopen

                  reopenSource.isEmpty shouldBe true
                  reopenTarget.isEmpty shouldBe true
                }
            }
          }
        }
      }

      "delete uncommitted Segment" when {
        "failure" in {
          runThis(5.times, log = true) {
            TestCaseSweeper {
              implicit sweeper =>
                import sweeper._

                //LEVEL1 - [1], [2]
                //LEVEL2 - EMPTY
                val sourceLevel = TestLevel()
                val targetLevel = TestLevel()

                val segments =
                  SortedSet(
                    TestSegment(Slice(Memory.put(1))),
                    TestSegment(Slice(Memory.put(2)))
                  )

                val keyValues = segments.iterator.flatMap(_.iterator()).toSlice

                sourceLevel.putSegments(segments) shouldBe IO.unit

                //assert that nothing changes in the Level.
                def assertNoChange(sourceLevel: Level, targetLevel: Level) = {
                  sourceLevel.isEmpty shouldBe false
                  targetLevel.isEmpty shouldBe true

                  if (persistent) {
                    targetLevel.segmentFilesOnDisk shouldBe empty
                    segments.foreach(_.existsOnDiskOrMemory shouldBe true)
                  } else {
                    //for memory we delete the last Segment so expect others to exist
                    segments.dropRight(1).foreach(_.existsOnDiskOrMemory shouldBe true)
                  }

                  assertReads(keyValues, sourceLevel)
                  assertEmpty(keyValues, targetLevel)
                }

                assertNoChange(sourceLevel, targetLevel)

                val task = CompactionTask.CompactSegments(sourceLevel, Seq(CompactionTask.Task(targetLevel, segments)))

                val existingSegment =
                  if (memory) { //if it's a memory test delete the last Segment to test failure
                    val segment = segments.last
                    segment.delete
                    segment
                  } else {
                    //for persistent expect
                    TestSegment(path = targetLevel.rootPath.resolve(s"${targetLevel.segmentIDGenerator.current + 1}.seg"))
                  }

                //Create a Segment file in the Level's folder giving it the Level's next SegmentId so that it fails
                //compaction because the fileName is already taken
                BehaviourCompactionTask.compactSegments(task, targetLevel).awaitFailureInf shouldBe a[Exception]
                if (persistent) existingSegment.delete //delete the Segment so that assertion can assert for empty Level

                //state remains unchanged
                assertNoChange(sourceLevel, targetLevel)

                if(persistent) {
                  val reopenSource = sourceLevel.reopen
                  val reopenTarget = targetLevel.reopen
                  assertNoChange(reopenSource, reopenTarget)
                }
            }
          }
        }
      }
    }
  }
}
