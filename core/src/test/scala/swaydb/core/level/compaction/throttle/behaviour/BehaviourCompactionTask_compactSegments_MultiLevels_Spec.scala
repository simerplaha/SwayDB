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

package swaydb.core.level.compaction.throttle.behaviour

import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core._
import swaydb.core.data.Memory
import swaydb.core.level.compaction.task.CompactionTask
import swaydb.core.segment.Segment
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem

import scala.collection.SortedSet

class BehaviourCompactionTask_compactSegments_MultiLevels_Spec0 extends BehaviourCompactionTask_compactSegments_MultiLevels_Spec

class BehaviourCompactionTask_compactSegments_MultiLevels_Spec1 extends BehaviourCompactionTask_compactSegments_MultiLevels_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class BehaviourCompactionTask_compactSegments_MultiLevels_Spec2 extends BehaviourCompactionTask_compactSegments_MultiLevels_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class BehaviourCompactionTask_compactSegments_MultiLevels_Spec3 extends BehaviourCompactionTask_compactSegments_MultiLevels_Spec {
  override def inMemoryStorage = true
}

sealed trait BehaviourCompactionTask_compactSegments_MultiLevels_Spec extends TestBase {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
  implicit val ec = TestExecutionContext.executionContext
  implicit val compactionParallelism: CompactionParallelism = CompactionParallelism.availableProcessors()

  "compactSegments" when {
    "there are multi levels" should {
      "write data to lower level" when {
        "target Level is empty" in {
          TestCaseSweeper {
            implicit sweeper =>
              import sweeper._

              //LEVEL1 - [1], [2], [3], [4]
              //LOWER LEVELS ARE EMPTY - THE RESULT SHOULD BE FOLLOWING
              //LEVEL2 - [1]
              //LEVEL3 -      [2]
              //LEVEL4 -           [3]
              //LEVEL5 -                [4]
              val level1 = TestLevel()
              val level2 = TestLevel()
              val level3 = TestLevel()
              val level4 = TestLevel()
              val level5 = TestLevel()

              val segments =
                SortedSet(
                  TestSegment(Slice(Memory.put(1))),
                  TestSegment(Slice(Memory.put(2))),
                  TestSegment(Slice(Memory.put(3))),
                  TestSegment(Slice(Memory.put(4)))
                )

              val keyValues = segments.iterator.flatMap(_.iterator(randomBoolean())).toSlice

              level1.putSegments(segments) shouldBe IO.unit

              level1.isEmpty shouldBe false
              level2.isEmpty shouldBe true
              level3.isEmpty shouldBe true
              level4.isEmpty shouldBe true
              level5.isEmpty shouldBe true

              val task =
                CompactionTask.CompactSegments(
                  source = level1,
                  tasks = Seq(
                    CompactionTask.Task(level2, segments.take(1)),
                    CompactionTask.Task(level3, segments.drop(1).take(1)),
                    CompactionTask.Task(level4, segments.drop(2).take(1)),
                    CompactionTask.Task(level5, segments.drop(3).take(1))
                  )
                )

              BehaviourCompactionTask.compactSegments(task, level5).awaitInf shouldBe unit

              level1.isEmpty shouldBe true
              assertReads(keyValues.take(1), level2)
              assertReads(keyValues.drop(1).take(1), level3)
              assertReads(keyValues.drop(2).take(1), level4)
              assertReads(keyValues.drop(3).take(1), level5)

              if (persistent) {
                val reopenLevel1 = level1.reopen
                val reopenLevel2 = level2.reopen
                val reopenLevel3 = level3.reopen
                val reopenLevel4 = level4.reopen
                val reopenLevel5 = level5.reopen

                reopenLevel1.isEmpty shouldBe true
                level1.isEmpty shouldBe true
                assertReads(keyValues.take(1), reopenLevel2)
                assertReads(keyValues.drop(1).take(1), reopenLevel3)
                assertReads(keyValues.drop(2).take(1), reopenLevel4)
                assertReads(keyValues.drop(3).take(1), reopenLevel5)
              }
          }
        }

        "target Level is non empty" in {
          TestCaseSweeper {
            implicit sweeper =>
              import sweeper._

              //LEVEL1 - [1], [2], [3], [4]
              //LOWER LEVELS HAVE ZERO VALUE
              //LEVEL2 - [1]
              //LEVEL3 -      [2]
              //LEVEL4 -           [3]
              //LEVEL5 -                [4]
              val level1 = TestLevel()
              val level2 = TestLevel(keyValues = Slice(Memory.put(1, Slice.Null)))
              val level3 = TestLevel(keyValues = Slice(Memory.put(2, Slice.Null)))
              val level4 = TestLevel(keyValues = Slice(Memory.put(3, Slice.Null)))
              val level5 = TestLevel(keyValues = Slice(Memory.put(4, Slice.Null)))

              //level1 has updated key-values
              val newSegments =
                SortedSet(
                  TestSegment(Slice(Memory.put(1, 1))),
                  TestSegment(Slice(Memory.put(2, 2))),
                  TestSegment(Slice(Memory.put(3, 3))),
                  TestSegment(Slice(Memory.put(4, 4)))
                )

              val newKeyValues = newSegments.iterator.flatMap(_.iterator(randomBoolean())).toSlice

              newKeyValues.foreach(_.getOrFetchValue shouldBe defined)

              level1.putSegments(newSegments) shouldBe IO.unit

              level1.isEmpty shouldBe false
              level2.isEmpty shouldBe false
              level3.isEmpty shouldBe false
              level4.isEmpty shouldBe false
              level5.isEmpty shouldBe false

              val task =
                CompactionTask.CompactSegments(
                  source = level1,
                  tasks = Seq(
                    CompactionTask.Task(level2, newSegments.take(1)),
                    CompactionTask.Task(level3, newSegments.drop(1).take(1)),
                    CompactionTask.Task(level4, newSegments.drop(2).take(1)),
                    CompactionTask.Task(level5, newSegments.drop(3).take(1))
                  )
                )

              BehaviourCompactionTask.compactSegments(task, level5).awaitInf shouldBe unit

              level1.isEmpty shouldBe true
              assertReads(newKeyValues.take(1), level2)
              assertReads(newKeyValues.drop(1).take(1), level3)
              assertReads(newKeyValues.drop(2).take(1), level4)
              assertReads(newKeyValues.drop(3).take(1), level5)

              if (persistent) {
                val reopenLevel1 = level1.reopen
                val reopenLevel2 = level2.reopen
                val reopenLevel3 = level3.reopen
                val reopenLevel4 = level4.reopen
                val reopenLevel5 = level5.reopen

                reopenLevel1.isEmpty shouldBe true
                level1.isEmpty shouldBe true
                assertReads(newKeyValues.take(1), reopenLevel2)
                assertReads(newKeyValues.drop(1).take(1), reopenLevel3)
                assertReads(newKeyValues.drop(2).take(1), reopenLevel4)
                assertReads(newKeyValues.drop(3).take(1), reopenLevel5)
              }
          }
        }
      }
    }
  }
}
