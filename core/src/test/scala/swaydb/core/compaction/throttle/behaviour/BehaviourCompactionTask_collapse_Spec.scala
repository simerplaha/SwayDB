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

package swaydb.core.compaction.throttle.behaviour

import swaydb.IO
import swaydb.config.MMAP
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core._
import swaydb.core.compaction.task.CompactionTask
import swaydb.core.segment.Segment
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.data.Memory
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._

import java.nio.file.FileAlreadyExistsException
import scala.concurrent.duration.Duration
import swaydb.testkit.TestKit._

class BehaviourCompactionTask_collapse_Spec0 extends BehaviourCompactionTask_collapse_Spec

class BehaviourCompactionTask_collapse_Spec1 extends BehaviourCompactionTask_collapse_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class BehaviourCompactionTask_collapse_Spec2 extends BehaviourCompactionTask_collapse_Spec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class BehaviourCompactionTask_collapse_Spec3 extends BehaviourCompactionTask_collapse_Spec {
  override def inMemoryStorage = true
}

sealed trait BehaviourCompactionTask_collapse_Spec extends TestBase {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
  implicit val ec = TestExecutionContext.executionContext

  "succeed" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel(segmentConfig = SegmentBlockConfig.random2(minSegmentSize = 1.byte, deleteDelay = Duration.Zero, mmap = mmapSegments))

          val segments =
            (1 to 10) map {
              key =>
                TestSegment(Slice(Memory.put(key)))
            }

          val keyValues = segments.iterator.flatMap(_.iterator(randomBoolean())).toSlice

          level.putSegments(segments) shouldBe IO.unit
          level.segmentsCount() shouldBe segments.size

          level.isEmpty shouldBe false
          assertReads(keyValues, level)

          if (persistent) {
            val levelReopen = level.reopen(segmentSize = Int.MaxValue)
            val task = CompactionTask.CollapseSegments(source = levelReopen, segments = levelReopen.segments())
            BehaviourCompactionTask.collapse(task, levelReopen).awaitInf
            levelReopen.segmentsCount() shouldBe 1

            val reopen = levelReopen.reopen
            assertReads(keyValues, reopen)
          }
      }
    }
  }

  "revert on failure" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel(segmentConfig = SegmentBlockConfig.random2(minSegmentSize = 1.byte, deleteDelay = Duration.Zero, mmap = mmapSegments))

          val segments =
            (1 to 10) map {
              key =>
                TestSegment(Slice(Memory.put(key)))
            }

          val keyValues = segments.iterator.flatMap(_.iterator(randomBoolean())).toSlice

          level.putSegments(segments) shouldBe IO.unit
          level.segmentsCount() shouldBe segments.size

          level.isEmpty shouldBe false
          assertReads(keyValues, level)

          if (persistent) {
            val levelReopen = level.reopen(segmentSize = Int.MaxValue)
            val task = CompactionTask.CollapseSegments(source = levelReopen, segments = levelReopen.segments())

            TestSegment(path = levelReopen.rootPath.resolve(s"${levelReopen.segmentIDGenerator.current + 1}.seg"))
            BehaviourCompactionTask.collapse(task, levelReopen).awaitFailureInf shouldBe a[FileAlreadyExistsException]

            level.segmentsCount() shouldBe segments.size

            val reopen = levelReopen.reopen
            assertReads(keyValues, reopen)
          }
      }
    }
  }
}
