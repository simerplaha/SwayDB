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
import swaydb.core.level.compaction.task.CompactionTask
import swaydb.core.segment.Segment
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.data.RunThis._
import swaydb.data.config.MMAP
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import java.nio.file.FileAlreadyExistsException
import scala.concurrent.duration.Duration

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

          val level = TestLevel(segmentConfig = SegmentBlock.Config.random2(minSegmentSize = 1.byte, deleteDelay = Duration.Zero, mmap = mmapSegments))

          val segments =
            (1 to 10) map {
              key =>
                TestSegment(Slice(Memory.put(key)))
            }

          val keyValues = segments.iterator.flatMap(_.iterator()).toSlice

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

          val level = TestLevel(segmentConfig = SegmentBlock.Config.random2(minSegmentSize = 1.byte, deleteDelay = Duration.Zero, mmap = mmapSegments))

          val segments =
            (1 to 10) map {
              key =>
                TestSegment(Slice(Memory.put(key)))
            }

          val keyValues = segments.iterator.flatMap(_.iterator()).toSlice

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
