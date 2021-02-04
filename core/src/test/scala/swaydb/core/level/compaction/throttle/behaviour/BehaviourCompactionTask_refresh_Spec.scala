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
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._

class BehaviourCompactionTask_refresh_Spec0 extends BehaviourCompactionTask_refresh_Spec

//class BehaviourCompactionTask_refresh_Spec1 extends BehaviourCompactionTask_refresh_Spec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
//  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
//}
//
//class BehaviourCompactionTask_refresh_Spec2 extends BehaviourCompactionTask_refresh_Spec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
//  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
//  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
//}

class BehaviourCompactionTask_refresh_Spec3 extends BehaviourCompactionTask_refresh_Spec {
  override def inMemoryStorage = true
}

sealed trait BehaviourCompactionTask_refresh_Spec extends TestBase {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
  implicit val ec = TestExecutionContext.executionContext
  implicit val compactionParallelism: CompactionParallelism = CompactionParallelism.availableProcessors()

  "succeed" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel()

          val segments =
            (1 to 10) map {
              key =>
                TestSegment(Slice(Memory.update(key)))
            }

          val keyValues = segments.iterator.flatMap(_.iterator(randomBoolean())).toSlice

          level.putSegments(segments) shouldBe IO.unit

          level.isEmpty shouldBe false
          assertReads(keyValues, level)

          val task = CompactionTask.RefreshSegments(source = level, segments = level.segments())
          BehaviourCompactionTask.refresh(task, level).awaitInf shouldBe unit
          level.isEmpty shouldBe true

          if (persistent) {
            val reopen = level.reopen
            reopen.isEmpty shouldBe true
          }
      }
    }
  }

  "revert on failure" in {
    runThis(10.times, log = true) {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel()

          val segments =
            (1 to 10) map {
              key =>
                if (key == 2)
                  TestSegment(Slice(Memory.put(key)))
                else
                  TestSegment(Slice(Memory.update(key)))
            }

          val keyValues = segments.iterator.flatMap(_.iterator(randomBoolean())).toSlice

          level.putSegments(segments) shouldBe IO.unit

          val segmentPathsBeforeRefresh = level.segments().map(_.path)

          level.isEmpty shouldBe false
          assertReads(keyValues, level)

          val task = CompactionTask.RefreshSegments(source = level, segments = level.segments())

          if (memory)
            level.segments().last.delete
          else
            TestSegment(path = level.rootPath.resolve(s"${level.segmentIDGenerator.current + 1}.seg"))

          BehaviourCompactionTask.refresh(task, level).awaitFailureInf shouldBe a[Exception]

          level.segments().map(_.path) shouldBe segmentPathsBeforeRefresh

          if (persistent) {
            val reopen = level.reopen
            assertReads(keyValues, reopen)
            reopen.segments().map(_.path) shouldBe segmentPathsBeforeRefresh
            reopen.segments().flatMap(_.iterator(randomBoolean())) shouldBe segments.flatMap(_.iterator(randomBoolean()))
          }
      }
    }
  }
}
