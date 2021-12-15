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
import swaydb.core.CommonAssertions._
import swaydb.core.CoreTestData._
import swaydb.core._
import swaydb.core.compaction.task.CompactionTask
import swaydb.core.level.ALevelSpec
import swaydb.core.segment.Segment
import swaydb.core.segment.data.Memory
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._

class BehaviourCompactionTask_refresh_Spec0 extends BehaviourCompactionTask_refresh_Spec

//class BehaviourCompactionTask_refresh_Spec1 extends BehaviourCompactionTask_refresh_Spec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def level0MMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows(), forceSave = TestForceSave.mmap())
//}
//
//class BehaviourCompactionTask_refresh_Spec2 extends BehaviourCompactionTask_refresh_Spec {
//  override def levelFoldersCount = 10
//  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
//  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
//  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
//}

class BehaviourCompactionTask_refresh_Spec3 extends BehaviourCompactionTask_refresh_Spec {
  override def isMemorySpec = true
}

sealed trait BehaviourCompactionTask_refresh_Spec extends ALevelSpec {

  implicit val timer = TestTimer.Empty
  implicit val keyOrder = KeyOrder.default
  implicit val segmentOrdering = keyOrder.on[Segment](_.minKey)
  implicit val ec = TestExecutionContext.executionContext

  "succeed" in {
    runThis(10.times, log = true) {
      TestSweeper {
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

          if (isPersistentSpec) {
            val reopen = level.reopen
            reopen.isEmpty shouldBe true
          }
      }
    }
  }

  "revert on failure" in {
    runThis(10.times, log = true) {
      TestSweeper {
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

          if (isMemorySpec)
            level.segments().last.delete()
          else
            TestSegment(path = level.rootPath.resolve(s"${level.segmentIDGenerator.current + 1}.seg"))

          BehaviourCompactionTask.refresh(task, level).awaitFailureInf shouldBe a[Exception]

          level.segments().map(_.path) shouldBe segmentPathsBeforeRefresh

          if (isPersistentSpec) {
            val reopen = level.reopen
            assertReads(keyValues, reopen)
            reopen.segments().map(_.path) shouldBe segmentPathsBeforeRefresh
            reopen.segments().flatMap(_.iterator(randomBoolean())) shouldBe segments.flatMap(_.iterator(randomBoolean()))
          }
      }
    }
  }
}
