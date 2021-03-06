/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.IO
import swaydb.IOValues._
import swaydb.core.TestData._
import swaydb.core._
import swaydb.core.data._
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.data.compaction.CompactionConfig.CompactionParallelism
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.testkit.RunThis._
import swaydb.utils.OperatingSystem
import swaydb.utils.StorageUnits._

import scala.concurrent.duration._

class LevelRefreshSpec0 extends LevelRefreshSpec

class LevelRefreshSpec1 extends LevelRefreshSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def level0MMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
  override def appendixStorageMMAP = MMAP.On(OperatingSystem.isWindows, forceSave = TestForceSave.mmap())
}

class LevelRefreshSpec2 extends LevelRefreshSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Off(forceSave = TestForceSave.channel())
  override def level0MMAP = MMAP.Off(forceSave = TestForceSave.channel())
  override def appendixStorageMMAP = MMAP.Off(forceSave = TestForceSave.channel())
}

class LevelRefreshSpec3 extends LevelRefreshSpec {
  override def inMemoryStorage = true
}

sealed trait LevelRefreshSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val ec = TestExecutionContext.executionContext
  implicit val compactionParallelism: CompactionParallelism = CompactionParallelism.availableProcessors()
  val keyValuesCount = 100

  "refresh" should {
    "remove expired key-values" in {
      runThis(5.times, log = true) {
        TestCaseSweeper {
          implicit sweeper =>
            import sweeper._

            val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.byte, mmap = mmapSegments))
            val keyValues = randomPutKeyValues(1000, valueSize = 0, startId = Some(0))(TestTimer.Empty)
            level.put(keyValues).runRandomIO.right.value
            //dispatch another put request so that existing Segment gets split
            level.put(Slice(keyValues.head)).runRandomIO.right.value
            level.segmentsCount() should be >= 1

            //expire all key-values
            level.put(Slice(Memory.Range(0, Int.MaxValue, Value.FromValue.Null, Value.Remove(Some(2.seconds.fromNow), Time.empty)))).runRandomIO.right.value
            level.segmentFilesInAppendix should be > 1

            sleep(3.seconds)

            //refresh multiple times if necessary to cover cases if throttle only allows
            //small number of segments to be refreshed at a time.
            var attempts = 20
            while (level.segmentFilesInAppendix != 0 && attempts > 0) {
              val segments = level.segments()
              val compact = level.refresh(segments, removeDeletedRecords = false).await
              level.commit(compact) shouldBe IO.unit
              attempts -= 1
            }

            if (isWindowsAndMMAPSegments())
              eventual(10.seconds) {
                sweeper.receiveAll()
                level.segmentFilesInAppendix shouldBe 0
              }
            else
              level.segmentFilesInAppendix shouldBe 0
        }
      }
    }

    "update createdInLevel" in {
      TestCaseSweeper {
        implicit sweeper =>
          import sweeper._

          val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb, mmap = mmapSegments))

          val keyValues = randomPutKeyValues(keyValuesCount, addExpiredPutDeadlines = false)
          val map = TestMap(keyValues)
          level.putMap(map).right.right.value

          val nextLevel = TestLevel()
          nextLevel.putSegments(level.segments()) shouldBe IO.unit

          nextLevel.segments() foreach {
            segment =>
              val compactResult = nextLevel.refresh(Seq(segment), removeDeletedRecords = false).await
              nextLevel.commit(compactResult) shouldBe IO.unit
          }
          nextLevel.segments() foreach (_.createdInLevel shouldBe nextLevel.levelNumber)
      }
    }
  }
}
