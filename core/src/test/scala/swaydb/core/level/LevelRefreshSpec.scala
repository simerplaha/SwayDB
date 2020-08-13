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

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data._
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.{TestBase, TestSweeper, TestTimer}
import swaydb.data.config.MMAP
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class LevelRefreshSpec0 extends LevelRefreshSpec

class LevelRefreshSpec1 extends LevelRefreshSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Enabled(OperatingSystem.isWindows)
  override def level0MMAP = MMAP.Enabled(OperatingSystem.isWindows)
  override def appendixStorageMMAP = MMAP.Enabled(OperatingSystem.isWindows)
}

class LevelRefreshSpec2 extends LevelRefreshSpec {
  override def levelFoldersCount = 10
  override def mmapSegments = MMAP.Disabled
  override def level0MMAP = MMAP.Disabled
  override def appendixStorageMMAP = MMAP.Disabled
}

class LevelRefreshSpec3 extends LevelRefreshSpec {
  override def inMemoryStorage = true
}

sealed trait LevelRefreshSpec extends TestBase with MockFactory with PrivateMethodTester {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val testTimer: TestTimer = TestTimer.Empty
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  val keyValuesCount = 100

  //  override def deleteFiles: Boolean =
  //    false

  implicit val maxOpenSegmentsCacheImplicitLimiter: FileSweeperActor = TestSweeper.fileSweeper
  implicit val memorySweeperImplicitSweeper: Option[MemorySweeper.All] = TestSweeper.memorySweeperMax
  implicit val skipListMerger = LevelZeroSkipListMerger

  "refresh" should {
    "remove expired key-values" in {
      val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.byte))
      val keyValues = randomPutKeyValues(1000, valueSize = 0, startId = Some(0))(TestTimer.Empty)
      level.putKeyValuesTest(keyValues).runRandomIO.right.value
      //dispatch another put request so that existing Segment gets split
      level.putKeyValuesTest(Slice(keyValues.head)).runRandomIO.right.value
      level.segmentsCount() should be >= 1

      //expire all key-values
      level.putKeyValuesTest(Slice(Memory.Range(0, Int.MaxValue, Value.FromValue.Null, Value.Remove(Some(2.seconds.fromNow), Time.empty)))).runRandomIO.right.value
      level.segmentFilesInAppendix should be > 1

      sleep(3.seconds)
      level.segmentsInLevel() foreach {
        segment =>
          level.refresh(segment).right.right.value
      }

      level.segmentFilesInAppendix shouldBe 0
    }

    "update createdInLevel" in {
      val level = TestLevel(segmentConfig = SegmentBlock.Config.random(minSegmentSize = 1.kb))

      val keyValues = randomPutKeyValues(keyValuesCount, addExpiredPutDeadlines = false)
      val maps = TestMap(keyValues)
      level.put(maps).right.right.value

      val nextLevel = TestLevel()
      nextLevel.put(level.segmentsInLevel()).right.right.value

      if (persistent)
        nextLevel.segmentsInLevel() foreach (_.createdInLevel shouldBe level.levelNumber)
      nextLevel.segmentsInLevel() foreach (segment => nextLevel.refresh(segment).right.right.value)
      nextLevel.segmentsInLevel() foreach (_.createdInLevel shouldBe nextLevel.levelNumber)
    }
  }
}
