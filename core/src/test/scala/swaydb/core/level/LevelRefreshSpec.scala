/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core.level

import org.scalamock.scalatest.MockFactory
import org.scalatest.PrivateMethodTester
import swaydb.Error.Segment.ErrorHandler
import swaydb.core.CommonAssertions._
import swaydb.core.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

import scala.concurrent.duration._

class LevelRefreshSpec0 extends LevelRefreshSpec

class LevelRefreshSpec1 extends LevelRefreshSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelRefreshSpec2 extends LevelRefreshSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
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

  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter
  implicit val groupingStrategy: Option[KeyValueGroupingStrategyInternal] = randomGroupingStrategyOption(keyValuesCount)
  implicit val skipListMerger = LevelZeroSkipListMerger

  "refresh" should {
    "remove expired key-values" in {
      val level = TestLevel(segmentSize = 1.kb)
      val keyValues = randomPutKeyValues(1000, valueSize = 0, startId = Some(0))(TestTimer.Empty)
      level.putKeyValuesTest(keyValues).runIO
      //dispatch another put request so that existing Segment gets split
      level.putKeyValuesTest(Slice(keyValues.head)).runIO
      level.segmentsCount() should be > 1

      //expire all key-values
      level.putKeyValuesTest(Slice(Memory.Range(0, Int.MaxValue, None, Value.Remove(Some(2.seconds.fromNow), Time.empty)))).runIO
      level.segmentFilesInAppendix should be > 1

      sleep(3.seconds)
      level.segmentsInLevel() foreach {
        segment =>
          level.refresh(segment).runIO
      }

      level.segmentFilesInAppendix shouldBe 0
    }

    "update createdInLevel" in {
      val level = TestLevel(segmentSize = 1.kb)

      val keyValues = randomPutKeyValues(keyValuesCount, addExpiredPutDeadlines = false)
      val maps = TestMap(keyValues.toTransient.toMemoryResponse)
      level.put(maps).runIO

      val nextLevel = TestLevel()
      nextLevel.put(level.segmentsInLevel()).runIO

      nextLevel.segmentsInLevel() foreach (_.createdInLevel.runIO shouldBe level.levelNumber)
      nextLevel.segmentsInLevel() foreach (segment => nextLevel.refresh(segment).runIO)
      nextLevel.segmentsInLevel() foreach (_.createdInLevel.runIO shouldBe nextLevel.levelNumber)
    }
  }
}
