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
import swaydb.core.CommonAssertions._
import swaydb.IOValues._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.level.zero.LevelZeroSkipListMerger
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class LevelRemoveSegmentSpec0 extends LevelRemoveSegmentSpec

class LevelRemoveSegmentSpec1 extends LevelRemoveSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class LevelRemoveSegmentSpec2 extends LevelRemoveSegmentSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class LevelRemoveSegmentSpec3 extends LevelRemoveSegmentSpec {
  override def inMemoryStorage = true
}

sealed trait LevelRemoveSegmentSpec extends TestBase with MockFactory with PrivateMethodTester {

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

  "removeSegments" should {
    "remove segments from disk and remove them from appendix" in {
      val level = TestLevel(segmentSize = 1.kb)
      level.putKeyValuesTest(randomPutKeyValues(keyValuesCount)).valueIOGet

      level.removeSegments(level.segmentsInLevel()).valueIOGet

      level.isEmpty shouldBe true

      if (persistent) {
        level.segmentFilesOnDisk shouldBe empty
        level.reopen.isEmpty shouldBe true
      }
    }
  }
}
