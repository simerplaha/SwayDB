/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment

import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.core.data.{KeyValueReadOnly, PersistentReadOnly}
import swaydb.core.io.file.DBFile
import swaydb.core.util.Benchmark
import swaydb.data.storage.{AppendixStorage, Level0Storage, LevelStorage}
import swaydb.order.KeyOrder
import swaydb.data.util.StorageUnits._

//@formatter:off
class SegmentPerformanceSpec1 extends SegmentPerformanceSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentPerformanceSpec2 extends SegmentPerformanceSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentPerformanceSpec3 extends SegmentPerformanceSpec {
  override def inMemoryStorage = true
}


class SegmentPerformanceSpec extends TestBase with Benchmark {

  implicit val ordering = KeyOrder.default
  val keyValuesCount = 100


  implicit val maxSegmentsOpenCacheImplicitLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: (PersistentReadOnly, Segment) => Unit = TestLimitQueues.keyValueLimiter

  val keyValues = randomIntKeyValues(keyValuesCount)

  def assertGet(segment: Segment) =
    keyValues.foreach(keyValue => segment.get(keyValue.key).assertGetOpt shouldBe Some(keyValue))

  def assertHigher(segment: Segment) =
    (0 until keyValues.size - 1) foreach {
      index =>
        //        segment.higherKey(keyValues(index).key)
        //        println(s"index: $index")
        val keyValue = keyValues(index)
        val expectedHigher = keyValues(index + 1)
        val higher = segment.higher(keyValue.key).assertGet
        higher.key shouldBe expectedHigher.key
        higher.getOrFetchValue.assertGetOpt shouldBe expectedHigher.getOrFetchValue.assertGetOpt
        higher.isDelete shouldBe expectedHigher.isDelete
    }

  def assertLower(segment: Segment) =
    (1 until keyValues.size) foreach {
      index =>
        //        println(s"index: $index")
        //        segment.lowerKeyValue(keyValues(index).key)
        val keyValue = keyValues(index)
        val expectedLower = keyValues(index - 1)
        val lower = segment.lower(keyValue.key).assertGet
        lower.key shouldBe expectedLower.key
        lower.getOrFetchValue.assertGetOpt shouldBe expectedLower.getOrFetchValue.assertGetOpt
        lower.isDelete shouldBe expectedLower.isDelete
    }

  var segment: Segment = null

  def initSegment() =
    segment = TestSegment(keyValues).assertGet

  def reopenSegment() = {
    println("Re-opening Segment")
    segment.close.assertGet
    segment.clearCache()
    segment = Segment(
      path = segment.path,
      mmapReads = levelStorage.mmapSegmentsOnRead,
      mmapWrites = levelStorage.mmapSegmentsOnWrite,
      cacheKeysOnCreate = false,
      minKey = keyValues.head.key,
      maxKey = keyValues.last.key,
      segmentSize = keyValues.last.stats.segmentSize,
      removeDeletes = false
    ).assertGet
  }

  "Segment read benchmark 1" in {
    initSegment()

    benchmark(s"read ${keyValues.size} key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertGet(segment)
    }
  }

  "Segment read benchmark 2" in {
    benchmark(s"read ${keyValues.size} cached key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertGet(segment)
    }
  }

  "Segment read benchmark 3" in {
    if (persistent) reopenSegment()
    benchmark(s"read ${keyValues.size} lower keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertLower(segment)
    }
  }

  "Segment read benchmark 4" in {
    benchmark(s"read ${keyValues.size} cached lower keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertLower(segment)
    }
  }

  "Segment read benchmark 5" in {
    if (persistent) reopenSegment()
    benchmark(s"read ${keyValues.size} higher keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertHigher(segment)
    }
  }

  "Segment read benchmark 6" in {
    benchmark(s"read ${keyValues.size} cached higher keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertHigher(segment)
    }
  }
}