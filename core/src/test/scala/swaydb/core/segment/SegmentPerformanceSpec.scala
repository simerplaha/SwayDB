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

import swaydb.configs.level.DefaultGroupingStrategy
import swaydb.core.data.KeyValue
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.io.file.DBFile
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.order.KeyOrder

//@formatter:off
class SegmentPerformanceSpec0 extends SegmentPerformanceSpec {
  val testGroupedKeyValues: Boolean = false
}

class SegmentPerformanceSpec1 extends SegmentPerformanceSpec {
  val testGroupedKeyValues: Boolean = false

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentPerformanceSpec2 extends SegmentPerformanceSpec {
  val testGroupedKeyValues: Boolean = false
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentPerformanceSpec3 extends SegmentPerformanceSpec {
  val testGroupedKeyValues: Boolean = false
  override def inMemoryStorage = true
}

class SegmentPerformanceGroupedKeyValuesSpec0 extends SegmentPerformanceSpec {
  val testGroupedKeyValues: Boolean = true
}

class SegmentPerformanceGroupedKeyValuesSpec1 extends SegmentPerformanceSpec {
  val testGroupedKeyValues: Boolean = true

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentPerformanceGroupedKeyValuesSpec2 extends SegmentPerformanceSpec {
  val testGroupedKeyValues: Boolean = true
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentPerformanceGroupedKeyValuesSpec3 extends SegmentPerformanceSpec {
  val testGroupedKeyValues: Boolean = true
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait SegmentPerformanceSpec extends TestBase with Benchmark {

  override implicit val ordering = KeyOrder.default
  implicit val compression = groupingStrategy

  def testGroupedKeyValues: Boolean

  val keyValuesCount = 1000000

  implicit val maxSegmentsOpenCacheImplicitLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter

  val unGroupedKeyValues: Slice[KeyValue.WriteOnly] =
    randomIntKeyValues(keyValuesCount, startId = Some(1))

  //  val unGroupedRandomKeyValues: List[KeyValue.WriteOnly] =
  //    Random.shuffle(unGroupedKeyValues.toList)

  val groupedKeyValues: Slice[KeyValue.WriteOnly] = {
    val grouped =
      SegmentMerger.split(
        keyValues = unGroupedKeyValues,
        minSegmentSize = 1000.mb,
        isLastLevel = false,
        forInMemory = false,
        bloomFilterFalsePositiveRate = 0.1,
        compressDuplicateValues = true
      )(ordering = ordering, groupingStrategy = Some(KeyValueGroupingStrategyInternal(DefaultGroupingStrategy()))).assertGet

    grouped should have size 1
    grouped.head.toSlice
  }

  def keyValues = if (testGroupedKeyValues) groupedKeyValues else unGroupedKeyValues

  def assertGet(segment: Segment) =
    unGroupedKeyValues foreach {
      keyValue =>
        segment.get(keyValue.key).assertGet shouldBe keyValue
    }

  def assertHigher(segment: Segment) = {
    (0 until unGroupedKeyValues.size - 1) foreach {
      index =>
        //        segment.higherKey(keyValues(index).key)
        //        println(s"index: $index")
        val keyValue = unGroupedKeyValues(index)
        val expectedHigher = unGroupedKeyValues(index + 1)
        segment.higher(keyValue.key).assertGet shouldBe expectedHigher
    }

  }

  def assertLower(segment: Segment) =
    (1 until unGroupedKeyValues.size) foreach {
      index =>
        //        println(s"index: $index")
        //        segment.lowerKeyValue(keyValues(index).key)
        val keyValue = unGroupedKeyValues(index)
        val expectedLower = unGroupedKeyValues(index - 1)
        segment.lower(keyValue.key).assertGet shouldBe expectedLower
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
      minKey = segment.minKey,
      maxKey = segment.maxKey,
      segmentSize = segment.segmentSize,
      nearestExpiryDeadline = segment.nearestExpiryDeadline,
      removeDeletes = false
    ).assertGet
  }

  "Segment get benchmark 1" in {
    initSegment()

    benchmark(s"get ${keyValues.size} key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertGet(segment)
    }
  }

  "Segment get benchmark 2" in {
    benchmark(s"get ${keyValues.size} cached key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertGet(segment)
    }
  }

  "Segment lower benchmark 3" in {
    if (persistent) reopenSegment()
    benchmark(s"lower ${keyValues.size} lower keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertLower(segment)
    }
  }

  "Segment lower benchmark 4" in {
    benchmark(s"lower ${keyValues.size} cached lower keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertLower(segment)
    }
  }

  "Segment higher benchmark 5" in {
    if (persistent) reopenSegment()
    benchmark(s"higher ${keyValues.size} higher keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertHigher(segment)
    }
  }

  "Segment higher benchmark 6" in {
    benchmark(s"higher ${keyValues.size} cached higher keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertHigher(segment)
    }
  }
}