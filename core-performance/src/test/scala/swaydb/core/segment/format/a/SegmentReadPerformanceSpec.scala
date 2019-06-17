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

package swaydb.core.segment.format.a

import swaydb.configs.level.DefaultGroupingStrategy
import swaydb.core.data.KeyValue
import swaydb.core.group.compression.data.KeyValueGroupingStrategyInternal
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestData, TestLimitQueues}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.core.TestData._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.IOAssert._
import swaydb.core.io.file.DBFile
import swaydb.core.segment.Segment

import scala.collection.mutable.ListBuffer

//@formatter:off
class SegmentReadPerformanceSpec0 extends SegmentReadPerformanceSpec {
  val testGroupedKeyValues: Boolean = false
}

class SegmentReadPerformanceSpec1 extends SegmentReadPerformanceSpec {
  val testGroupedKeyValues: Boolean = false

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentReadPerformanceSpec2 extends SegmentReadPerformanceSpec {
  val testGroupedKeyValues: Boolean = false
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentReadPerformanceSpec3 extends SegmentReadPerformanceSpec {
  val testGroupedKeyValues: Boolean = false
  override def inMemoryStorage = true
}

class SegmentReadPerformanceGroupedKeyValuesSpec0 extends SegmentReadPerformanceSpec {
  val testGroupedKeyValues: Boolean = true
}

class SegmentReadPerformanceGroupedKeyValuesSpec1 extends SegmentReadPerformanceSpec {
  val testGroupedKeyValues: Boolean = true

  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentReadPerformanceGroupedKeyValuesSpec2 extends SegmentReadPerformanceSpec {
  val testGroupedKeyValues: Boolean = true
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentReadPerformanceGroupedKeyValuesSpec3 extends SegmentReadPerformanceSpec {
  val testGroupedKeyValues: Boolean = true
  override def inMemoryStorage = true
}
//@formatter:on

sealed trait SegmentReadPerformanceSpec extends TestBase with Benchmark {

  implicit val keyOrder = KeyOrder.default
  implicit def groupingStrategy = randomGroupingStrategyOption(randomNextInt(1000))
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  def testGroupedKeyValues: Boolean

  val keyValuesCount = 10000000

  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter

  lazy val unGroupedKeyValues: Slice[KeyValue.WriteOnly] =
    randomKeyValues(keyValuesCount, startId = Some(1))

  //  val unGroupedRandomKeyValues: List[KeyValue.WriteOnly] =
  //    Random.shuffle(unGroupedKeyValues.toList)

  lazy val groupedKeyValues: Slice[KeyValue.WriteOnly] = {
    val grouped =
      SegmentMerger.split(
        keyValues = unGroupedKeyValues,
        minSegmentSize = 1000.mb,
        isLastLevel = false,
        forInMemory = false,
        maxProbe = TestData.maxProbe,
        bloomFilterFalsePositiveRate = TestData.falsePositiveRate,
        resetPrefixCompressionEvery = TestData.resetPrefixCompressionEvery,
        minimumNumberOfKeyForHashIndex = TestData.minimumNumberOfKeyForHashIndex,
        enableRangeFilterAndIndex = TestData.enableRangeFilterAndIndex,
        hashIndexCompensation = TestData.hashIndexCompensation,
        compressDuplicateValues = true
      )(keyOrder = keyOrder, groupingStrategy = Some(KeyValueGroupingStrategyInternal(DefaultGroupingStrategy()))).assertGet

    grouped should have size 1
    grouped.head.toSlice
  }

  def keyValues = if (testGroupedKeyValues) groupedKeyValues else unGroupedKeyValues

  def assertGet(segment: Segment) =
    unGroupedKeyValues foreach {
      keyValue =>
        val key = keyValue.key.readInt()
        if (key % 1000 == 0)
          println(key)
        segment.get(keyValue.key)
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

  def initSegment() = {
    println("Creating segment...")
    segment = TestSegment(keyValues).assertGet
    println("Segment created.")
  }

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
      nearestExpiryDeadline = segment.nearestExpiryDeadline
    ).assertGet
  }

  "Segment getFromHashIndex benchmark 1" in {
    initSegment()

    benchmark(s"getFromHashIndex ${keyValues.size} key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertGet(segment)
    }
  }

  "Segment getFromHashIndex benchmark 2" in {
    benchmark(s"getFromHashIndex ${keyValues.size} cached key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
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