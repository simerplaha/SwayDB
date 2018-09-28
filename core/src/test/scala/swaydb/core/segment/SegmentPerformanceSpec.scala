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

import swaydb.core.data.{KeyValue, Persistent}
import swaydb.core.io.file.DBFile
import swaydb.core.queue.KeyValueLimiter
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.segment.MaxKey
import swaydb.order.KeyOrder

//@formatter:off
class SegmentPerformanceSpec0 extends SegmentPerformanceSpec

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


sealed trait SegmentPerformanceSpec extends TestBase with Benchmark {

  override implicit val ordering = KeyOrder.default
  implicit val compression = groupingStrategy
  val keyValuesCount = 10000


  implicit val maxSegmentsOpenCacheImplicitLimiter: DBFile => Unit = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter

  val keyValues = randomIntKeyValues(keyValuesCount)

  def assertGet(segment: Segment) =
    keyValues.foreach(keyValue => segment.get(keyValue.key).assertGet shouldBe keyValue)

  def assertHigher(segment: Segment) =
    (0 until keyValues.size - 1) foreach {
      index =>
        //        segment.higherKey(keyValues(index).key)
        //        println(s"index: $index")
        val keyValue = keyValues(index)
        val expectedHigher = keyValues(index + 1)
        segment.higher(keyValue.key).assertGet shouldBe expectedHigher
    }

  def assertLower(segment: Segment) =
    (1 until keyValues.size) foreach {
      index =>
        //        println(s"index: $index")
        //        segment.lowerKeyValue(keyValues(index).key)
        val keyValue = keyValues(index)
        val expectedLower = keyValues(index - 1)
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
      minKey = keyValues.head.key,
      maxKey =
      keyValues.last match {
        case range: KeyValue.WriteOnly.Range =>
          MaxKey.Range(range.fromKey, range.toKey)
        case _ =>
          MaxKey.Fixed(keyValues.last.key)
      },
      segmentSize = keyValues.last.stats.segmentSize,
      nearestExpiryDeadline = segment.nearestExpiryDeadline,
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