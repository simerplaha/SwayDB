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

import swaydb.IOValues._
import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.Transient
import swaydb.core.group.compression.GroupByInternal
import swaydb.core.queue.{FileLimiter, KeyValueLimiter}
import swaydb.core.segment.Segment
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.merge.SegmentMerger
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestLimitQueues}
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._

class SegmentReadPerformanceSpec0 extends SegmentReadPerformanceSpec {
  val testGroupedKeyValues: Boolean = false
  //  override def mmapSegmentsOnWrite = false
  //  override def mmapSegmentsOnRead = false
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

sealed trait SegmentReadPerformanceSpec extends TestBase with Benchmark {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  def testGroupedKeyValues: Boolean

  val keyValuesCount = 1000000

  implicit val maxSegmentsOpenCacheImplicitLimiter: FileLimiter = TestLimitQueues.fileOpenLimiter
  implicit val keyValuesLimitImplicitLimiter: KeyValueLimiter = TestLimitQueues.keyValueLimiter

  def strategy(action: IOAction): IOStrategy =
    action match {
      case IOAction.ReadDataOverview(size) =>
        IOStrategy.ConcurrentIO(cacheOnAccess = true)
      case IOAction.ReadCompressedData(compressedSize, decompressedSize) =>
        ???
      case IOAction.ReadUncompressedData(size) =>
        IOStrategy.ConcurrentIO(cacheOnAccess = false)
      case IOAction.OpenResource =>
        IOStrategy.ConcurrentIO(cacheOnAccess = true)
    }

  implicit val segmentIO =
    new SegmentIO(
      segmentBlockIO = strategy,
      hashIndexBlockIO = strategy,
      bloomFilterBlockIO = _ => IOStrategy.ConcurrentIO(cacheOnAccess = true),
      binarySearchIndexBlockIO = strategy,
      sortedIndexBlockIO = strategy,
      valuesBlockIO = strategy,
      segmentFooterBlockIO = strategy
    )

  //    lazy val unGroupedKeyValues: Slice[Transient] =
  //      randomKeyValues(
  //        keyValuesCount,
  //        startId = Some(1),
  //        valuesConfig =
  //          ValuesBlock.Config(
  //            compressDuplicateValues = true,
  //            compressDuplicateRangeValues = true,
  //            blockIO = strategy,
  //            compressions = _ => Seq.empty
  //          ),
  //        sortedIndexConfig =
  //          SortedIndexBlock.Config(
  //            blockIO = strategy,
  //            prefixCompressionResetCount = 0,
  //            enableAccessPositionIndex = true,
  //            compressions = _ => Seq.empty
  //          ),
  //        binarySearchIndexConfig =
  //          BinarySearchIndexBlock.Config(
  //            enabled = true,
  //            minimumNumberOfKeys = 1,
  //            fullIndex = true,
  //            blockIO = strategy,
  //            compressions = _ => Seq.empty
  //          ),
  //        hashIndexConfig =
  //          HashIndexBlock.Config(
  //            maxProbe = 5,
  //            minimumNumberOfKeys = 2,
  //            minimumNumberOfHits = 2,
  //            allocateSpace = _.requiredSpace * 10,
  //            blockIO = strategy,
  //            compressions = _ => Seq.empty
  //          ),
  //        bloomFilterConfig =
  //          BloomFilterBlock.Config(
  //            falsePositiveRate = 0.001,
  //            minimumNumberOfKeys = 2,
  //            blockIO = strategy,
  //            compressions = _ => Seq.empty
  //          )
  //      )

  lazy val unGroupedKeyValues: Slice[Transient] =
    randomKeyValues(
      keyValuesCount,
      valueSize = 4,
      startId = Some(1),
      sortedIndexConfig =
        SortedIndexBlock.Config(
          blockIO = strategy,
          prefixCompressionResetCount = 0,
          enableAccessPositionIndex = false,
          compressions = _ => Seq.empty
        ),
      binarySearchIndexConfig =
        BinarySearchIndexBlock.Config.disabled,
      valuesConfig =
        ValuesBlock.Config(
          compressDuplicateValues = true,
          compressDuplicateRangeValues = true,
          blockIO = strategy,
          compressions = _ => Seq.empty
        ),
      hashIndexConfig =
        HashIndexBlock.Config(
          maxProbe = 5,
          minimumNumberOfKeys = 2,
          minimumNumberOfHits = 2,
          allocateSpace = _.requiredSpace * 10,
          blockIO = strategy,
          compressions = _ => Seq.empty
        ),
      bloomFilterConfig =
        BloomFilterBlock.Config.disabled
      //      bloomFilterConfig =
      //        BloomFilterBlock.Config(
      //          falsePositiveRate = 0.001,
      //          minimumNumberOfKeys = 2,
      //          blockIO = _ => IOStrategy.ConcurrentIO(cacheOnAccess = true),
      //          compressions = _ => Seq.empty
      //        )
    )

  //  val unGroupedRandomKeyValues: List[Transient] =
  //    Random.shuffle(unGroupedKeyValues.toList)

  lazy val groupedKeyValues: Slice[Transient] = {
    val grouped =
      SegmentMerger.split(
        keyValues = unGroupedKeyValues,
        minSegmentSize = 1000.mb,
        isLastLevel = false,
        forInMemory = false,
        createdInLevel = randomIntMax(),
        valuesConfig = ValuesBlock.Config.random,
        sortedIndexConfig = SortedIndexBlock.Config.random,
        binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
        hashIndexConfig = HashIndexBlock.Config.random,
        bloomFilterConfig = BloomFilterBlock.Config.random,
        segmentIO = SegmentIO.random
      )(keyOrder = keyOrder, groupBy = Some(randomGroupBy(100))).value

    grouped should have size 1
    grouped.head.toSlice
  }

  def keyValues = if (testGroupedKeyValues) groupedKeyValues else unGroupedKeyValues

  def assertGet(segment: Segment) = {
    //        val shuffed = Random.shuffle(unGroupedKeyValues)
    //        Benchmark("shuffled") {
    //          shuffed foreach {
    //            keyValue =>
    //              //        val key = keyValue.key.readInt()
    //              //        if (key % 1000 == 0)
    //              //          println(key)
    //              segment.get(keyValue.key).get
    //          }
    //        }

    keyValues.par foreach {
      keyValue =>
        //        val key = keyValue.key.readInt()
        //        if (key % 1000 == 0)
        //          println(key)
        segment.get(keyValue.key)
    }
  }

  def assertHigher(segment: Segment) = {
    (0 until unGroupedKeyValues.size - 1) foreach {
      index =>
        //        segment.higherKey(keyValues(index).key)
        //        println(s"index: $index")
        val keyValue = unGroupedKeyValues(index)
        val expectedHigher = unGroupedKeyValues(index + 1)
        segment.higher(keyValue.key).value shouldBe expectedHigher
    }
  }

  def assertLower(segment: Segment) =
    (1 until unGroupedKeyValues.size) foreach {
      index =>
        //        println(s"index: $index")
        //        segment.lowerKeyValue(keyValues(index).key)
        val keyValue = unGroupedKeyValues(index)
        val expectedLower = unGroupedKeyValues(index - 1)
        segment.lower(keyValue.key).value shouldBe expectedLower
    }

  var segment: Segment = null

  def warmUp() =
    Benchmark("warm up") {
      BaseEntryIdFormatA.baseIds.foreach(id => id.getClass)
    }

  def initSegment() = {
    warmUp()
    Benchmark(s"Creating segment. keyValues: ${keyValues.size}") {
      implicit val groupBy: Option[GroupByInternal.KeyValues] = None
      val segmentConfig = SegmentBlock.Config(strategy, _ => Seq.empty)
      segment = TestSegment(keyValues, segmentConfig = segmentConfig).value
    }
  }

  def reopenSegment() = {
    println("Re-opening Segment")
    segment.close.value
    segment.clearAllCaches()
    segment = Segment(
      path = segment.path,
      mmapReads = levelStorage.mmapSegmentsOnRead,
      mmapWrites = levelStorage.mmapSegmentsOnWrite,
      minKey = segment.minKey,
      maxKey = segment.maxKey,
      segmentSize = segment.segmentSize,
      nearestExpiryDeadline = segment.nearestExpiryDeadline,
      minMaxFunctionId = segment.minMaxFunctionId
    ).value
  }

  "Segment value benchmark 1" in {
    initSegment()

    benchmark(s"value ${keyValues.size} key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertGet(segment)
    }

    //    segment.clearCachedKeyValues()
    //
    benchmark(s"value ${keyValues.size} key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertGet(segment)
    }
  }

  "Segment value benchmark 2" in {
    benchmark(s"value ${keyValues.size} cached key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
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
