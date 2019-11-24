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

import swaydb.core.TestData._
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.Transient
import swaydb.core.io.file.BlockCache
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.segment.format.a.entry.id.BaseEntryIdFormatA
import swaydb.core.segment.{PersistentSegment, ReadState, Segment}
import swaydb.core.util.{Benchmark, BlockCacheFileIDGenerator}
import swaydb.core.{TestBase, TestSweeper, TestTimer}
import swaydb.data.config.{IOAction, IOStrategy}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice

import scala.util.Random

class SegmentReadPerformanceSpec0 extends SegmentReadPerformanceSpec {
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
}

class SegmentReadPerformanceSpec1 extends SegmentReadPerformanceSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = true
  override def mmapSegmentsOnRead = true
  override def level0MMAP = true
  override def appendixStorageMMAP = true
}

class SegmentReadPerformanceSpec2 extends SegmentReadPerformanceSpec {
  override def levelFoldersCount = 10
  override def mmapSegmentsOnWrite = false
  override def mmapSegmentsOnRead = false
  override def level0MMAP = false
  override def appendixStorageMMAP = false
}

class SegmentReadPerformanceSpec3 extends SegmentReadPerformanceSpec {
  override def inMemoryStorage = true
}

sealed trait SegmentReadPerformanceSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  val keyValuesCount = 1000000

  //    override def deleteFiles = false

  implicit val maxOpenSegmentsCacheImplicitLimiter: FileSweeper.Enabled = TestSweeper.fileSweeper
  //    implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = TestSweeper.someMemorySweeperMax
  implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = TestSweeper.someMemorySweeper10
  //  implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = None
  implicit val blockCache: Option[BlockCache.State] = TestSweeper.blockCache

  def strategy(action: IOAction): IOStrategy =
    action match {
      case IOAction.OpenResource =>
        IOStrategy.SynchronisedIO(cacheOnAccess = true)
      case IOAction.ReadDataOverview =>
        IOStrategy.SynchronisedIO(cacheOnAccess = true)
      case IOAction.ReadCompressedData(compressedSize, decompressedSize) =>
        ???
      case IOAction.ReadUncompressedData(size) =>
        IOStrategy.SynchronisedIO(cacheOnAccess = false)
    }

  implicit val segmentIO =
    new SegmentIO(
      segmentBlockIO = {
        case IOAction.OpenResource =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case IOAction.ReadDataOverview =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DataAction =>
          IOStrategy.SynchronisedIO(cacheOnAccess = false)
      },
      hashIndexBlockIO = {
        case IOAction.OpenResource =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case IOAction.ReadDataOverview =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DataAction =>
          IOStrategy.SynchronisedIO(cacheOnAccess = false)
      },
      bloomFilterBlockIO = {
        case IOAction.OpenResource =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case IOAction.ReadDataOverview =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DataAction =>
          IOStrategy.SynchronisedIO(cacheOnAccess = false)
      },
      binarySearchIndexBlockIO = {
        case IOAction.OpenResource =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case IOAction.ReadDataOverview =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DataAction =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
      },
      sortedIndexBlockIO = {
        case IOAction.OpenResource =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case IOAction.ReadDataOverview =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DataAction =>
          IOStrategy.SynchronisedIO(cacheOnAccess = false)
      },
      valuesBlockIO = {
        case IOAction.OpenResource =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case IOAction.ReadDataOverview =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DataAction =>
          IOStrategy.SynchronisedIO(cacheOnAccess = false)
      },
      segmentFooterBlockIO = {
        case IOAction.OpenResource =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case IOAction.ReadDataOverview =>
          IOStrategy.SynchronisedIO(cacheOnAccess = true)
        case action: IOAction.DataAction =>
          IOStrategy.SynchronisedIO(cacheOnAccess = false)
      }
    )

  implicit val timer = TestTimer.Empty

  val keyValues: Slice[Transient] =
    randomKeyValues(
      keyValuesCount,
      valueSize = 4,
      startId = Some(1),
      sortedIndexConfig =
        SortedIndexBlock.Config(
          ioStrategy = _ => IOStrategy.ConcurrentIO(cacheOnAccess = false),
          prefixCompressionResetCount = 0,
          enableAccessPositionIndex = true,
          disableKeyPrefixCompression = false,
          normaliseIndex = false,
          compressions = _ => Seq.empty
        ),
      binarySearchIndexConfig =
        BinarySearchIndexBlock.Config(
          enabled = true,
          minimumNumberOfKeys = 1,
          searchSortedIndexDirectlyIfPossible = false,
          fullIndex = true,
          ioStrategy = _ => IOStrategy.ConcurrentIO(cacheOnAccess = false),
          compressions = _ => Seq.empty
        ),
      //      binarySearchIndexConfig =
      //        BinarySearchIndexBlock.Config.disabled,
      valuesConfig =
        ValuesBlock.Config(
          compressDuplicateValues = true,
          compressDuplicateRangeValues = true,
          ioStrategy = strategy,
          compressions = _ => Seq.empty
        ),
      //      hashIndexConfig =
      //        HashIndexBlock.Config(
      //          maxProbe = 10,
      //          copyIndex = false,
      //          minimumNumberOfKeys = 5,
      //          minimumNumberOfHits = 5,
      //          allocateSpace = _.requiredSpace * 2,
      //          ioStrategy = _ => IOStrategy.ConcurrentIO(cacheOnAccess = false),
      //          compressions = _ => Seq.empty
      //        ),
      hashIndexConfig = HashIndexBlock.Config.disabled,
      bloomFilterConfig =
        BloomFilterBlock.Config.disabled
      //        BloomFilterBlock.Config(
      //          falsePositiveRate = 0.001,
      //          minimumNumberOfKeys = 2,
      //          optimalMaxProbe = _ => 1,
      //          blockIO = _ => IOStrategy.SynchronisedIO(cacheOnAccess = true),
      //          compressions = _ => Seq.empty
      //        )
    )

  val shuffledKeyValues = Random.shuffle(keyValues)

  def assertGet(segment: Segment) = {
    val readState = ReadState.limitHashMap(1)
    shuffledKeyValues foreach {
      keyValue =>
        //        if (index % 10000 == 0)
        //          segment.get(shuffledKeyValues.head.key)
        //
        //        val key = keyValue.key.readInt()
        ////        if (key % 1000 == 0)
        //          println(key)
        //        val found = segment.get(keyValue.key).get.get
        //        found.getOrFetchValue
        //        segment.get(keyValue.key).get.key shouldBe keyValue.key
        segment.get(keyValue.key, readState).get
    }
  }

  def assertHigher(segment: Segment) = {
    val readState = ReadState.hashMap()
    (0 until keyValues.size - 1) foreach {
      index =>
        //        segment.higherKey(keyValues(index).key)
        //        println(s"index: $index")
        val keyValue = keyValues(index)
        //        val expectedHigher = unGroupedKeyValues(index + 1)
        //        segment.higher(keyValue.key).get.get shouldBe expectedHigher
        segment.higher(keyValue.key, readState).get
    }
  }

  def assertLower(segment: Segment) = {
    val readState = ReadState.hashMap()
    (1 until keyValues.size) foreach {
      index =>
        //        println(s"index: $index")
        //        segment.lowerKeyValue(keyValues(index).key)
        val keyValue = keyValues(index)
        //        val expectedLower = unGroupedKeyValues(index - 1)
        //        segment.lower(keyValue.key).value.get shouldBe expectedLower
        segment.lower(keyValue.key, readState).get
    }
  }

  var segment: Segment = null

  def warmUp() =
    Benchmark("warm up") {
      BaseEntryIdFormatA.baseIds.foreach(id => id.getClass)
    }

  def initSegment() = {
    warmUp()

    Benchmark(s"Creating segment. keyValues: ${keyValues.size}") {
      val segmentConfig = SegmentBlock.Config(strategy, _ => Seq.empty)
      segment = TestSegment(keyValues, segmentConfig = segmentConfig)
    }
  }

  def reopenSegment() = {
    println("Re-opening Segment")
    segment.close()
    segment.clearAllCaches()
    segment = Segment(
      path = segment.path,
      segmentId = segment.segmentId,
      mmapReads = levelStorage.mmapSegmentsOnRead,
      mmapWrites = levelStorage.mmapSegmentsOnWrite,
      blockCacheFileId = BlockCacheFileIDGenerator.nextID,
      minKey = segment.minKey,
      maxKey = segment.maxKey,
      segmentSize = segment.segmentSize,
      nearestExpiryDeadline = segment.nearestExpiryDeadline,
      minMaxFunctionId = segment.minMaxFunctionId
    )
  }

  "Segment value benchmark 1" in {
    initSegment()

    //    val all = segment.getAll()

    println(s"PrefixedCompressed     count: ${keyValues.count(_.isPrefixCompressed)}")
    println(s"not PrefixedCompressed count: ${keyValues.count(!_.isPrefixCompressed)}")
    println

    segment.asInstanceOf[PersistentSegment].segmentCache.blockCache.getHashIndex() foreach {
      hashIndex =>
        println(s"hashIndex.hit: ${hashIndex.hit}")
        println(s"hashIndex.miss: ${hashIndex.miss}")
        println(s"hashIndex.size: ${hashIndex.offset.size}")
        println
    }

    segment.asInstanceOf[PersistentSegment].segmentCache.blockCache.getBinarySearchIndex() foreach {
      binarySearch =>
        println(s"binarySearch.valuesCount: ${binarySearch.valuesCount}")
        println(s"binarySearch.bytesPerValue: ${binarySearch.bytesPerValue}")
        println(s"binarySearch.isFullIndex: ${binarySearch.isFullIndex}")
        println(s"binarySearch.size: ${binarySearch.offset.size}")
        println
    }

    //    (0 to EntryReader.readers.last.maxID) foreach {
    //      id =>
    //        try
    //          EntryReader.readers.foreach(_.read(id, id, id, None, null, None, 0, 0, 0, None, null).get)
    //        catch {
    //          case exception: Throwable =>
    //        }
    //    }

    //
    //    val file = DBFile.mmapRead(segment.path, randomIOStrategy(false), true).get
    //
    //
    //    val reader = Reader(file)
    //
    //    Benchmark("") {
    //      (1 to 1000000) foreach {
    //        i =>
    //          val sisisis = reader.moveTo(randomIntMax(reader.size.get.toInt - 5)).read(4).get
    //          println(reader.moveTo(randomIntMax(reader.size.get.toInt - 5)).read(4).get)
    //          println(reader.moveTo(randomIntMax(reader.size.get.toInt - 5)).read(4).get)
    //        //          println(sisisis)
    //      }
    //    }

    Benchmark(s"value ${keyValues.size} key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertGet(segment)
//            segment.getAll()
      //      blockCache.foreach(_.clear())
//            segment.getAll()
      //      segment.getAll().get
      //      blockCache.foreach(_.clear())
      //      segment.getAll().get
      //      blockCache.foreach(_.clear())
      //      segment.getAll().get
      //      blockCache.foreach(_.clear())
      //      segment.getAll().get
    }

    def printStats() = {

      //      println("seqSeeks: " + SegmentSearcher.seqSeeks)
      //      println("successfulSeqSeeks: " + SegmentSearcher.successfulSeqSeeks)
      //      println("failedSeqSeeks: " + SegmentSearcher.failedSeqSeeks)
      //      println
      //
      //      println("hashIndexSeeks: " + SegmentSearcher.hashIndexSeeks)
      //      println("successfulHashIndexSeeks: " + SegmentSearcher.successfulHashIndexSeeks)
      //      println("failedHashIndexSeeks: " + SegmentSearcher.failedHashIndexSeeks)
      //      println

      //      println("binarySeeks: " + BinarySearchIndexBlock.binarySeeks)
      //      println("binarySuccessfulSeeks: " + BinarySearchIndexBlock.binarySuccessfulSeeks)
      //      println("binaryFailedSeeks: " + BinarySearchIndexBlock.binaryFailedSeeks)
      //      println("failedWithLower: " + BinarySearchIndexBlock.failedWithLower)
      //      println("greaterLower: " + BinarySearchIndexBlock.greaterLower)
      //      println("sameLower: " + BinarySearchIndexBlock.sameLower)
      //      println("Hops: " + BinarySearchIndexBlock.totalHops)
      //      println("maxHops: " + BinarySearchIndexBlock.maxHop)
      //      println("minHop: " + BinarySearchIndexBlock.minHop)
      println

      //      println("diskSeeks: " + BlockCache.diskSeeks)
      //      println("memorySeeks: " + BlockCache.memorySeeks)
      //      println("splitsCount: " + BlockCache.splitsCount)

      //      println("ends: " + SortedIndexBlock.ends)
      println
    }

    printStats()
    //
    //    BlockCache.diskSeeks = 0
    //    BlockCache.memorySeeks = 0
    //    BlockCache.splitsCount = 0
    //    BinarySearchIndexBlock.totalHops = 0
    //    BinarySearchIndexBlock.minHop = 0
    //    BinarySearchIndexBlock.maxHop = 0
    //    BinarySearchIndexBlock.binarySeeks = 0
    //    BinarySearchIndexBlock.binarySuccessfulSeeks = 0
    //    BinarySearchIndexBlock.binaryFailedSeeks = 0
    //    BinarySearchIndexBlock.failedWithLower = 0
    //    BinarySearchIndexBlock.greaterLower = 0
    //    BinarySearchIndexBlock.sameLower = 0
    //    SegmentSearcher.hashIndexSeeks = 0
    //    SegmentSearcher.successfulHashIndexSeeks = 0
    //    SegmentSearcher.failedHashIndexSeeks = 0
    //
    //    //    blockCache.foreach(_.clear())
    //
    //    //todo - TOO MANY BINARY HOPS BUT SUCCESSES ARE LESS.
    //    //
    //    Benchmark(s"value ${keyValues.size} key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
    //      assertGet(segment)
    //      //      segment.getAll().get
    //    }
    //
    //    printStats()
  }

  "Segment value benchmark 2" in {
    Benchmark(s"value ${keyValues.size} cached key values when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertGet(segment)
    }
  }

  "Segment lower benchmark 3" in {
    initSegment()
    //    if (persistent) reopenSegment()
    Benchmark(s"lower ${keyValues.size} lower keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertLower(segment)
    }

    Benchmark(s"lower ${keyValues.size} lower keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertLower(segment)
    }
  }

  "Segment lower benchmark 4" in {
    Benchmark(s"lower ${keyValues.size} cached lower keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertLower(segment)
    }
  }

  "Segment higher benchmark 5" in {
    initSegment()
    //    if (persistent) reopenSegment()
    Benchmark(s"higher ${keyValues.size} higher keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertHigher(segment)
    }

    Benchmark(s"higher ${keyValues.size} higher keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertHigher(segment)
    }
  }

  "Segment higher benchmark 6" in {
    Benchmark(s"higher ${keyValues.size} cached higher keys when Segment memory = $memory, mmapSegmentWrites = ${levelStorage.mmapSegmentsOnWrite}, mmapSegmentReads = ${levelStorage.mmapSegmentsOnRead}") {
      assertHigher(segment)
    }
  }
}
