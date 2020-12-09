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

package swaydb.core.segment.format.a

import java.nio.file.Path
import swaydb.core.TestData._
import swaydb.core.actor.FileSweeper
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.data.Memory
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.segment.format.a.block.binarysearch.{BinarySearchEntryFormat, BinarySearchIndexBlock}
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.hashindex.{HashIndexBlock, HashIndexEntryFormat}
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.core.segment.format.a.entry.reader.PersistentReader
import swaydb.core.segment.{PersistentSegmentMany, PersistentSegmentOne, Segment, SegmentReadIO, ThreadReadState}
import swaydb.core.util.Benchmark
import swaydb.core.{TestBase, TestCaseSweeper, TestForceSave, TestSweeper, TestTimer}
import swaydb.data.config.{Dir, IOAction, IOStrategy, MMAP, PushForwardStrategy}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.OperatingSystem
import TestCaseSweeper._
import swaydb.core.level.PathsDistributor
import swaydb.core.segment.merge.MergeStats
import swaydb.data.util.StorageUnits._

import scala.concurrent.duration._
import scala.util.Random

class SegmentReadPerformanceSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  val keyValuesCount = 1000000

  val keyValues: Slice[Memory] =
    randomKeyValues(
      count = keyValuesCount,
      startId = Some(1),
      valueSize = 4
    )

  val shuffledKeyValues = Random.shuffle(keyValues)

  def initSegment(): Segment = {
    PersistentReader.populateBaseEntryIds()

    val path: Path = testSegmentFile

    implicit val pathsDistributor = PathsDistributor(Seq(Dir(path.getParent, 1)), () => Seq.empty)

    val sortedIndexConfig =
      SortedIndexBlock.Config(
        ioStrategy = {
          case _: IOAction.ReadDataOverview =>
            IOStrategy.ConcurrentIO(cacheOnAccess = true)

          case _: IOAction.DecompressAction =>
            IOStrategy.ConcurrentIO(cacheOnAccess = false)
        },
        enablePrefixCompression = false,
        shouldPrefixCompress = _ % 5 == 0,
        prefixCompressKeysOnly = true,
        enableAccessPositionIndex = true,
        optimiseForReverseIteration = true,
        normaliseIndex = false,
        compressions = _ => Seq.empty
      )

    val binarySearchIndexConfig =
      BinarySearchIndexBlock.Config(
        enabled = true,
        format = BinarySearchEntryFormat.Reference,
        minimumNumberOfKeys = 1,
        searchSortedIndexDirectlyIfPossible = false,
        fullIndex = true,
        ioStrategy = {
          case _: IOAction.ReadDataOverview =>
            IOStrategy.ConcurrentIO(cacheOnAccess = true)

          case _: IOAction.DecompressAction =>
            IOStrategy.ConcurrentIO(cacheOnAccess = false)
        },
        compressions = _ => Seq.empty
      )

    //    val binarySearchIndexConfig =
    //      BinarySearchIndexBlock.Config.disabled

    val valuesConfig =
      ValuesBlock.Config(
        compressDuplicateValues = true,
        compressDuplicateRangeValues = true,
        ioStrategy = {
          case _: IOAction.ReadDataOverview =>
            IOStrategy.ConcurrentIO(cacheOnAccess = true)

          case _: IOAction.DecompressAction =>
            IOStrategy.ConcurrentIO(cacheOnAccess = false)
        },
        compressions = _ => Seq.empty
      )

    //    val hashIndexConfig =
    //      HashIndexBlock.Config(
    //        maxProbe = 2,
    //        format = HashIndexEntryFormat.Reference,
    //        minimumNumberOfKeys = 5,
    //        minimumNumberOfHits = 5,
    //        allocateSpace = _.requiredSpace,
    //        ioStrategy = {
    //          case _: IOAction.ReadDataOverview =>
    //            IOStrategy.ConcurrentIO(cacheOnAccess = true)
    //
    //          case _: IOAction.DecompressAction =>
    //            IOStrategy.ConcurrentIO(cacheOnAccess = false)
    //        },
    //        compressions = _ => Seq.empty
    //      )

    val hashIndexConfig = HashIndexBlock.Config.disabled

    val bloomFilterConfig =
      BloomFilterBlock.Config.disabled

    //        BloomFilterBlock.Config(
    //          falsePositiveRate = 0.001,
    //          minimumNumberOfKeys = 2,
    //          optimalMaxProbe = _ => 1,
    //          blockIO = _ => IOStrategy.SynchronisedIO(cacheOnAccess = true),
    //          compressions = _ => Seq.empty
    //      )

    val segmentConfig =
      SegmentBlock.Config.applyInternal(
        fileOpenIOStrategy = IOStrategy.SynchronisedIO(cacheOnAccess = true),
        blockIOStrategy = {
          case _: IOAction.ReadDataOverview =>
            IOStrategy.ConcurrentIO(cacheOnAccess = true)

          case _: IOAction.DecompressAction =>
            IOStrategy.ConcurrentIO(cacheOnAccess = false)
        },
        cacheBlocksOnCreate = false,
        minSize = Int.MaxValue,
        maxCount =
          //                    keyValuesCount / 100,
          keyValuesCount,
        segmentRefCacheWeight = 100.bytes,
        enableHashIndexForListSegment = true,
        pushForward = PushForwardStrategy.Off,
        mmap = mmapSegments,
        deleteDelay = 0.seconds,
        //              compressions = _ => Seq(CompressionInternal.randomLZ4(Double.MinValue))
        compressions = _ => Seq.empty
      )

    implicit val fileSweeper = TestSweeper.createFileSweeper()
    implicit val byteBufferSweeper = TestSweeper.createBufferCleaner()
    implicit val forceSaveApplier: ForceSaveApplier = ForceSaveApplier.On
    //        implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = TestSweeper.someMemorySweeperMax
    //      implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = TestSweeper.someMemorySweeper10
    implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] = None

    implicit val state: Option[MemorySweeper.Block] =
      Some(MemorySweeper.BlockSweeper(blockSize = 4098.bytes, cacheSize = 1.gb, skipBlockCacheSeekSize = 1.mb, disableForSearchIO = false, actorConfig = None))
    //      None

    implicit val segmentIO: SegmentReadIO =
      SegmentReadIO(
        bloomFilterConfig = bloomFilterConfig,
        hashIndexConfig = hashIndexConfig,
        binarySearchIndexConfig = binarySearchIndexConfig,
        sortedIndexConfig = sortedIndexConfig,
        valuesConfig = valuesConfig,
        segmentConfig = segmentConfig
      )

    val segments =
      Benchmark(s"Creating segment. keyValues: ${keyValues.size}") {
        Segment.persistent(
          pathsDistributor = pathsDistributor,
          createdInLevel = 1,
          bloomFilterConfig = bloomFilterConfig,
          hashIndexConfig = hashIndexConfig,
          binarySearchIndexConfig = binarySearchIndexConfig,
          sortedIndexConfig = sortedIndexConfig,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          mergeStats =
            MergeStats
              .persistentBuilder(keyValues)
              .close(
                hasAccessPositionIndex = sortedIndexConfig.enableAccessPositionIndex,
                optimiseForReverseIteration = sortedIndexConfig.optimiseForReverseIteration
              )
        )
      }

    segments should have size 1
    //    segments.head.getClass.getSimpleName shouldBe "PersistentSegmentMany"
    segments.head
  }


  //  def reopenSegment() = {
  //    println("Re-opening Segment")
  //    segment.close()
  //    segment.clearAllCaches()
  //    segment =
  //      Segment(
  //        path = segment.path,
  //        segmentId = segment.segmentId,
  //        mmapReads = levelStorage.mmapSegmentsOnRead,
  //        mmapWrites = levelStorage.mmapSegmentsOnWrite,
  //        blockCacheFileId = BlockCacheFileIDGenerator.nextID,
  //        minKey = segment.minKey,
  //        maxKey = segment.maxKey,
  //        segmentSize = segment.segmentSize,
  //        nearestExpiryDeadline = segment.nearestExpiryDeadline,
  //        minMaxFunctionId = segment.minMaxFunctionId
  //      )
  //  }

  "Get" in {
    TestCaseSweeper {
      implicit sweeper =>

        def assertGet(segment: Segment) = {
          val readState = ThreadReadState.limitHashMap(1)
          shuffledKeyValues foreach {
            keyValue =>
              //        if (keyValue.key.readInt() % 1000 == 0)
              //          segment.get(shuffledKeyValues.head.key, readState)
              //
              //        val key = keyValue.key.readInt()
              ////        if (key % 1000 == 0)
              //          println(key)
              //        val found = segment.get(keyValue.key).get.get
              //        found.getOrFetchValue
              //            segment.get(keyValue.key, readState).getUnsafe.key shouldBe keyValue.key
              segment.get(keyValue.key, readState).getUnsafe
          }
        }


        val segment = initSegment().sweep()

        //    val all = segment.getAll()

        //    println(s"PrefixedCompressed     count: ${keyValues.count(_.isPrefixCompressed)}")
        //    println(s"not PrefixedCompressed count: ${keyValues.count(!_.isPrefixCompressed)}")
        //    println

        //        segment.asInstanceOf[PersistentSegmentOne].ref.segmentBlockCache.getHashIndex() foreach {
        //          hashIndex =>
        //            println(s"hashIndex.hit: ${hashIndex.hit}")
        //            println(s"hashIndex.miss: ${hashIndex.miss}")
        //            println(s"hashIndex.size: ${hashIndex.offset.size}")
        //            println
        //        }
        //
        //        segment.asInstanceOf[PersistentSegmentOne].ref.segmentBlockCache.getBinarySearchIndex() foreach {
        //          binarySearch =>
        //            println(s"binarySearch.valuesCount: ${binarySearch.valuesCount}")
        //            println(s"binarySearch.bytesPerValue: ${binarySearch.bytesPerValue}")
        //            println(s"binarySearch.isFullIndex: ${binarySearch.isFullIndex}")
        //            println(s"binarySearch.size: ${binarySearch.offset.size}")
        //            println
        //        }

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

        Benchmark(s"value ${keyValues.size} key values when ${segment.getClass.getSimpleName}, mmapSegmentWrites = ${mmapSegments.mmapWrites}, mmapSegmentReads = ${mmapSegments.mmapReads}") {
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

        //        Benchmark(s"value ${keyValues.size} key values ${segment.getClass.getSimpleName}, mmapSegmentWrites = ${mmapSegments.mmapWrites}, mmapSegmentReads = ${mmapSegments.mmapReads}") {
        //          assertGet(segment)
        //        }

        def printStats() = {

          //      println("seqSeeks: " + SegmentSearcher.seqSeeks)
          //      println("successfulSeqSeeks: " + SegmentSearcher.successfulSeqSeeks)
          //      println("failedSeqSeeks: " + SegmentSearcher.failedSeqSeeks)
          //      println
          //      //
          //      println("hashIndexSeeks: " + SegmentSearcher.hashIndexSeeks)
          //      println("successfulHashIndexSeeks: " + SegmentSearcher.successfulHashIndexSeeks)
          //      println("failedHashIndexSeeks: " + SegmentSearcher.failedHashIndexSeeks)
          //      println

          //          println("binarySeeks: " + BinarySearchIndexBlock.binarySeeks)
          //          println("binarySuccessfulDirectSeeks: " + BinarySearchIndexBlock.binarySuccessfulDirectSeeks)
          //          println("binarySuccessfulSeeksWithWalkForward: " + BinarySearchIndexBlock.binarySuccessfulSeeksWithWalkForward)
          //          println("binaryFailedSeeks: " + BinarySearchIndexBlock.binaryFailedSeeks)
          //          println("failedWithLower: " + BinarySearchIndexBlock.failedWithLower)
          //          println("greaterLower: " + BinarySearchIndexBlock.greaterLower)
          //          println("sameLower: " + BinarySearchIndexBlock.sameLower)
          //          println("Total hops: " + BinarySearchIndexBlock.totalHops)
          //          println("maxHops: " + BinarySearchIndexBlock.maxHop)
          //          println("minHop: " + BinarySearchIndexBlock.minHop)
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
      //    Benchmark(s"value ${keyValues.size} key values ${segment.getClass.getSimpleName}, mmapSegmentWrites = ${mmapSegments.mmapWrites}, mmapSegmentReads = ${mmapSegments.mmapReads}") {
      //      assertGet(segment)
      //      //      segment.getAll().get
      //    }
      //
      //    printStats()


    }
  }

  "Higher" in {
    TestCaseSweeper {
      implicit sweeper =>
        def assertHigher(segment: Segment) = {
          val readState = ThreadReadState.hashMap()
          (0 until keyValues.size - 1) foreach {
            index =>
              //        segment.higherKey(keyValues(index).key)
              //        println(s"index: $index")
              val keyValue = keyValues(index)

              //        if (keyValue.key.readInt() % 1000 == 0) {
              //          val key = keyValues(randomIntMax(index)).key
              //          if (randomBoolean())
              //            segment.higher(key, readState).getUnsafe
              //          else if (randomBoolean())
              //            segment.lower(key, readState).getUnsafe
              //          else
              //            segment.get(key, readState).getUnsafe.key shouldBe key
              //        }

              //        val expectedHigher = unGroupedKeyValues(index + 1)
              //        segment.higher(keyValue.key).get.get shouldBe expectedHigher
              segment.higher(keyValue.key, readState).getUnsafe
          }
        }

        val segment = initSegment().sweep()

        //    if (persistent) reopenSegment()
        Benchmark(s"higher ${keyValues.size} higher keys ${segment.getClass.getSimpleName}, mmapSegmentWrites = ${mmapSegments.mmapWrites}, mmapSegmentReads = ${mmapSegments.mmapReads}") {
          assertHigher(segment)
        }

        Benchmark(s"higher ${keyValues.size} higher keys ${segment.getClass.getSimpleName}, mmapSegmentWrites = ${mmapSegments.mmapWrites}, mmapSegmentReads = ${mmapSegments.mmapReads}") {
          assertHigher(segment)
        }

        Benchmark(s"higher ${keyValues.size} higher keys ${segment.getClass.getSimpleName}, mmapSegmentWrites = ${mmapSegments.mmapWrites}, mmapSegmentReads = ${mmapSegments.mmapReads}") {
          assertHigher(segment)
        }
    }
  }

  "Lower" in {
    TestCaseSweeper {
      implicit sweeper =>

        val range = (1 until keyValues.size).reverse

        def assertLower(segment: Segment) = {
          val readState = ThreadReadState.hashMap()
          range foreach {
            index =>
              //        println(s"index: $index")
              //        segment.lowerKeyValue(keyValues(index).key)

              val keyValue = keyValues(index)

              //        if (keyValue.key.readInt() % 1000 == 0) {
              //          val key = keyValues(randomIntMax(index)).key
              //          if (randomBoolean())
              //            segment.higher(key, readState).getUnsafe
              //          else if (randomBoolean())
              //            segment.lower(key, readState).getUnsafe
              //          else
              //            segment.get(key, readState).getUnsafe.key shouldBe key
              //        }

              //        println
              //        println(s"Key: ${keyValue.key.readInt()}")

              //        if (keyValue.key.readInt() % 1000 == 0)
              //          segment.higher(keyValues(randomIntMax(index)).key, readState)

              //        val expectedLower = unGroupedKeyValues(index - 1)
              //        segment.lower(keyValue.key).value.get shouldBe expectedLower
              segment.lower(keyValue.key, readState).getUnsafe
          }
        }

        val segment = initSegment().sweep()
        //    if (persistent) reopenSegment()
        Benchmark(s"lower ${keyValues.size} lower keys ${segment.getClass.getSimpleName}, mmapSegmentWrites = ${mmapSegments.mmapWrites}, mmapSegmentReads = ${mmapSegments.mmapReads}") {
          assertLower(segment)
        }

        Benchmark(s"lower ${keyValues.size} lower keys ${segment.getClass.getSimpleName}, mmapSegmentWrites = ${mmapSegments.mmapWrites}, mmapSegmentReads = ${mmapSegments.mmapReads}") {
          assertLower(segment)
        }

        Benchmark(s"lower ${keyValues.size} lower keys ${segment.getClass.getSimpleName}, mmapSegmentWrites = ${mmapSegments.mmapWrites}, mmapSegmentReads = ${mmapSegments.mmapReads}") {
          assertLower(segment)
        }
    }
  }
}
