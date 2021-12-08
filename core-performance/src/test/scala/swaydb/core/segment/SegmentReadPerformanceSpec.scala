/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.segment

import swaydb.Benchmark
import swaydb.config.SegmentRefCacheLife
import swaydb.core.TestCaseSweeper._
import swaydb.core.CoreTestData._
import swaydb.core.file.ForceSaveApplier
import swaydb.core.segment.PathsDistributor
import swaydb.core.segment.block.binarysearch.{BinarySearchEntryFormat, BinarySearchIndexBlockConfig}
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.HashIndexBlockConfig
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.segment.cache.sweeper.MemorySweeper
import swaydb.core.segment.data.Memory
import swaydb.core.segment.data.merge.stats.MergeStats
import swaydb.core.segment.entry.reader.PersistentReader
import swaydb.core.segment.io.SegmentReadIO
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.{ACoreSpec, TestCaseSweeper, TestExecutionContext, TestSweeper}
import swaydb.effect.{Dir, IOAction, IOStrategy}
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.testkit.RunThis.FutureImplicits
import swaydb.utils.StorageUnits._

import java.nio.file.Path
import scala.concurrent.duration._
import scala.util.Random

class SegmentReadPerformanceSpec extends ASegmentSpec {

  implicit val keyOrder = KeyOrder.default
  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val ec = TestExecutionContext.executionContext

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
      SortedIndexBlockConfig(
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
      BinarySearchIndexBlockConfig(
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
    //      BinarySearchIndexConfig.disabled

    val valuesConfig =
      ValuesBlockConfig(
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
    //      HashIndexConfig(
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

    val hashIndexConfig = HashIndexBlockConfig.disabled()

    val bloomFilterConfig =
      BloomFilterBlockConfig.disabled()

    //        BloomFilterConfig(
    //          falsePositiveRate = 0.001,
    //          minimumNumberOfKeys = 2,
    //          optimalMaxProbe = _ => 1,
    //          blockIO = _ => IOStrategy.SynchronisedIO(cacheOnAccess = true),
    //          compressions = _ => Seq.empty
    //      )

    val segmentConfig =
      SegmentBlockConfig.applyInternal(
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
        segmentRefCacheLife = SegmentRefCacheLife.Temporary,
        enableHashIndexForListSegment = true,
        mmap = mmapSegments,
        deleteDelay = 0.seconds,
        //              compressions = _ => Seq(CompressionInternal.randomLZ4(Double.MinValue))
        compressions = _ => Seq.empty,
        initialiseIteratorsInOneSeek = false
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
        ).awaitInf
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
        //    val file = CoreFile.mmapRead(segment.path, randomIOStrategy(false), true).get
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
