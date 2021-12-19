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

package swaydb.core.segment.block

import org.scalatest.matchers.should.Matchers._
import org.scalatest.wordspec.AnyWordSpec
import swaydb.{Benchmark, IO}
import swaydb.config.CoreConfigTestKit._
import swaydb.config.MMAP
import swaydb.core.{CoreSpecType, CoreTestSweeper}
import swaydb.core.compression.CompressionTestKit._
import swaydb.core.log.timer.TestTimer
import swaydb.core.segment.{PersistentSegmentMany, PersistentSegmentOne, GenSegment}
import swaydb.core.segment.block.binarysearch.BinarySearchIndexBlockConfig
import swaydb.core.segment.block.bloomfilter.BloomFilterBlockConfig
import swaydb.core.segment.block.hashindex.{HashIndexBlock, HashIndexBlockConfig}
import swaydb.core.segment.block.segment.SegmentBlockConfig
import swaydb.core.segment.block.sortedindex.SortedIndexBlockConfig
import swaydb.core.segment.block.values.ValuesBlockConfig
import swaydb.core.segment.block.SegmentBlockTestKit._
import swaydb.core.segment.data._
import swaydb.core.segment.data.KeyValueTestKit._
import swaydb.core.segment.ref.search.SegmentSearchTestKit._
import swaydb.core.segment.SegmentTestKit._
import swaydb.effect.EffectTestKit._
import swaydb.effect.IOStrategy
import swaydb.serializers._
import swaydb.serializers.Default._
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.testkit.RunThis._
import swaydb.testkit.TestKit._
import swaydb.utils.StorageUnits._

import scala.util.Random

class SegmentBlockInitialisationSpec extends AnyWordSpec {

  val keyValueCount = 100

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default

  implicit def testTimer: TestTimer = TestTimer.random

  "BinarySearchIndex" should {
    "not be created" when {
      "disabled" in {
        runThis(10.times) {
          CoreTestSweeper {
            implicit sweeper =>
              import sweeper._

              val keyValues: Slice[Memory] =
                randomizedKeyValues(
                  count = 100,
                  startId = Some(1)
                )

              val blocks =
                getSegmentBlockCacheSingle(
                  keyValues = keyValues,
                  binarySearchIndexConfig =
                    BinarySearchIndexBlockConfig(
                      enabled = false,
                      format = randomBinarySearchFormat(),
                      minimumNumberOfKeys = 0,
                      fullIndex = randomBoolean(),
                      searchSortedIndexDirectlyIfPossible = randomBoolean(),
                      ioStrategy = _ => IOStrategy.ConcurrentIO(true),
                      compressions = _ => randomCompressionsOrEmpty()
                    )
                )
              blocks.createBinarySearchIndexReaderOrNull() shouldBe null
              blocks.binarySearchIndexReaderCacheOrNull.isCached shouldBe true
              blocks.binarySearchIndexReaderCacheOrNull.state() shouldBe Some(IO(null))
              blocks.createBinarySearchIndexReaderOrNull() shouldBe null
          }
        }
      }

      "minimumNumberOfKeys is not met" in {
        runThis(10.times) {
          CoreTestSweeper {
            implicit sweeper =>
              import sweeper._

              val generatedKeyValues =
                randomizedKeyValues(
                  count = 100,
                  startId = Some(1)
                )

              val keyValues: Slice[Memory] = generatedKeyValues

              val blocks =
                getBlocksSingle(
                  keyValues = keyValues,
                  binarySearchIndexConfig =
                    BinarySearchIndexBlockConfig(
                      enabled = true,
                      format = randomBinarySearchFormat(),
                      searchSortedIndexDirectlyIfPossible = randomBoolean(),
                      minimumNumberOfKeys = generatedKeyValues.size + 1,
                      fullIndex = randomBoolean(),
                      ioStrategy = _ => genIOAccess(),
                      compressions = _ => randomCompressionsOrEmpty()
                    )
                ).get

              blocks.binarySearchIndexReader shouldBe empty
          }
        }
      }
    }

    "partially created" when {
      "hashIndex is not perfect" in {
        runThis(1.times) {
          CoreTestSweeper {
            implicit sweeper =>
              import sweeper._

              val compressions = randomCompressionsOrEmpty()

              val keyValues: Slice[Memory] =
                randomizedKeyValues(
                  count = 1000,
                  startId = Some(1)
                  //              addRanges = true,
                  //              addUpdates = false,
                  //              addRemoves = false,
                  //              addFunctions = false,
                  //              addPendingApply = false
                )

              keyValues should not be empty

              val blocks =
                getBlocksSingle(
                  keyValues = keyValues,
                  sortedIndexConfig =
                    SortedIndexBlockConfig.random.copy(
                      shouldPrefixCompress = _ => randomBoolean(),
                      enablePrefixCompression = false,
                      normaliseIndex = false
                    ),
                  binarySearchIndexConfig =
                    BinarySearchIndexBlockConfig(
                      enabled = true,
                      format = randomBinarySearchFormat(),
                      searchSortedIndexDirectlyIfPossible = randomBoolean(),
                      minimumNumberOfKeys = 0,
                      fullIndex = false,
                      ioStrategy = _ => genIOAccess(),
                      compressions = _ => compressions
                    ),
                  hashIndexConfig =
                    HashIndexBlockConfig(
                      maxProbe = 1,
                      allocateSpace = _.requiredSpace,
                      format = randomHashIndexSearchFormat(),
                      minimumNumberOfKeys = 0,
                      minimumNumberOfHits = 0,
                      ioStrategy = _ => genIOAccess(),
                      compressions = _ => compressions
                    )
                ).get

              blocks.hashIndexReader.get.block.hit should be > 0
              blocks.hashIndexReader.get.block.hit should be < keyValues.size
              blocks.hashIndexReader.get.block.miss should be > 0
              blocks.hashIndexReader.get.block.miss should be < keyValues.size

              blocks.binarySearchIndexReader.get.block.valuesCount shouldBe blocks.hashIndexReader.get.block.miss
          }
        }
      }
    }

    "fully be created" when {
      "perfect hashIndex" in {
        runThis(10.times) {
          CoreTestSweeper {
            implicit sweeper =>
              import sweeper._

              val compressions = randomCompressionsOrEmpty()

              val keyValues: Slice[Memory] =
                randomizedKeyValues(
                  count = 100,
                  startId = Some(1)
                  //              addPut = true,
                  //              addRanges = true,
                  //              addUpdates = false,
                  //              addRemoves = false,
                  //              addFunctions = false,
                  //              addPendingApply = false
                )

              val blocks =
                getBlocksSingle(
                  keyValues,
                  binarySearchIndexConfig =
                    BinarySearchIndexBlockConfig(
                      enabled = true,
                      format = randomBinarySearchFormat(),
                      searchSortedIndexDirectlyIfPossible = false,
                      minimumNumberOfKeys = 0,
                      fullIndex = true,
                      ioStrategy = _ => genIOAccess(),
                      compressions = _ => compressions
                    ),
                  hashIndexConfig =
                    HashIndexBlockConfig(
                      maxProbe = 100,
                      minimumNumberOfKeys = 0,
                      minimumNumberOfHits = 0,
                      format = randomHashIndexSearchFormat(),
                      allocateSpace = _.requiredSpace * 20,
                      ioStrategy = _ => genIOAccess(),
                      compressions = _ => compressions
                    ),
                  sortedIndexConfig =
                    SortedIndexBlockConfig.random.copy(
                      shouldPrefixCompress = _ => randomBoolean(),
                      enablePrefixCompression = false,
                      normaliseIndex = false
                    )
                ).get

              blocks.hashIndexReader shouldBe defined
              blocks.hashIndexReader.get.block.hit shouldBe keyValues.size
              blocks.hashIndexReader.get.block.miss shouldBe 0

              blocks.binarySearchIndexReader.get.block.valuesCount shouldBe keyValues.size
          }
        }
      }

      //      "for partial binary search index when hashIndex is completely disabled" in {
      //        runThis(10.times) {
      //          val compressions = randomCompressionsOrEmpty()
      //
      //          val keyValues: Slice[Memory] =
      //            randomizedKeyValues(
      //              count = 100,
      //              startId = Some(1),
      //              //              addPut = true,
      //              //              addRanges = true,
      //              //              addUpdates = false,
      //              //              addRemoves = false,
      //              //              addFunctions = false,
      //              //              addPendingApply = false
      //            )
      //
      //          val blocks =
      //            getBlocksSingle(
      //              keyValues = keyValues,
      //              binarySearchIndexConfig =
      //                BinarySearchIndexConfig(
      //                  enabled = true,
      //                  format = randomBinarySearchFormat(),
      //                  searchSortedIndexDirectlyIfPossible = randomBoolean(),
      //                  minimumNumberOfKeys = 0,
      //                  fullIndex = false,
      //                  ioStrategy = _ => genIOAccess(),
      //                  compressions = _ => compressions
      //                ),
      //              hashIndexConfig =
      //                HashIndexConfig(
      //                  maxProbe = 5,
      //                  minimumNumberOfKeys = Int.MaxValue,
      //                  minimumNumberOfHits = 0,
      //                  format = randomHashIndexSearchFormat(),
      //                  allocateSpace = _.requiredSpace * 0,
      //                  ioStrategy = _ => genIOAccess(),
      //                  compressions = _ => compressions
      //                )
      //            ).get
      //
      //          blocks.hashIndexReader shouldBe empty
      //
      ////          if (!blocks.sortedIndexReader.block.isBinarySearchable) {
      ////            blocks.binarySearchIndexReader shouldBe defined
      ////            val expectedBinarySearchValuesCount =
      ////              keyValues
      ////                .count {
      ////                  range =>
      ////                    range.previous.forall(_.stats.segmentAccessIndexOffset != range.stats.segmentAccessIndexOffset)
      ////                }
      ////
      ////            blocks.binarySearchIndexReader.get.block.valuesCount shouldBe expectedBinarySearchValuesCount
      ////          }
      //        }
      //      }
    }
  }

  "bloomFilter" should {
    "not be created" when {
      "falsePositive is high" in {
        runThis(10.times) {
          CoreTestSweeper {
            implicit sweeper =>
              import sweeper._

              val keyValues: Slice[Memory] =
                randomizedKeyValues(
                  count = 100,
                  addPut = true,
                  startId = Some(1)
                )

              val blocks =
                getBlocksSingle(
                  keyValues = keyValues,
                  bloomFilterConfig =
                    BloomFilterBlockConfig(
                      falsePositiveRate = 1,
                      minimumNumberOfKeys = 0,
                      optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
                      ioStrategy = _ => genIOAccess(),
                      compressions = _ => randomCompressionsOrEmpty()
                    )
                ).get

              blocks.bloomFilterReader shouldBe empty
          }
        }
      }

      "minimum number of key is not met" in {
        runThis(10.times) {
          CoreTestSweeper {
            implicit sweeper =>
              import sweeper._

              val keyValues =
              //do not use randomised because it will generate remove ranges
                randomPutKeyValues(
                  count = 100,
                  startId = Some(1)
                )

              val blocks =
                getBlocksSingle(
                  keyValues,
                  bloomFilterConfig =
                    BloomFilterBlockConfig(
                      falsePositiveRate = 0.001,
                      optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
                      minimumNumberOfKeys = keyValues.size + 1,
                      ioStrategy = _ => genIOAccess(),
                      compressions = _ => randomCompressionsOrEmpty()
                    )
                ).get

              blocks.bloomFilterReader shouldBe empty
          }
        }
      }

      "key-values contain remove ranges" in {
        runThis(10.times) {
          CoreTestSweeper {
            implicit sweeper =>
              import sweeper._

              val keyValues: Slice[Memory] =
                randomPutKeyValues(
                  count = 100,
                  startId = Some(100)
                )

              val range: Slice[Memory.Range] =
                Slice(
                  Memory.Range(
                    fromKey = 0,
                    toKey = 1,
                    fromValue = randomFromValueOption(),
                    rangeValue =
                      eitherOne(
                        randomFunctionValue(),
                        Value.Remove(randomDeadlineOption(), Time.empty),
                        Value.PendingApply(
                          eitherOne(
                            Slice(randomFunctionValue()),
                            Slice(Value.Remove(randomDeadlineOption(), Time.empty)),
                            Slice(
                              randomFunctionValue(),
                              Value.Remove(randomDeadlineOption(), Time.empty)
                            )
                          )
                        )
                      )
                  )
                )

              val allKeyValues = range ++ keyValues

              allKeyValues.exists(_.mightContainRemoveRange) shouldBe true

              val blocks =
                getBlocksSingle(
                  allKeyValues,
                  bloomFilterConfig =
                    BloomFilterBlockConfig(
                      falsePositiveRate = 0.001,
                      minimumNumberOfKeys = 0,
                      optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
                      ioStrategy = _ => genIOAccess(),
                      compressions = _ => randomCompressionsOrEmpty()
                    )
                ).get

              blocks.bloomFilterReader shouldBe empty
          }
        }
      }
    }
  }

  "hashIndex" should {
    "not be created if minimum number is not met" in {
      runThis(10.times) {
        CoreTestSweeper {
          implicit sweeper =>
            import sweeper._

            val keyValues =
              randomizedKeyValues(
                count = 1000,
                addPut = true,
                startId = Some(1)
              )

            val blocks =
              getBlocksSingle(
                keyValues = keyValues,
                hashIndexConfig =
                  HashIndexBlockConfig(
                    maxProbe = 5,
                    minimumNumberOfKeys = 0, //set miminimum to be 10 for hash to be created.
                    minimumNumberOfHits = 10,
                    format = randomHashIndexSearchFormat(),
                    allocateSpace = _ =>
                      HashIndexBlock.optimalBytesRequired(
                        keyCounts = 1, //allocate space enough for 1
                        minimumNumberOfKeys = 0,
                        writeAbleLargestValueSize = Int.MaxValue,
                        allocateSpace = _.requiredSpace
                      ),
                    ioStrategy = _ => genIOAccess(),
                    compressions = _ => randomCompressionsOrEmpty()
                  )
              ).get

            blocks.hashIndexReader shouldBe empty
        }
      }
    }

    "cache everything" in {
      runThis(10.times, log = true) {
        CoreTestSweeper {
          implicit sweeper =>
            import sweeper._

            val compressions = Slice.fill(6)(randomCompressionsOrEmpty())

            val keyValues =
              Benchmark("Creating key-values", inlinePrint = true) {
                randomizedKeyValues(
                  count = 10000,
                  startId = Some(1),
                  addPut = true
                )
              }

            val cache =
              Benchmark("Creating cache", inlinePrint = true) {
                getSegmentBlockCacheSingle(
                  keyValues = keyValues,
                  valuesConfig =
                    ValuesBlockConfig(
                      compressDuplicateValues = randomBoolean(),
                      compressDuplicateRangeValues = randomBoolean(),
                      ioStrategy = _ => genIOAccess(cacheOnAccess = true),
                      compressions = _ => compressions.head
                    ),
                  sortedIndexConfig =
                    SortedIndexBlockConfig(
                      ioStrategy = _ => genIOAccess(cacheOnAccess = true),
                      shouldPrefixCompress = _ => randomBoolean(),
                      enablePrefixCompression = randomBoolean(),
                      optimiseForReverseIteration = randomBoolean(),
                      prefixCompressKeysOnly = randomBoolean(),
                      enableAccessPositionIndex = true,
                      normaliseIndex = randomBoolean(),
                      compressions = _ => compressions(1)
                    ),
                  binarySearchIndexConfig =
                    BinarySearchIndexBlockConfig(
                      enabled = true,
                      format = randomBinarySearchFormat(),
                      searchSortedIndexDirectlyIfPossible = randomBoolean(),
                      minimumNumberOfKeys = 1,
                      fullIndex = true,
                      ioStrategy = _ => genIOAccess(cacheOnAccess = true),
                      compressions = _ => compressions(2)
                    ),
                  hashIndexConfig =
                    HashIndexBlockConfig(
                      maxProbe = 5,
                      minimumNumberOfKeys = 2,
                      minimumNumberOfHits = 2,
                      format = randomHashIndexSearchFormat(),
                      allocateSpace = _.requiredSpace * 10,
                      ioStrategy = _ => genIOAccess(cacheOnAccess = true),
                      compressions = _ => compressions(3)
                    ),
                  bloomFilterConfig =
                    BloomFilterBlockConfig(
                      falsePositiveRate = 0.001,
                      minimumNumberOfKeys = 2,
                      optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
                      ioStrategy = _ => genIOAccess(cacheOnAccess = true),
                      compressions = _ => compressions(4)
                    ),
                  segmentConfig =
                    SegmentBlockConfig.applyInternal(
                      fileOpenIOStrategy = genThreadSafeIOStrategy(cacheOnAccess = true),
                      blockIOStrategy = _ => genIOAccess(cacheOnAccess = true),
                      cacheBlocksOnCreate = false,
                      minSize = randomIntMax(2.mb),
                      maxCount = randomIntMax(1000),
                      segmentRefCacheLife = randomSegmentRefCacheLife(),
                      enableHashIndexForListSegment = randomBoolean(),
                      mmap = MMAP.randomForSegment(),
                      deleteDelay = randomFiniteDuration(),
                      compressions = _ => compressions(4),
                      initialiseIteratorsInOneSeek = randomBoolean()
                    )
                )
              }

            cache.isCached shouldBe false

            cache.getFooter()
            cache.isCached shouldBe true
            cache.footerBlockCache.isCached shouldBe true
            cache.clear()
            cache.isCached shouldBe false
            cache.footerBlockCache.isCached shouldBe false

            cache.getBinarySearchIndex()
            cache.getSortedIndex()
            cache.getBloomFilter()
            cache.getHashIndex()
            cache.getValues()

            cache.binarySearchIndexBlockCache.isCached shouldBe true
            cache.sortedIndexBlockCache.isCached shouldBe true
            cache.bloomFilterBlockCache.isCached shouldBe true
            cache.hashIndexBlockCache.isCached shouldBe true
            cache.valuesBlockCache.isCached shouldBe true

            cache.segmentReaderCache.isCached shouldBe true

            cache.binarySearchIndexReaderCacheOrNull.isCached shouldBe false
            cache.sortedIndexReaderCache.isCached shouldBe false
            cache.bloomFilterReaderCacheOrNull.isCached shouldBe false
            cache.hashIndexReaderCacheOrNull.isCached shouldBe false
            cache.valuesReaderCacheOrNull.isCached shouldBe false

            cache.clear()
            cache.isCached shouldBe false
        }
      }
    }
  }

  "cacheBlocksOnCreate" when {
    "false" should {
      "not cache bytes" in {
        CoreTestSweeper {
          implicit sweeper =>
            import sweeper._

            val keyValues = randomKeyValues(100)

            val segmentBlockCache =
              getSegmentBlockCacheSingle(
                keyValues = keyValues,
                segmentConfig = SegmentBlockConfig.random(hasCompression = randomBoolean(), cacheBlocksOnCreate = false)
              )

            segmentBlockCache.isCached shouldBe false
        }
      }
    }

    "true" should {
      "cache blocks on initialisation and maintain reads on clear" in {
        runThis(100.times) {
          CoreTestSweeper {
            implicit sweeper =>
              import sweeper._

              implicit val coreSpecType: CoreSpecType = CoreSpecType.random()

              val keyValues = randomKeyValues(100)

              val segment =
                GenSegment(
                  keyValues = keyValues,
                  segmentConfig = SegmentBlockConfig.random(hasCompression = randomBoolean(), cacheBlocksOnCreate = true, minSegmentSize = Int.MaxValue)
                )

              val refs =
                segment match {
                  case segment: PersistentSegmentMany =>
                    segment.segmentRefs(randomBoolean()).toList

                  case segment: PersistentSegmentOne =>
                    List(segment.ref)
                }

              refs foreach {
                ref =>
                  val blockCache = ref.segmentBlockCache

                  blockCache.isCached shouldBe true

                  assertReads(keyValues, segment)

                  //randomly clear caches and reads for result in the same output
                  Random.shuffle(blockCache.allCaches) foreach {
                    cache =>
                      if (randomBoolean())
                        cache.clear()
                  }

                  assertReads(keyValues, segment)

                  blockCache.clear()
                  blockCache.isCached shouldBe false

                  assertReads(keyValues, segment)
              }
          }
        }
      }
    }
  }
}
