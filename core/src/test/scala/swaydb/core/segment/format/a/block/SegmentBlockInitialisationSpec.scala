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

package swaydb.core.segment.format.a.block

import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.segment.format.a.block.binarysearch.BinarySearchIndexBlock
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.config.{IOStrategy, UncompressedBlockInfo}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Default._
import swaydb.serializers._

class SegmentBlockInitialisationSpec extends TestBase {

  val keyValueCount = 100

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val memorySweeper = TestLimitQueues.memorySweeper

  implicit def testTimer: TestTimer = TestTimer.random

  "BinarySearchIndex" should {
    "not be created" when {
      "disabled" in {
        runThis(10.times) {
          val keyValues: Slice[Transient] =
            randomizedKeyValues(
              count = 100,
              addPut = true,
              startId = Some(1)
            ).updateStats(
              binarySearchIndexConfig =
                BinarySearchIndexBlock.Config(
                  enabled = false,
                  minimumNumberOfKeys = 0,
                  fullIndex = randomBoolean(),
                  searchSortedIndexDirectlyIfPossible = randomBoolean(),
                  ioStrategy = _ => IOStrategy.defaultBlockReadersStored,
                  compressions = _ => randomCompressionsOrEmpty()
                )
            )

          val blocks = getSegmentBlockCache(keyValues)
          blocks.createBinarySearchIndexReader().get shouldBe empty
          blocks.binarySearchIndexReaderCache.isCached shouldBe true
          blocks.binarySearchIndexReaderCache.getIO() shouldBe Some(IO.none)
          blocks.createBinarySearchIndexReader().get shouldBe empty
        }
      }

      "minimumNumberOfKeys is not met" in {
        runThis(10.times) {
          val generatedKeyValues =
            randomizedKeyValues(
              count = 100,
              addPut = true,
              startId = Some(1)
            )

          val keyValues: Slice[Transient] =
            generatedKeyValues
              .updateStats(
                binarySearchIndexConfig =
                  BinarySearchIndexBlock.Config(
                    enabled = true,
                    searchSortedIndexDirectlyIfPossible = randomBoolean(),
                    minimumNumberOfKeys = generatedKeyValues.size + 1,
                    fullIndex = randomBoolean(),
                    ioStrategy = _ => randomIOAccess(),
                    compressions = _ => randomCompressionsOrEmpty()
                  )
              )

          val blocks = getBlocks(keyValues).get
          blocks.binarySearchIndexReader shouldBe empty
        }
      }
    }

    "partially created for ranges" when {
      "perfect hashIndex" in {
        runThis(100.times) {
          val compressions = randomCompressionsOrEmpty()

          val keyValues: Slice[Transient] =
            randomizedKeyValues(
              count = 1000,
              startId = Some(1),
              addPut = true,
              addRanges = true,
              addUpdates = false,
              addRemoves = false,
              addFunctions = false,
              addPendingApply = false
            ).updateStats(
              binarySearchIndexConfig =
                BinarySearchIndexBlock.Config(
                  enabled = true,
                  searchSortedIndexDirectlyIfPossible = randomBoolean(),
                  minimumNumberOfKeys = 0,
                  fullIndex = false,
                  ioStrategy = _ => randomIOAccess(),
                  compressions = _ => compressions
                ),
              hashIndexConfig =
                HashIndexBlock.Config(
                  maxProbe = 5,
                  allocateSpace = _.requiredSpace * 30,
                  copyIndex = randomBoolean(),
                  minimumNumberOfKeys = 0,
                  minimumNumberOfHits = 0,
                  ioStrategy = _ => randomIOAccess(),
                  compressions = _ => compressions
                )
            )

          keyValues should not be empty

          val blocks = getBlocks(keyValues).get
          blocks.hashIndexReader shouldBe defined
          blocks.hashIndexReader.get.block.hit shouldBe keyValues.size
          blocks.hashIndexReader.get.block.miss shouldBe 0

          if (keyValues.last.stats.segmentTotalNumberOfRanges > 0 && !blocks.sortedIndexReader.block.isNormalisedBinarySearchable) {
            val expectedBinarySearchValuesCount = keyValues.count(keyValue => keyValue.isRange && !keyValue.isPrefixCompressed)
            if (expectedBinarySearchValuesCount > 0) { //if all ranges were prefix compressed then binary search index is not created.
              blocks.binarySearchIndexReader shouldBe defined
              blocks.binarySearchIndexReader.get.block.valuesCount shouldBe expectedBinarySearchValuesCount
            }
          } else {
            blocks.binarySearchIndexReader shouldBe empty
          }
        }
      }
    }

    "fully be created" when {
      "perfect hashIndex" in {
        runThis(10.times) {
          val compressions = randomCompressionsOrEmpty()

          val keyValues: Slice[Transient] =
            randomizedKeyValues(
              count = 100,
              startId = Some(1),
              addPut = true,
              addRanges = true,
              addUpdates = false,
              addRemoves = false,
              addFunctions = false,
              addPendingApply = false
            ).updateStats(
              binarySearchIndexConfig =
                BinarySearchIndexBlock.Config(
                  enabled = true,
                  searchSortedIndexDirectlyIfPossible = randomBoolean(),
                  minimumNumberOfKeys = 0,
                  fullIndex = true,
                  ioStrategy = _ => randomIOAccess(),
                  compressions = _ => compressions
                ),
              hashIndexConfig =
                HashIndexBlock.Config(
                  maxProbe = 100,
                  minimumNumberOfKeys = 0,
                  minimumNumberOfHits = 0,
                  copyIndex = randomBoolean(),
                  allocateSpace = _.requiredSpace * 20,
                  ioStrategy = _ => randomIOAccess(),
                  compressions = _ => compressions
                )
            )

          val blocks = getBlocks(keyValues).get
          blocks.hashIndexReader shouldBe defined
          blocks.hashIndexReader.get.block.hit shouldBe keyValues.size
          blocks.hashIndexReader.get.block.miss shouldBe 0

          val expectedBinarySearchValuesCount = keyValues.count(keyValue => !keyValue.isPrefixCompressed)
          //if all ranges were prefix compressed then binary search index is not created.
          if (expectedBinarySearchValuesCount > 0 && !blocks.sortedIndexReader.block.isNormalisedBinarySearchable) {
            blocks.binarySearchIndexReader shouldBe defined
            blocks.binarySearchIndexReader.get.block.valuesCount shouldBe expectedBinarySearchValuesCount
          }
        }
      }

      "for partial binary search index when hashIndex is completely disabled" in {
        runThis(10.times) {
          val compressions = randomCompressionsOrEmpty()

          val keyValues: Slice[Transient] =
            randomizedKeyValues(
              count = 100,
              startId = Some(1),
              addPut = true,
              addRanges = true,
              addUpdates = false,
              addRemoves = false,
              addFunctions = false,
              addPendingApply = false
            ).updateStats(
              binarySearchIndexConfig =
                BinarySearchIndexBlock.Config(
                  enabled = true,
                  searchSortedIndexDirectlyIfPossible = randomBoolean(),
                  minimumNumberOfKeys = 0,
                  fullIndex = false,
                  ioStrategy = _ => randomIOAccess(),
                  compressions = _ => compressions
                ),
              hashIndexConfig =
                HashIndexBlock.Config(
                  maxProbe = 5,
                  minimumNumberOfKeys = Int.MaxValue,
                  minimumNumberOfHits = 0,
                  copyIndex = randomBoolean(),
                  allocateSpace = _.requiredSpace * 0,
                  ioStrategy = _ => randomIOAccess(),
                  compressions = _ => compressions
                )
            )

          val blocks = getBlocks(keyValues).get
          blocks.hashIndexReader shouldBe empty

          if (!blocks.sortedIndexReader.block.isNormalisedBinarySearchable) {
            blocks.binarySearchIndexReader shouldBe defined
            val expectedBinarySearchValuesCount =
              keyValues
                .count {
                  range =>
                    range.previous.forall(_.stats.thisKeyValuesAccessIndexOffset != range.stats.thisKeyValuesAccessIndexOffset)
                }
            blocks.binarySearchIndexReader.get.block.valuesCount shouldBe expectedBinarySearchValuesCount
          }
        }
      }
    }
  }

  "bloomFilter" should {
    "not be created" when {
      "falsePositive is high" in {
        runThis(10.times) {
          val keyValues: Slice[Transient] =
            randomizedKeyValues(
              count = 100,
              addPut = true,
              startId = Some(1)
            ).updateStats(
              bloomFilterConfig =
                BloomFilterBlock.Config(
                  falsePositiveRate = 1,
                  minimumNumberOfKeys = 0,
                  optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
                  blockIO = _ => randomIOAccess(),
                  compressions = _ => randomCompressionsOrEmpty()
                )
            )

          val blocks = getBlocks(keyValues).get
          blocks.bloomFilterReader shouldBe empty
        }
      }

      "minimum number of key is not met" in {
        runThis(10.times) {
          val generatedKeyValues =
          //do not use randomised because it will generate remove ranges
            randomPutKeyValues(
              count = 100,
              startId = Some(1)
            )

          val keyValues: Slice[Transient] =
            generatedKeyValues
              .toTransient(
                bloomFilterConfig =
                  BloomFilterBlock.Config(
                    falsePositiveRate = 0.001,
                    optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
                    minimumNumberOfKeys = generatedKeyValues.size + 1,
                    blockIO = _ => randomIOAccess(),
                    compressions = _ => randomCompressionsOrEmpty()
                  )
              )

          val blocks = getBlocks(keyValues).get
          blocks.bloomFilterReader shouldBe empty
        }
      }

      "key-values contain remove ranges" in {
        runThis(10.times) {
          val generatedKeyValues =
            randomPutKeyValues(
              count = 100,
              startId = Some(100)
            )

          val range =
            Memory.Range(
              fromKey = 0,
              toKey = 1,
              fromValue = randomFromValueOption(),
              rangeValue =
                eitherOne(
                  randomFunctionValue(),
                  Value.Remove(randomDeadlineOption, Time.empty),
                  Value.PendingApply(
                    eitherOne(
                      Slice(randomFunctionValue()),
                      Slice(Value.Remove(randomDeadlineOption, Time.empty)),
                      Slice(
                        randomFunctionValue(),
                        Value.Remove(randomDeadlineOption, Time.empty)
                      )
                    )
                  )
                )
            )

          val keyValues: Slice[Transient] =
            (Slice(range) ++ generatedKeyValues)
              .toTransient(
                bloomFilterConfig =
                  BloomFilterBlock.Config(
                    falsePositiveRate = 0.001,
                    minimumNumberOfKeys = 0,
                    optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
                    blockIO = _ => randomIOAccess(),
                    compressions = _ => randomCompressionsOrEmpty()
                  )
              )

          keyValues.last.stats.segmentHasRemoveRange shouldBe true

          val blocks = getBlocks(keyValues).get
          blocks.bloomFilterReader shouldBe empty
        }
      }
    }
  }

  "hashIndex" should {
    "not be created if minimum number is not met" in {
      runThis(10.times) {
        val generatedKeyValues =
          randomizedKeyValues(
            count = 1000,
            addPut = true,
            startId = Some(1)
          )

        val keyValues: Slice[Transient] =
          generatedKeyValues
            .updateStats(
              hashIndexConfig =
                HashIndexBlock.Config(
                  maxProbe = 5,
                  minimumNumberOfKeys = 0, //set miminimum to be 10 for hash to be created.
                  minimumNumberOfHits = 10,
                  copyIndex = randomBoolean(),
                  allocateSpace = _ =>
                    HashIndexBlock.optimalBytesRequired(
                      keyCounts = 1, //allocate space enough for 1
                      minimumNumberOfKeys = 0,
                      writeAbleLargestValueSize = Int.MaxValue,
                      hasCompression = generatedKeyValues.last.hashIndexConfig.compressions(UncompressedBlockInfo(randomIntMax(1.mb))).nonEmpty,
                      copyIndex = randomBoolean(),
                      allocateSpace = _.requiredSpace
                    ),
                  ioStrategy = _ => randomIOAccess(),
                  compressions = _ => randomCompressionsOrEmpty()
                )
            )

        val blocks = getBlocks(keyValues).get
        blocks.hashIndexReader shouldBe empty
      }
    }
  }

  "cache everything" in {
    runThis(10.times, log = true) {
      val compressions = Slice.fill(5)(randomCompressionsOrEmpty())

      val keyValues =
        randomizedKeyValues(
          count = 10000,
          startId = Some(1),
          addPut = true
        ).updateStats(
          valuesConfig =
            ValuesBlock.Config(
              compressDuplicateValues = randomBoolean(),
              compressDuplicateRangeValues = randomBoolean(),
              ioStrategy = _ => randomIOAccess(cacheOnAccess = true),
              compressions = _ => compressions.head
            ),
          sortedIndexConfig =
            SortedIndexBlock.Config(
              ioStrategy = _ => randomIOAccess(cacheOnAccess = true),
              prefixCompressionResetCount = 0,
              enableAccessPositionIndex = true,
              enablePartialRead = randomBoolean(),
              disableKeyPrefixCompression = randomBoolean(),
              normaliseIndex = randomBoolean(),
              compressions = _ => compressions(1)
            ),
          binarySearchIndexConfig =
            BinarySearchIndexBlock.Config(
              enabled = true,
              searchSortedIndexDirectlyIfPossible = randomBoolean(),
              minimumNumberOfKeys = 1,
              fullIndex = true,
              ioStrategy = _ => randomIOAccess(cacheOnAccess = true),
              compressions = _ => compressions(2)
            ),
          hashIndexConfig =
            HashIndexBlock.Config(
              maxProbe = 5,
              minimumNumberOfKeys = 2,
              minimumNumberOfHits = 2,
              copyIndex = randomBoolean(),
              allocateSpace = _.requiredSpace * 10,
              ioStrategy = _ => randomIOAccess(cacheOnAccess = true),
              compressions = _ => compressions(3)
            ),
          bloomFilterConfig =
            BloomFilterBlock.Config(
              falsePositiveRate = 0.001,
              minimumNumberOfKeys = 2,
              optimalMaxProbe = optimalMaxProbe => optimalMaxProbe,
              blockIO = _ => randomIOAccess(cacheOnAccess = true),
              compressions = _ => compressions(4)
            )
        )

      val cache = getSegmentBlockCache(keyValues, SegmentIO.defaultSynchronisedStored)
      cache.isCached shouldBe false

      cache.getFooter().get
      cache.isCached shouldBe true
      cache.footerBlockCache.isCached shouldBe true
      cache.clear()
      cache.isCached shouldBe false
      cache.footerBlockCache.isCached shouldBe false

      cache.getBinarySearchIndex().get
      cache.getSortedIndex().get
      cache.getBloomFilter().get
      cache.getHashIndex().get
      cache.getValues().get

      cache.binarySearchIndexBlockCache.isCached shouldBe true
      cache.sortedIndexBlockCache.isCached shouldBe true
      cache.bloomFilterBlockCache.isCached shouldBe true
      cache.hashIndexBlockCache.isCached shouldBe true
      cache.valuesBlockCache.isCached shouldBe true

      cache.segmentReaderCache.isCached shouldBe true

      cache.binarySearchIndexReaderCache.isCached shouldBe false
      cache.sortedIndexReaderCache.isCached shouldBe false
      cache.bloomFilterReaderCache.isCached shouldBe false
      cache.hashIndexReaderCache.isCached shouldBe false
      cache.valuesReaderCache.isCached shouldBe false

      cache.clear()
      cache.isCached shouldBe false
    }
  }
}
