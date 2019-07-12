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

import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.config.UncompressedBlockInfo
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.data.util.StorageUnits._

class SegmentBlockInitialisationSpec extends TestBase {

  val keyValueCount = 100

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit val keyValueLimiter = TestLimitQueues.keyValueLimiter

  implicit def testTimer: TestTimer = TestTimer.random

  "BinarySearchIndex" should {
    "not be created" when {
      "disabled" in {
        runThis(10.times) {
          val keyValues: Slice[KeyValue.WriteOnly] =
            randomizedKeyValues(
              count = 100,
              startId = Some(1),
            ).updateStats(
              binarySearchIndexConfig =
                BinarySearchIndex.Config(
                  enabled = false,
                  minimumNumberOfKeys = 0,
                  fullIndex = randomBoolean(),
                  blockIO = _ => randomIOAccess(),
                  compressions = _ => randomCompressionsOrEmpty()
                )
            )

          val blocks = getBlocks(keyValues).get
          blocks.binarySearchIndexReader shouldBe empty
        }
      }

      "minimumNumberOfKeys is not met" in {
        runThis(10.times) {
          val generatedKeyValues =
            randomizedKeyValues(
              count = 100,
              startId = Some(1),
            )

          val keyValues: Slice[KeyValue.WriteOnly] =
            generatedKeyValues
              .updateStats(
                binarySearchIndexConfig =
                  BinarySearchIndex.Config(
                    enabled = true,
                    minimumNumberOfKeys = generatedKeyValues.size + 1,
                    fullIndex = randomBoolean(),
                    blockIO = _ => randomIOAccess(),
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
        runThis(10.times) {
          val keyValues: Slice[KeyValue.WriteOnly] =
            randomizedKeyValues(
              count = 100,
              startId = Some(1),
              addRandomGroups = false,
              addPut = true,
              addRandomRanges = true,
              addRandomUpdates = false,
              addRandomRemoves = false,
              addRandomFunctions = false,
              addRandomPendingApply = false
            ).updateStats(
              binarySearchIndexConfig =
                BinarySearchIndex.Config(
                  enabled = true,
                  minimumNumberOfKeys = 0,
                  fullIndex = false,
                  blockIO = _ => randomIOAccess(),
                  compressions = _ => randomCompressionsOrEmpty()
                ),
              hashIndexConfig =
                HashIndex.Config(
                  maxProbe = 5,
                  allocateSpace = _.requiredSpace * 10,
                  minimumNumberOfKeys = 0,
                  minimumNumberOfHits = 0,
                  blockIO = _ => randomIOAccess(),
                  compressions = _ => randomCompressionsOrEmpty()
                )
            )

          val blocks = getBlocks(keyValues).get
          blocks.hashIndexReader shouldBe defined
          blocks.hashIndexReader.get.block.hit shouldBe keyValues.size
          blocks.hashIndexReader.get.block.miss shouldBe 0

          if (keyValues.last.stats.segmentTotalNumberOfRanges > 0) {
            blocks.binarySearchIndexReader shouldBe defined

            val expectedBinarySearchValues =
              keyValues
                .collect { case range: Transient.Range => range }
                .count {
                  range =>
                    range.previous.forall(_.stats.thisKeyValuesAccessIndexOffset != range.stats.thisKeyValuesAccessIndexOffset)
                }

            blocks.binarySearchIndexReader.get.block.valuesCount shouldBe expectedBinarySearchValues
          } else {
            blocks.binarySearchIndexReader shouldBe empty
          }
        }
      }
    }

    "fully be created" when {
      "perfect hashIndex" in {
        runThis(10.times) {
          val keyValues: Slice[KeyValue.WriteOnly] =
            randomizedKeyValues(
              count = 100,
              startId = Some(1),
              addRandomGroups = false,
              addPut = true,
              addRandomRanges = true,
              addRandomUpdates = false,
              addRandomRemoves = false,
              addRandomFunctions = false,
              addRandomPendingApply = false
            ).updateStats(
              binarySearchIndexConfig =
                BinarySearchIndex.Config(
                  enabled = true,
                  minimumNumberOfKeys = 0,
                  fullIndex = true,
                  blockIO = _ => randomIOAccess(),
                  compressions = _ => randomCompressionsOrEmpty()
                ),
              hashIndexConfig =
                HashIndex.Config(
                  maxProbe = 5,
                  minimumNumberOfKeys = 0,
                  minimumNumberOfHits = 0,
                  allocateSpace = _.requiredSpace * 10,
                  blockIO = _ => randomIOAccess(),
                  compressions = _ => randomCompressionsOrEmpty()
                )
            )

          val blocks = getBlocks(keyValues).get
          blocks.hashIndexReader shouldBe defined
          blocks.hashIndexReader.get.block.hit shouldBe keyValues.size
          blocks.hashIndexReader.get.block.miss shouldBe 0

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

      "for partial binary search index when hashIndex is completely disabled" in {
        runThis(10.times) {
          val keyValues: Slice[KeyValue.WriteOnly] =
            randomizedKeyValues(
              count = 100,
              startId = Some(1),
              addRandomGroups = false,
              addPut = true,
              addRandomRanges = true,
              addRandomUpdates = false,
              addRandomRemoves = false,
              addRandomFunctions = false,
              addRandomPendingApply = false
            ).updateStats(
              binarySearchIndexConfig =
                BinarySearchIndex.Config(
                  enabled = true,
                  minimumNumberOfKeys = 0,
                  fullIndex = false,
                  blockIO = _ => randomIOAccess(),
                  compressions = _ => randomCompressionsOrEmpty()
                ),
              hashIndexConfig =
                HashIndex.Config(
                  maxProbe = 5,
                  minimumNumberOfKeys = Int.MaxValue,
                  minimumNumberOfHits = 0,
                  allocateSpace = _.requiredSpace * 0,
                  blockIO = _ => randomIOAccess(),
                  compressions = _ => randomCompressionsOrEmpty()
                )
            )

          val blocks = getBlocks(keyValues).get
          blocks.hashIndexReader shouldBe empty

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

  "bloomFilter" should {
    "not be created" when {
      "falsePositive is high" in {
        runThis(10.times) {
          val keyValues: Slice[KeyValue.WriteOnly] =
            randomizedKeyValues(
              count = 100,
              addPut = true,
              startId = Some(1)
            ).updateStats(
              bloomFilterConfig =
                BloomFilter.Config(
                  falsePositiveRate = 1,
                  minimumNumberOfKeys = 0,
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

          val keyValues: Slice[KeyValue.WriteOnly] =
            generatedKeyValues
              .toTransient(
                bloomFilterConfig =
                  BloomFilter.Config(
                    falsePositiveRate = 0.001,
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

          val keyValues: Slice[KeyValue.WriteOnly] =
            (Slice(range) ++ generatedKeyValues)
              .toTransient(
                bloomFilterConfig =
                  BloomFilter.Config(
                    falsePositiveRate = 0.001,
                    minimumNumberOfKeys = 0,
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
            addRandomGroups = false,
            startId = Some(1)
          )

        val keyValues: Slice[KeyValue.WriteOnly] =
          generatedKeyValues
            .updateStats(
              hashIndexConfig =
                HashIndex.Config(
                  maxProbe = 5,
                  minimumNumberOfKeys = 0, //set miminimum to be 10 for hash to be created.
                  minimumNumberOfHits = 10,
                  allocateSpace = _ =>
                    HashIndex.optimalBytesRequired(
                      keyCounts = 1, //allocate space enough for 1
                      minimumNumberOfKeys = 0,
                      largestValue = Int.MaxValue,
                      hasCompression = generatedKeyValues.last.hashIndexConfig.compressions(UncompressedBlockInfo(randomIntMax(1.mb))).nonEmpty,
                      allocateSpace = _.requiredSpace
                    ),
                  blockIO = _ => randomIOAccess(),
                  compressions = _ => randomCompressionsOrEmpty()
                )
            )

        val blocks = getBlocks(keyValues).get
        blocks.hashIndexReader shouldBe empty
      }
    }
  }
}
