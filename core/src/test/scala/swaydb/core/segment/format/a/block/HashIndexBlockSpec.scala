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
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.data.Transient
import swaydb.core.segment.format.a.block.HashIndexBlock.HashIndexBlockOps
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.data.config.RandomKeyIndex.RequiredSpace
import swaydb.data.io.Core
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random
import swaydb.data.io.Core.IO.Error.ErrorHandler

class HashIndexBlockSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default

  val keyValueCount = 10000

  import keyOrder._

  /**
    * Asserts that both HashIndexes will eventually result in the same index.
    */
  def assertHashIndexes(keyValues: Iterable[Transient],
                        hashIndex1: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock],
                        hashIndex2: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock])(implicit order: KeyOrder[Slice[Byte]]) = {
    keyValues foreach {
      keyValue =>
        val uncompressedIndexes = ListBuffer.empty[Int]
        HashIndexBlock.search(
          key = keyValue.key,
          reader = hashIndex1,
          assertValue =
            index => {
              uncompressedIndexes += index
              IO.none
            }
        ).get

        val compressedIndexes = ListBuffer.empty[Int]
        HashIndexBlock.search(
          key = keyValue.key,
          reader = hashIndex2,
          assertValue =
            index => {
              compressedIndexes += index
              IO.none
            }
        ).get

        uncompressedIndexes should contain atLeastOneElementOf compressedIndexes
    }
  }

  "optimalBytesRequired" should {
    "allocate optimal byte" in {
      HashIndexBlock.optimalBytesRequired(
        keyCounts = 1,
        largestValue = 1,
        allocateSpace = _.requiredSpace,
        hasCompression = false,
        minimumNumberOfKeys = 0
      ) shouldBe
        HashIndexBlock.headerSize(
          keyCounts = 1,
          hasCompression = false,
          writeAbleLargestValueSize = 1
        ) + 1 + 1
    }
  }

  "it" should {
    "write compressed HashIndex and result in the same as uncompressed HashIndex" in {
      runThis(10.times) {
        val maxProbe = 10

        def allocateMoreSpace(requiredSpace: RequiredSpace) = requiredSpace.requiredSpace * 10

        val uncompressedKeyValues =
          randomKeyValues(
            count = 1000,
            addRemoves = true,
            addFunctions = true,
            addRemoveDeadlines = true,
            addUpdates = true,
            addPendingApply = true,
            hashIndexConfig =
              HashIndexBlock.Config.random.copy(
                allocateSpace = allocateMoreSpace,
                compressions = _ => Seq.empty,
                maxProbe = maxProbe
              )
          )

        uncompressedKeyValues should not be empty

        val uncompressedState =
          HashIndexBlock.init(keyValues = uncompressedKeyValues).get

        val compressedState =
          HashIndexBlock.init(
            keyValues =
              uncompressedKeyValues
                .updateStats(
                  hashIndexConfig =
                    HashIndexBlock.Config(
                      allocateSpace = allocateMoreSpace,
                      compressions = _ => randomCompressionsLZ4OrSnappy(),
                      maxProbe = maxProbe,
                      minimumNumberOfKeys = 0,
                      minimumNumberOfHits = 0,
                      blockIO = _ => randomIOAccess()
                    )
                )
          ).get

        uncompressedKeyValues foreach {
          keyValue =>
            HashIndexBlock.write(
              key = keyValue.key,
              value = keyValue.stats.thisKeyValuesAccessIndexOffset,
              state = uncompressedState
            ).get

            HashIndexBlock.write(
              key = keyValue.key,
              value = keyValue.stats.thisKeyValuesAccessIndexOffset,
              state = compressedState
            ).get
        }

        HashIndexBlock.close(uncompressedState).get
        HashIndexBlock.close(compressedState).get

        //compressed bytes should be smaller
        compressedState.bytes.size should be <= uncompressedState.bytes.size

        val uncompressedHashIndex = Block.unblock[HashIndexBlock.Offset, HashIndexBlock](uncompressedState.bytes).get

        val compressedHashIndex = Block.unblock[HashIndexBlock.Offset, HashIndexBlock](compressedState.bytes).get

        uncompressedHashIndex.block.compressionInfo shouldBe empty
        compressedHashIndex.block.compressionInfo shouldBe defined

        uncompressedHashIndex.block.bytesToReadPerIndex shouldBe compressedHashIndex.block.bytesToReadPerIndex
        uncompressedHashIndex.block.hit shouldBe compressedHashIndex.block.hit
        uncompressedHashIndex.block.miss shouldBe compressedHashIndex.block.miss
        uncompressedHashIndex.block.maxProbe shouldBe compressedHashIndex.block.maxProbe
        uncompressedHashIndex.block.writeAbleLargestValueSize shouldBe compressedHashIndex.block.writeAbleLargestValueSize
        uncompressedHashIndex.offset.start shouldBe compressedHashIndex.offset.start

        //        val uncompressedBlockReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock] = Block.unblock(uncompressedHashIndex, SegmentBlock.unblocked(uncompressedState.bytes), randomBoolean()).get
        //        val compressedBlockReader = Block.unblock(compressedHashIndex, SegmentBlock.unblocked(compressedState.bytes), randomBoolean()).get

        //assert that both compressed and uncompressed HashIndexes should result in the same value eventually.
        assertHashIndexes(uncompressedKeyValues, uncompressedHashIndex, compressedHashIndex)
      }
    }
  }

  "build index" when {
    "the hash is perfect" in {
      runThis(100.times) {
        val maxProbe = 1000
        val startId = Some(0)

        val compressions = randomCompressionsOrEmpty()

        val keyValues =
          randomizedKeyValues(
            count = randomIntMax(1000) max 1,
            startId = startId,
            addPut = true,
            hashIndexConfig =
              HashIndexBlock.Config(
                allocateSpace = _.requiredSpace * 5,
                compressions = _ => compressions,
                maxProbe = maxProbe,
                minimumNumberOfKeys = 0,
                minimumNumberOfHits = 0,
                blockIO = _ => randomIOAccess()
              )
          )

        keyValues should not be empty

        val state =
          HashIndexBlock.init(keyValues = keyValues).get

        val allocatedBytes = state.bytes.allocatedSize

        keyValues foreach {
          keyValue =>
            HashIndexBlock.write(
              key = keyValue.key,
              value = keyValue.stats.thisKeyValuesAccessIndexOffset,
              state = state
            ).get
        }

        println(s"hit: ${state.hit}")
        println(s"miss: ${state.miss}")
        println

        HashIndexBlock.close(state).get

        println(s"Bytes allocated: $allocatedBytes")
        println(s"Bytes written: ${state.bytes.size}")

        state.hit shouldBe keyValues.size
        state.miss shouldBe 0
        state.hit + state.miss shouldBe keyValues.size

        println("Building ListMap")
        val indexOffsetMap = mutable.HashMap.empty[Int, ListBuffer[Transient]]

        keyValues foreach {
          keyValue =>
            indexOffsetMap.getOrElseUpdate(keyValue.stats.thisKeyValuesAccessIndexOffset, ListBuffer(keyValue)) += keyValue
        }

        println(s"ListMap created with size: ${indexOffsetMap.size}")

        def findKey(indexOffset: Int, key: Slice[Byte]): IO[Core.IO.Error, Option[Transient]] =
          indexOffsetMap.get(indexOffset) match {
            case Some(keyValues) =>
              IO(keyValues.find(_.key equiv key))

            case None =>
              IO.Failure(Core.IO.Error.Fatal(s"Got index that does not exist: $indexOffset"))
          }

        val hashIndexReader = Block.unblock(BlockRefReader(state.bytes)).get

        keyValues foreach {
          keyValue =>
            val found =
              HashIndexBlock.search(
                key = keyValue.key,
                reader = hashIndexReader,
                assertValue = findKey(_, keyValue.key)
              ).get.get
            (found.key equiv keyValue.key) shouldBe true
        }
      }
    }
  }

  "searching a segment" should {
    "value" in {
      runThis(10.times, log = true) {
        //create perfect hash
        val compressions = if (randomBoolean()) randomCompressions() else Seq.empty

        val keyValues =
          randomizedKeyValues(
            count = 100,
            addPut = true,
            startId = Some(1)
          ).updateStats(
            hashIndexConfig =
              HashIndexBlock.Config(
                maxProbe = 1000,
                minimumNumberOfKeys = 0,
                minimumNumberOfHits = 0,
                allocateSpace = _.requiredSpace * 30,
                blockIO = _ => randomIOStrategy(),
                compressions = _ => compressions
              ),
            sortedIndexConfig =
              SortedIndexBlock.Config(
                blockIO = _ => randomIOStrategy(),
                prefixCompressionResetCount = 0,
                enableAccessPositionIndex = randomBoolean(),
                compressions = _ => compressions
              )
          )

        val blocks = getBlocks(keyValues).get
        blocks.hashIndexReader shouldBe defined
        blocks.hashIndexReader.get.block.hit shouldBe keyValues.last.stats.segmentUniqueKeysCount
        blocks.hashIndexReader.get.block.miss shouldBe 0

        Random.shuffle(keyValues.toList) foreach {
          keyValue =>
            HashIndexBlock.search(
              key = keyValue.key,
              hashIndexReader = blocks.hashIndexReader.get,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get.get shouldBe keyValue
        }
      }
    }
  }
}
