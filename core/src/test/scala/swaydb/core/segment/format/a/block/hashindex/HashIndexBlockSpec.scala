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

package swaydb.core.segment.format.a.block.hashindex

import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.data.Transient
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock.HashIndexBlockOps
import swaydb.core.segment.format.a.block.reader.{BlockRefReader, UnblockedReader}
import swaydb.core.segment.format.a.block.{Block, KeyMatcher, SortedIndexBlock}
import swaydb.data.config.RandomKeyIndex.RequiredSpace
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import org.scalatest.OptionValues._
import swaydb.IOValues._
import swaydb.core.segment.format.a.block.KeyMatcher.Result

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

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
        writeAbleLargestValueSize = 1,
        allocateSpace = _.requiredSpace,
        hasCompression = false,
        minimumNumberOfKeys = 0,
        copyIndex = randomBoolean()
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
      runThis(20.times) {
        val maxProbe = 10

        def allocateMoreSpace(requiredSpace: RequiredSpace) = requiredSpace.requiredSpace * 10

        val copyIndex = randomBoolean()

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
                copyIndex = copyIndex,
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
                      copyIndex = copyIndex,
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
                copyIndex = false,
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

        def findKey(indexOffset: Int, key: Slice[Byte]): IO[swaydb.Error.Segment, Option[Transient]] =
          indexOffsetMap.get(indexOffset) match {
            case Some(keyValues) =>
              IO(keyValues.find(_.key equiv key))

            case None =>
              IO.Left(swaydb.Error.Fatal(s"Got index that does not exist: $indexOffset"))
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
      runThis(100.times, log = true) {
        //create perfect hash
        val compressions = if (randomBoolean()) randomCompressions() else Seq.empty

        val keyValues =
          randomizedKeyValues(
            count = 1000,
            startId = Some(1),
          ).updateStats(
            hashIndexConfig =
              HashIndexBlock.Config(
                maxProbe = 1000,
                minimumNumberOfKeys = 0,
                minimumNumberOfHits = 0,
                copyIndex = randomBoolean(),
                allocateSpace = _.requiredSpace * 2,
                blockIO = _ => randomIOStrategy(),
                compressions = _ => compressions
              ),
            sortedIndexConfig =
              SortedIndexBlock.Config(
                ioStrategy = _ => randomIOStrategy(),
                prefixCompressionResetCount = randomIntMax(10),
                enableAccessPositionIndex = randomBoolean(),
                enablePartialRead = randomBoolean(),
                disableKeyPrefixCompression = randomBoolean(),
                normaliseIndex = randomBoolean(),
                compressions = _ => compressions
              )
          )

        val blocks = getBlocks(keyValues).get
        blocks.hashIndexReader shouldBe defined
        blocks.hashIndexReader.get.block.hit shouldBe keyValues.last.stats.linkedPosition
        blocks.hashIndexReader.get.block.miss shouldBe 0

        keyValues foreach {
          keyValue =>
            HashIndexBlock.search(
              key = keyValue.key,
              hashIndexReader = blocks.hashIndexReader.get,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReader = blocks.valuesReader
            ).get match {
              case notFound: HashIndexSearchResult.NotFound =>
                //if it's not found then the index must be prefix compressed.
                blocks.sortedIndexReader.block.hasPrefixCompression shouldBe true

                notFound match {
                  case HashIndexSearchResult.None =>
                    fail("Expected Lower.")

                  case HashIndexSearchResult.Lower(lower) =>
                    SortedIndexBlock.seekAndMatchOrSeek(
                      matcher = KeyMatcher.Get(keyValue.key),
                      previous = lower.toPersistent.get,
                      next = None,
                      fullRead = true,
                      indexReader = blocks.sortedIndexReader,
                      valuesReader = blocks.valuesReader
                    ).value match {
                      case Result.Matched(previous, result, next) =>
                        result shouldBe keyValue

                      case Result.BehindStopped(_) =>
                        fail()

                      case Result.AheadOrNoneOrEnd =>
                        fail()
                    }
                }

              case HashIndexSearchResult.Found(found) =>
                found shouldBe keyValue
            }
        }
      }
    }
  }
}