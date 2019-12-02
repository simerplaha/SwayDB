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

import org.scalatest.OptionValues._
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.hashindex.HashIndexBlock.HashIndexBlockOps
import swaydb.core.segment.format.a.block.{Block, SortedIndexBlock}
import swaydb.core.{TestBase, TestSweeper}
import swaydb.data.config.RandomKeyIndex.RequiredSpace
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

class HashIndexBlockSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))

  val keyValueCount = 10000

  implicit val blockCacheMemorySweeper = TestSweeper.memorySweeperBlock

  "it" should {
    "write compressed HashIndex and result in the same as uncompressed HashIndex" in {
      runThis(100.times, log = true) {
        val maxProbe = 10

        def allocateMoreSpace(requiredSpace: RequiredSpace) = requiredSpace.requiredSpace * 10

        val format = randomHashIndexSearchFormat()

        val uncompressedKeyValues =
          randomKeyValues(
            count = 1000,
            startId = Some(1),
            //            addRemoves = true,
            //            addFunctions = true,
            //            addRemoveDeadlines = true,
            //            addUpdates = true,
            //            addPendingApply = true,
            hashIndexConfig =
              HashIndexBlock.Config(
                allocateSpace = allocateMoreSpace,
                compressions = _ => Seq.empty,
                format = format,
                maxProbe = maxProbe,
                minimumNumberOfKeys = 0,
                minimumNumberOfHits = 0,
                ioStrategy = _ => randomIOAccess()
              ),
            sortedIndexConfig =
              SortedIndexBlock.Config.random.copy(prefixCompressionResetCount = 0)
          )

        uncompressedKeyValues should not be empty

        val uncompressedState =
          HashIndexBlock.init(keyValues = uncompressedKeyValues).value

        val compressedState =
          HashIndexBlock.init(
            keyValues =
              uncompressedKeyValues
                .updateStats(
                  hashIndexConfig =
                    HashIndexBlock.Config(
                      allocateSpace = allocateMoreSpace,
                      compressions = _ => randomCompressionsLZ4OrSnappy(),
                      format = format,
                      maxProbe = maxProbe,
                      minimumNumberOfKeys = 0,
                      minimumNumberOfHits = 0,
                      ioStrategy = _ => randomIOAccess()
                    )
                )
          ).get

        uncompressedKeyValues foreach {
          keyValue =>
            val uncompressedWriteResult =
              HashIndexBlock.write(
                keyValue = keyValue,
                state = uncompressedState
              )

            val compressedWriteResult =
              HashIndexBlock.write(
                keyValue = keyValue,
                state = compressedState
              )

            uncompressedWriteResult shouldBe compressedWriteResult
        }

        HashIndexBlock.close(uncompressedState)
        HashIndexBlock.close(compressedState)

        //compressed bytes should be smaller
        compressedState.bytes.size should be <= uncompressedState.bytes.size

        val uncompressedHashIndex = Block.unblock[HashIndexBlock.Offset, HashIndexBlock](uncompressedState.bytes)

        val compressedHashIndex = Block.unblock[HashIndexBlock.Offset, HashIndexBlock](compressedState.bytes)

        uncompressedHashIndex.block.compressionInfo shouldBe empty
        compressedHashIndex.block.compressionInfo shouldBe defined

        uncompressedHashIndex.block.bytesToReadPerIndex shouldBe compressedHashIndex.block.bytesToReadPerIndex
        uncompressedHashIndex.block.hit shouldBe compressedHashIndex.block.hit
        uncompressedHashIndex.block.miss shouldBe compressedHashIndex.block.miss
        uncompressedHashIndex.block.maxProbe shouldBe compressedHashIndex.block.maxProbe
        uncompressedHashIndex.block.writeAbleLargestValueSize shouldBe compressedHashIndex.block.writeAbleLargestValueSize

        //        println(s"hit: ${uncompressedHashIndex.block.hit}")
        //        println(s"miss: ${uncompressedHashIndex.block.miss}")
        //        println(s"prefixCompressionResetCount: ${uncompressedKeyValues.last.sortedIndexConfig.prefixCompressionResetCount}")

        //        val uncompressedBlockReader: UnblockedReader[HashIndexBlock.Offset, HashIndexBlock] = Block.unblock(uncompressedHashIndex, SegmentBlock.unblocked(uncompressedState.bytes), randomBoolean())
        //        val compressedBlockReader = Block.unblock(compressedHashIndex, SegmentBlock.unblocked(compressedState.bytes), randomBoolean())

      }
    }
  }

  "searching a segment" should {
    "succeed" in {
      runThis(100.times, log = true) {
        //create perfect hash
        val compressions = if (randomBoolean()) randomCompressions() else Seq.empty

        val keyValues =
          randomizedKeyValues(
            count = 1000,
            startId = Some(1)
          ).updateStats(
            hashIndexConfig =
              HashIndexBlock.Config(
                maxProbe = 1000,
                minimumNumberOfKeys = 0,
                minimumNumberOfHits = 0,
                format = randomHashIndexSearchFormat(),
                allocateSpace = _.requiredSpace * 2,
                ioStrategy = _ => randomIOStrategy(),
                compressions = _ => compressions
              ),
            sortedIndexConfig =
              SortedIndexBlock.Config(
                ioStrategy = _ => randomIOStrategy(),
                prefixCompressionResetCount = 0,
                enableAccessPositionIndex = randomBoolean(),
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
            ) match {
              case None =>
                fail("None on perfect hash.")

              case Some(found) =>
                found.toPersistent shouldBe keyValue
            }
        }
      }
    }
  }
}
