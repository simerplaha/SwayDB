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

package swaydb.core.group.compression

import swaydb.compression.CompressionInternal
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data._
import swaydb.core.segment.format.a.block._
import swaydb.core.{TestBase, TestLimitQueues, TestTimer}
import swaydb.data.config.UncompressedBlockInfo
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

/**
 * [[swaydb.core.group.compression.GroupCompressor]] is always invoked directly from [[Transient.Group]] there these test cases initialise the Group
 * to find full code coverage.
 *
 */
class GroupCompressorSpec extends TestBase {

  implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default
  implicit def testTimer: TestTimer = TestTimer.random
  implicit val limiter = TestLimitQueues.memorySweeper

  val keyValueCount = 100

  def genKeyValuesWithCompression(compressions: UncompressedBlockInfo => Seq[CompressionInternal]) =
    eitherOne(
      //either one key-value
      left =
        eitherOne(
          Slice(randomFixedKeyValue(1, eitherOne(None, Some(2)))),
          Slice(randomRangeKeyValue(1, 2, randomFromValueOption(), rangeValue = Value.update(2, randomDeadlineOption)))
        ).toTransient,
      right =
        //multiple key-values
        randomizedKeyValues(keyValueCount, startId = Some(1), addPut = true)
    ).updateStats(
      valuesConfig = ValuesBlock.Config.random.copy(compressions = compressions),
      sortedIndexConfig = SortedIndexBlock.Config.random.copy(compressions = compressions),
      binarySearchIndexConfig = BinarySearchIndexBlock.Config.random.copy(compressions = compressions),
      hashIndexConfig = HashIndexBlock.Config.random.copy(compressions = compressions),
      bloomFilterConfig = BloomFilterBlock.Config.random.copy(compressions = compressions)
    )

  "GroupCompressor" should {
    "return no Group if key-values are empty" in {
      Transient.Group(
        keyValues = Slice.empty,
        previous = None,
        groupConfig = SegmentBlock.Config.random,
        valuesConfig = ValuesBlock.Config.random,
        sortedIndexConfig = SortedIndexBlock.Config.random,
        binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
        hashIndexConfig = HashIndexBlock.Config.random,
        bloomFilterConfig = BloomFilterBlock.Config.random,
        createdInLevel = randomIntMax()
      ).failed.get.exception.getMessage shouldBe GroupCompressor.cannotGroupEmptyValues.exception.getMessage
    }

    "create a group" when {
      "key-values are un-compressible" in {
        runThis(100.times) {
          val compressions = randomCompressionsLZ4OrSnappy(Int.MaxValue)

          val keyValues = genKeyValuesWithCompression(_ => compressions)

          val group =
            Transient.Group(
              keyValues = keyValues,
              previous = None,
              groupConfig = SegmentBlock.Config.random,
              valuesConfig = ValuesBlock.Config.random,
              sortedIndexConfig = SortedIndexBlock.Config.random,
              binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
              hashIndexConfig = HashIndexBlock.Config.random,
              bloomFilterConfig = BloomFilterBlock.Config.random,
              createdInLevel = randomIntMax()
            ).get

          //none of the group's blocks are compressed.
          try
            assertGroup(group)
          catch {
            case exception: Exception =>
              val persistedGroup = assertGroup(group)
              throw exception
          }
          //          persistedGroup.segment.blockCache.createSortedIndexReader().get.block.compressionInfo shouldBe empty
          //          persistedGroup.segment.blockCache.createBinarySearchReader().get foreach (_.block.compressionInfo shouldBe empty)
          //          persistedGroup.segment.blockCache.createHashIndexReader().get foreach (_.block.compressionInfo shouldBe empty)
          //          persistedGroup.segment.blockCache.createBloomFilterReader().get foreach (_.block.compressionInfo shouldBe empty)
          //          persistedGroup.segment.blockCache.createValuesReader().get foreach (_.block.compressionInfo shouldBe empty)
        }
      }

      "key-values are compressible" in {
        runThis(100.times, log = true) {

          //          val compressions = randomCompressionsLZ4OrSnappy(Int.MinValue)
          val compressions = randomCompressionsLZ4OrSnappy(Int.MinValue)

          //          val keyValues = genKeyValuesWithCompression(_ => compressions)
          val keyValues =
            Slice(
              randomGroup(Slice(randomPutKeyValue(1, value = Some(11)).toTransient)),
              randomGroup(Slice(randomPutKeyValue(2, value = Some(22)).toTransient))
            ).updateStats

          val group =
            Transient.Group(
              keyValues = keyValues,
              previous = None,
              groupConfig = SegmentBlock.Config.random,
              valuesConfig = ValuesBlock.Config.random,
              sortedIndexConfig = SortedIndexBlock.Config.random,
              binarySearchIndexConfig = BinarySearchIndexBlock.Config.random,
              hashIndexConfig = HashIndexBlock.Config.random,
              bloomFilterConfig = BloomFilterBlock.Config.random,
              createdInLevel = randomIntMax()
            ).get

          //none of the group's blocks are compressed.
          assertGroup(group)
          //          persistedGroup.segment.blockCache.createSortedIndexReader().get.block.compressionInfo shouldBe defined
          //          persistedGroup.segment.blockCache.createBinarySearchReader().get foreach (_.block.compressionInfo shouldBe defined)
          //          persistedGroup.segment.blockCache.createHashIndexReader().get foreach (_.block.compressionInfo shouldBe defined)
          //          persistedGroup.segment.blockCache.createBloomFilterReader().get foreach (_.block.compressionInfo shouldBe defined)
          //          persistedGroup.segment.blockCache.createValuesReader().get foreach (_.block.compressionInfo shouldBe defined)
        }
      }
    }
  }
}
