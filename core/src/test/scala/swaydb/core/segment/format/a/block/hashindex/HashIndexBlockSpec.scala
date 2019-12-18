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

import swaydb.compression.CompressionInternal
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.{Memory, Persistent, Time}
import swaydb.core.segment.format.a.block.SortedIndexBlock
import swaydb.core.{TestBase, TestSweeper}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers._
import swaydb.serializers.Default._

class HashIndexBlockSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))

  val keyValueCount = 10000

  implicit val blockCacheMemorySweeper = TestSweeper.memorySweeperBlock

  "searching a segment" in {
    runThis(100.times, log = true) {
      //create perfect hash
      //      val compressions = if (randomBoolean()) randomCompressions() else Seq.empty
      val compressions: Seq[CompressionInternal] = Seq.empty

      val keyValues =
        randomizedKeyValues(
          count = 100,
          startId = Some(1)
        )
      val segments =
        getBlocks(
          segmentSize = randomIntMax(100) max 1,
          keyValues = keyValues,
          sortedIndexConfig =
            SortedIndexBlock.Config(
              ioStrategy = _ => randomIOStrategy(),
              prefixCompressionResetCount = 0,
              prefixCompressKeysOnly = randomBoolean(),
              enableAccessPositionIndex = randomBoolean(),
              normaliseIndex = false,
              compressions = _ => compressions
            ),
          hashIndexConfig =
            HashIndexBlock.Config(
              maxProbe = 1000,
              minimumNumberOfKeys = 0,
              minimumNumberOfHits = 0,
              format = randomHashIndexSearchFormat(),
              allocateSpace = _.requiredSpace * 2,
              ioStrategy = _ => randomIOStrategy(),
              compressions = _ => compressions
            )
        ).get

      var keyValueIndex = 0
      var segmentsIndex = 0
      var successCount = 0

      while (keyValueIndex < keyValues.size) {
        val keyValue = keyValues(keyValueIndex)
        val segment = segments(segmentsIndex)

        segment.hashIndexReader shouldBe defined
        //        segment.hashIndexReader.get.block.hit shouldBe keyValues.size
        segment.hashIndexReader.get.block.miss shouldBe 0

        HashIndexBlock.search(
          key = keyValue.key,
          hashIndexReader = segment.hashIndexReader.get,
          sortedIndexReader = segment.sortedIndexReader,
          valuesReader = segment.valuesReader
        ) match {
          case None =>
            segmentsIndex += 1
          //            fail("None on perfect hash.")
          //            false

          case Some(found) =>
            found.toPersistent shouldBe keyValue
            successCount += 1
            keyValueIndex += 1
        }
      }

      successCount shouldBe keyValues.size

    }
  }
}
