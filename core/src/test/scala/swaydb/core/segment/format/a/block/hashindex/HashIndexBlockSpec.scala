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

import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestData._
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.SortedIndexBlock
import swaydb.core.{SegmentBlocks, TestBase, TestSweeper}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class HashIndexBlockSpec extends TestBase {

  implicit val keyOrder = KeyOrder.default
  implicit val partialKeyOrder: KeyOrder[Persistent.Partial] = KeyOrder(Ordering.by[Persistent.Partial, Slice[Byte]](_.key)(keyOrder))

  val keyValueCount = 10000

  implicit val blockCacheMemorySweeper = TestSweeper.memorySweeperBlock

  "searching a segment" in {
    runThis(100.times, log = true) {
      //create perfect hash
      val compressions = if (randomBoolean()) randomCompressions() else Seq.empty

      val keyValues =
        randomizedKeyValues(
          count = 1000,
          startId = Some(1)
        )

      val segments: Slice[SegmentBlocks] =
        getBlocks(
          segmentSize = randomIntMax(1000) max 1,
          keyValues = keyValues,
          sortedIndexConfig =
            SortedIndexBlock.Config(
              ioStrategy = _ => randomIOStrategy(),
              prefixCompressionResetCount = 0,
              prefixCompressKeysOnly = randomBoolean(),
              enableAccessPositionIndex = randomBoolean(),
              normaliseIndex = randomBoolean(),
              compressions = _ => compressions
            ),
          hashIndexConfig =
            HashIndexBlock.Config(
              maxProbe = 1000,
              minimumNumberOfKeys = 0,
              minimumNumberOfHits = 0,
              format = randomHashIndexSearchFormat(),
              allocateSpace = _.requiredSpace * 4,
              ioStrategy = _ => randomIOStrategy(),
              compressions = _ => compressions
            )
        ).get

      var keyValueIndex = 0
      var segmentsIndex = 0
      var successCount = 0

      //all key-values get written to multiple segment. So iterate and search all Segment sequentially
      //until all key-values are read via HashIndex.
      while (keyValueIndex < keyValues.size) {
        val keyValue = keyValues(keyValueIndex)
        val segment = segments(segmentsIndex)

        segment.hashIndexReader shouldBe defined
        segment.hashIndexReader.get.block.hit shouldBe segment.footer.keyValueCount
        segment.hashIndexReader.get.block.miss shouldBe 0

        HashIndexBlock.search(
          key = keyValue.key,
          hashIndexReader = segment.hashIndexReader.get,
          sortedIndexReader = segment.sortedIndexReader,
          valuesReaderNullable = segment.valuesReader.orNull
        ) match {
          case Persistent.Partial.Null =>
            //may be it's in the next Segment.
            segmentsIndex += 1

          case found: Persistent.Partial =>
            found.toPersistent shouldBe keyValue
            successCount += 1
            keyValueIndex += 1
        }
      }

      successCount shouldBe keyValues.size
    }
  }
}
