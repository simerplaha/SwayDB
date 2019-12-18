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
import swaydb.core.{TestBase, TestSweeper}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

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
          count = 2,
          startId = Some(1)
        )

      val segments =
        getBlocks(
          segmentSize = 10,
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
              allocateSpace = _.requiredSpace * 2,
              ioStrategy = _ => randomIOStrategy(),
              compressions = _ => compressions
            )
        ).get

      segments should have size 1
      val blocks = segments.head

      blocks.hashIndexReader shouldBe defined
      blocks.hashIndexReader.get.block.hit shouldBe keyValues.size
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
