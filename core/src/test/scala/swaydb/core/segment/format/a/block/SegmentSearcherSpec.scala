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

import org.scalamock.scalatest.MockFactory
import swaydb.core.CommonAssertions._
import swaydb.core.RunThis._
import swaydb.core.TestBase
import swaydb.core.TestData._
import swaydb.core.data.Transient
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class SegmentSearcherSpec extends TestBase with MockFactory {
  implicit val order = KeyOrder.default

  "search" should {
    "not invoke binary search index if hashIndex is perfect and segment contains & does not contain ranges" in {

      def runTest(_keyValues: Slice[Transient]) = {
        val fullIndex = randomBoolean()

        val keyValues =
          _keyValues.updateStats(
            binarySearchIndexConfig =
              BinarySearchIndexBlock.Config(
                enabled = true,
                minimumNumberOfKeys = 0,
                fullIndex = fullIndex,
                blockIO = _ => randomIOAccess(),
                compressions = _ => randomCompressionsOrEmpty()
              ),
            hashIndexConfig =
              HashIndexBlock.Config(
                maxProbe = 100,
                minimumNumberOfKeys = 0,
                minimumNumberOfHits = 0,
                allocateSpace = _.requiredSpace * 10,
                blockIO = _ => randomIOAccess(),
                compressions = _ => randomCompressionsLZ4OrSnappy()
              )
          )

        val blocks = getBlocks(keyValues).get
        blocks.hashIndexReader shouldBe defined
        blocks.hashIndexReader.get.block.miss shouldBe 0
        blocks.hashIndexReader.get.block.hit shouldBe keyValues.last.stats.segmentUniqueKeysCount
        if (fullIndex)
          blocks.binarySearchIndexReader shouldBe defined

        keyValues foreach {
          keyValue =>
            SegmentSearcher.search(
              key = keyValue.minKey,
              start = None,
              end = None,
              hashIndexReader = blocks.hashIndexReader,
              binarySearchIndexReader = null, //set it to null. BinarySearchIndex is not accessed.
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderReader = blocks.valuesReader,
              hasRange = keyValues.last.stats.segmentHasRange
            ).get shouldBe keyValue
        }

        //check keys that do not exist return none. This will also not hit binary search index since HashIndex is perfect.
        (1000000 to 1000000 + 100) foreach {
          key =>
            SegmentSearcher.search(
              key = key,
              start = None,
              end = None,
              hashIndexReader = blocks.hashIndexReader,
              binarySearchIndexReader =
                if (keyValues.last.stats.segmentHasRange) //if has range binary search index will be used.
                  blocks.binarySearchIndexReader
                else
                  null,
              sortedIndexReader = blocks.sortedIndexReader,
              valuesReaderReader = blocks.valuesReader,
              hasRange = keyValues.last.stats.segmentHasRange
            ).get shouldBe empty
        }
      }

      runThis(20.times, log = true, "testing on 1000 key-values") {
        val useRanges = randomBoolean()
        val keyValues = randomizedKeyValues(startId = Some(1), count = 1000, addPut = true, addRandomRanges = useRanges)
        if (keyValues.nonEmpty) runTest(keyValues)
      }
    }
  }
}
