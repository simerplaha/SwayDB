/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 * SwayDB is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.entry.writer

import swaydb.core.CommonAssertions._
import swaydb.core.TestData._
import swaydb.core.data.{Time, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.SegmentWriter
import swaydb.core.segment.format.a.block._
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, BaseEntryIdFormatA, TransientToKeyValueIdBinder}
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

class ValueReaderWriterSpec extends TestBase {

  "it" should {
    "not compress valueOffset and valueLength if prefixCompression is disabled and compressDuplicateValues is true" in {
      implicit val binder = TransientToKeyValueIdBinder.PutBinder

      def assertIndexBytes(result: KeyValueWriter.WriteResult) = {
        val readKeyValueId = result.indexBytes.readIntUnsigned().get
        binder.keyValueId.hasKeyValueId(readKeyValueId) shouldBe true

        val baseId = binder.keyValueId.adjustKeyValueIdToBaseId(readKeyValueId)
        val typedBaseId = BaseEntryIdFormatA.baseIds.find(_.baseId == baseId).get

        //check the ids assigned should be of correct types.
        typedBaseId shouldBe a[BaseEntryId.Value.FullyCompressed]
        typedBaseId shouldBe a[BaseEntryId.ValueOffset.Uncompressed]
        typedBaseId shouldBe a[BaseEntryId.ValueLength.Uncompressed]
      }

      implicit val timer = TestTimer.Empty

      val keyValues =
        Slice(
          Transient.put(1, 100),
          Transient.update(1, 100),
          Transient.Function(
            key = 1,
            function = 100,
            deadline = randomDeadlineOption(),
            time = Time.empty,
            previous = None,
            valuesConfig =
              Values.Config(
                compressDuplicateValues = true,
                compressDuplicateRangeValues = true,
                cacheOnRead = false,
                hasCompression = true
              ),
            sortedIndexConfig =
              SortedIndex.Config(
                cacheOnRead = false,
                prefixCompressionResetCount = 0,
                hasCompression = false,
                enableAccessPositionIndex = true
              ),
            binarySearchIndexConfig = BinarySearchIndex.Config.disabled,
            hashIndexConfig = HashIndex.Config.disabled,
            bloomFilterConfig = BloomFilter.Config.disabled
          )
        ).updateStats

      //the first one is always going to be uncompressed so drop it.
      keyValues.drop(1) foreach {
        keyValue =>
          val result =
            ValueWriter.write(
              current = keyValue,
              enablePrefixCompression = false,
              compressDuplicateValues = true,
              entryId = BaseEntryIdFormatA.format.start.noTime,
              plusSize = 0,
              isKeyUncompressed = false,
              hasPrefixCompressed = false
            )
          assertIndexBytes(result)
      }

      //just a mini test to test these key-values are writable to Segment.
      implicit val keyOrder = KeyOrder.default
      val bytes =
        SegmentWriter.write(
          keyValues = keyValues,
          createdInLevel = 0,
          maxProbe = 0,
          blockCompressions = BlocksCompression.disabled
        ).get.flattenSegmentBytes

      val (footer, valuesReader, sortedIndex, hashIndex, binarySearchIndex, bloomFilter) = getIndexes(Reader(bytes)).get

      footer.hasGroup shouldBe false
      footer.bloomFilterItemsCount shouldBe keyValues.size
      footer.hasRange shouldBe false
      footer.hasPut shouldBe true

      SortedIndex.readAll(
        keyValueCount = footer.keyValueCount,
        sortedIndexReader = sortedIndex,
        valuesReader = valuesReader
      ).get shouldBe keyValues
    }
  }
}
