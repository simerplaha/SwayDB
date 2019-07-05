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

import swaydb.core.TestData._
import swaydb.core.data.{Memory, Time, Transient}
import swaydb.core.segment.format.a.block.{SortedIndex, Values}
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, BaseEntryIdFormatA, TransientToKeyValueIdBinder}
import swaydb.core.segment.format.a.entry.reader.ValueReader
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._

/**
  * These tests can also be within [[swaydb.core.data.TransientSpec]] because they are
  * asserting on the result of [[Transient.valueEntryBytes]] and [[Transient.indexEntryBytes]].
  *
  * The scope of these tests are larger than it should. ValueWriter should remove references to KeyValue
  * but currently it is like that for performance reasons. We do not value to serialise values to bytes unless
  * [[ValueWriter]] decides to do so.
  */
class ValueReaderWriterSpec extends TestBase {

  "write" should {
    "not compress valueOffset and valueLength if prefixCompression is disabled and compressDuplicateValues is true" in {
      implicit val binder = TransientToKeyValueIdBinder.PutBinder

      def assertIndexBytes(indexBytes: Slice[Byte]) = {
        val readKeyValueId = indexBytes.readIntUnsigned().get
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
          Memory.put(1, 100),
          Memory.put(2, 100),
          Memory.put(3, 100)
        ).toTransient(
          valuesConfig =
            Values.Config(
              compressDuplicateValues = true,
              compressDuplicateRangeValues = true,
              cacheOnAccess = randomBoolean(),
              compressions = randomCompressionsOrEmpty()
            ),
          sortedIndexConfig =
            SortedIndex.Config.random.copy(
              prefixCompressionResetCount = 0
            )
        )

      //the first one is always going to be uncompressed so drop it.
      keyValues.drop(1) foreach {
        keyValue =>
          keyValue.valueEntryBytes shouldBe empty
          assertIndexBytes(keyValue.indexEntryBytes)
      }
    }
  }
}
