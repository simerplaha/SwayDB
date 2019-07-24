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
import swaydb.core.data.{Memory, Persistent, Time, Transient}
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.{SortedIndexBlock, ValuesBlock}
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, BaseEntryIdFormatA, TransientToKeyValueIdBinder}
import swaydb.core.segment.format.a.entry.reader.EntryReader
import swaydb.core.{TestBase, TestTimer}
import swaydb.data.slice.Slice
import swaydb.serializers.Default._
import swaydb.serializers._
import swaydb.data.io.Core.IO.Error.ErrorHandler

/**
  * These tests can also be within [[swaydb.core.data.TransientSpec]] because they are
  * asserting on the result of [[Transient.valueEntryBytes]] and [[Transient.indexEntryBytes]].
  *
  * The scope of these tests are larger than it should. ValueWriter should remove references to KeyValue
  * but currently it is like that for performance reasons. We do not value to serialise values to bytes unless
  * [[ValueWriter]] decides to do so.
  *
  * Need a simpler way of testing these but this will do for now.
  *
  * What being doing is here is that a transient key-value is serialised and re-read byte [[EntryReader]].
  */
class ValueReaderWriterSpec extends TestBase {
  implicit val timer = TestTimer.Empty

  "not compress valueOffset and valueLength if prefixCompression is disabled and compressDuplicateValues is true" in {
    val keyValues =
      Slice(
        Memory.put(1, 100),
        Memory.update(2, 100),
        Memory.Function(3, 100, Time.empty)
      ).toTransient(
        valuesConfig =
          ValuesBlock.Config(
            compressDuplicateValues = true,
            compressDuplicateRangeValues = true,
            blockIO = _ => randomIOAccess(),
            compressions = _ => randomCompressionsOrEmpty()
          ),
        sortedIndexConfig =
          SortedIndexBlock.Config.random.copy(
            prefixCompressionResetCount = 0
          )
      )

    //HEAD KEY-VALUE
    keyValues.head.valueEntryBytes.headOption shouldBe defined
    var readKeyValueId = keyValues.head.indexEntryBytes.readIntUnsigned().get
    TransientToKeyValueIdBinder.PutBinder.keyValueId.hasKeyValueId(readKeyValueId) shouldBe true
    var baseId = TransientToKeyValueIdBinder.PutBinder.keyValueId.adjustKeyValueIdToBaseId(readKeyValueId)
    var typedBaseId = BaseEntryIdFormatA.baseIds.find(_.baseId == baseId).get
    //check the ids are correct types.
    typedBaseId shouldBe a[BaseEntryId.Value.Uncompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueOffset.Uncompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueLength.Uncompressed]
    //read key-value using the persistent information.
    val readHeadKeyValue =
      EntryReader.read(
        indexReader = Reader(keyValues.head.indexEntryBytes),
        mightBeCompressed = randomBoolean(),
        valueCache = Some(buildSingleValueCache(keyValues.head.valueEntryBytes.head)),
        indexOffset = 0,
        nextIndexOffset = keyValues.head.indexEntryBytes.size,
        nextIndexSize = keyValues(1).indexEntryBytes.size,
        accessPosition = 0,
        previous = None
      ).get
    readHeadKeyValue shouldBe a[Persistent.Put]
    readHeadKeyValue.key shouldBe keyValues.head.key
    readHeadKeyValue.asInstanceOf[Persistent.Put].getOrFetchValue.get.get shouldBe keyValues.head.values.head

    //SECOND KEY-VALUE
    keyValues(1).valueEntryBytes shouldBe empty
    readKeyValueId = keyValues(1).indexEntryBytes.readIntUnsigned().get
    TransientToKeyValueIdBinder.UpdateBinder.keyValueId.hasKeyValueId(readKeyValueId) shouldBe true
    baseId = TransientToKeyValueIdBinder.UpdateBinder.keyValueId.adjustKeyValueIdToBaseId(readKeyValueId)
    typedBaseId = BaseEntryIdFormatA.baseIds.find(_.baseId == baseId).get
    //check the ids are correct types.
    typedBaseId shouldBe a[BaseEntryId.Value.FullyCompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueOffset.Uncompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueLength.Uncompressed]
    //read the key-value giving it the previous key-value.
    val readNextKeyValue =
      EntryReader.read(
        indexReader = Reader(keyValues(1).indexEntryBytes),
        mightBeCompressed = true,
        valueCache = Some(buildSingleValueCache(keyValues.head.valueEntryBytes.head)),
        indexOffset = 0,
        nextIndexOffset = keyValues(1).indexEntryBytes.size,
        nextIndexSize = keyValues(2).indexEntryBytes.size,
        accessPosition = 0,
        previous = Some(readHeadKeyValue)
      ).get
    readNextKeyValue shouldBe a[Persistent.Update]
    readNextKeyValue.key shouldBe keyValues(1).key
    readNextKeyValue.asInstanceOf[Persistent.Update].getOrFetchValue.get.get shouldBe keyValues(1).values.head

    //THIRD KEY-VALUE
    keyValues(2).valueEntryBytes shouldBe empty
    readKeyValueId = keyValues(2).indexEntryBytes.readIntUnsigned().get
    TransientToKeyValueIdBinder.FunctionBinder.keyValueId.hasKeyValueId(readKeyValueId) shouldBe true
    baseId = TransientToKeyValueIdBinder.FunctionBinder.keyValueId.adjustKeyValueIdToBaseId(readKeyValueId)
    typedBaseId = BaseEntryIdFormatA.baseIds.find(_.baseId == baseId).get
    //check the ids are correct types.
    typedBaseId shouldBe a[BaseEntryId.Value.FullyCompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueOffset.Uncompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueLength.Uncompressed]
    //read the key-value giving it the previous key-value.
    val readLastKeyValue =
      EntryReader.read(
        indexReader = Reader(keyValues(2).indexEntryBytes),
        mightBeCompressed = true,
        valueCache = Some(buildSingleValueCache(keyValues.head.valueEntryBytes.head)),
        indexOffset = 0,
        nextIndexOffset = 0,
        nextIndexSize = 0,
        accessPosition = 0,
        previous = Some(readNextKeyValue)
      ).get
    readLastKeyValue shouldBe a[Persistent.Function]
    readLastKeyValue.key shouldBe keyValues(2).key
    readLastKeyValue.asInstanceOf[Persistent.Function].getOrFetchFunction.get shouldBe keyValues(2).values.head
  }

  "compress valueOffset and valueLength if prefixCompression is true and compressDuplicateValues is true" in {
    val keyValues =
      Slice(
        Memory.put(1, 100),
        Memory.update(2, 100),
        Memory.Function(3, 100, Time.empty)
      ).toTransient(
        valuesConfig =
          ValuesBlock.Config(
            compressDuplicateValues = true,
            compressDuplicateRangeValues = true,
            blockIO = _ => randomIOAccess(),
            compressions = _ => randomCompressionsOrEmpty()
          ),
        sortedIndexConfig =
          SortedIndexBlock.Config.random.copy(
            prefixCompressionResetCount = Int.MaxValue
          )
      )

    //HEAD KEY-VALUE
    keyValues.head.valueEntryBytes.headOption shouldBe defined
    var readKeyValueId = keyValues.head.indexEntryBytes.readIntUnsigned().get
    TransientToKeyValueIdBinder.PutBinder.keyValueId.hasKeyValueId(readKeyValueId) shouldBe true
    var baseId = TransientToKeyValueIdBinder.PutBinder.keyValueId.adjustKeyValueIdToBaseId(readKeyValueId)
    var typedBaseId = BaseEntryIdFormatA.baseIds.find(_.baseId == baseId).get
    //check the ids are correct types.
    typedBaseId shouldBe a[BaseEntryId.Value.Uncompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueOffset.Uncompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueLength.Uncompressed]
    //read key-value using the persistent information.
    val readHeadKeyValue =
      EntryReader.read(
        indexReader = Reader(keyValues.head.indexEntryBytes),
        mightBeCompressed = randomBoolean(),
        valueCache = Some(buildSingleValueCache(keyValues.head.valueEntryBytes.head)),
        indexOffset = 0,
        nextIndexOffset = keyValues.head.indexEntryBytes.size,
        nextIndexSize = keyValues(1).indexEntryBytes.size,
        accessPosition = 0,
        previous = None
      ).get
    readHeadKeyValue shouldBe a[Persistent.Put]
    readHeadKeyValue.key shouldBe keyValues.head.key
    readHeadKeyValue.asInstanceOf[Persistent.Put].getOrFetchValue.get.get shouldBe keyValues.head.values.head

    //SECOND KEY-VALUE
    keyValues(1).valueEntryBytes shouldBe empty
    readKeyValueId = keyValues(1).indexEntryBytes.readIntUnsigned().get
    TransientToKeyValueIdBinder.UpdateBinder.keyValueId.hasKeyValueId(readKeyValueId) shouldBe true
    baseId = TransientToKeyValueIdBinder.UpdateBinder.keyValueId.adjustKeyValueIdToBaseId(readKeyValueId)
    typedBaseId = BaseEntryIdFormatA.baseIds.find(_.baseId == baseId).get
    //check the ids are correct types.
    typedBaseId shouldBe a[BaseEntryId.Value.FullyCompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueOffset.FullyCompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueLength.FullyCompressed]
    //read the key-value giving it the previous key-value.
    val readNextKeyValue =
      EntryReader.read(
        indexReader = Reader(keyValues(1).indexEntryBytes),
        mightBeCompressed = true,
        valueCache = Some(buildSingleValueCache(keyValues.head.valueEntryBytes.head)),
        indexOffset = 0,
        nextIndexOffset = keyValues(1).indexEntryBytes.size,
        nextIndexSize = keyValues(2).indexEntryBytes.size,
        accessPosition = 0,
        previous = Some(readHeadKeyValue)
      ).get
    readNextKeyValue shouldBe a[Persistent.Update]
    readNextKeyValue.key shouldBe keyValues(1).key
    readNextKeyValue.asInstanceOf[Persistent.Update].getOrFetchValue.get.get shouldBe keyValues(1).values.head

    //THIRD KEY-VALUE
    keyValues(2).valueEntryBytes shouldBe empty
    readKeyValueId = keyValues(2).indexEntryBytes.readIntUnsigned().get
    TransientToKeyValueIdBinder.FunctionBinder.keyValueId.hasKeyValueId(readKeyValueId) shouldBe true
    baseId = TransientToKeyValueIdBinder.FunctionBinder.keyValueId.adjustKeyValueIdToBaseId(readKeyValueId)
    typedBaseId = BaseEntryIdFormatA.baseIds.find(_.baseId == baseId).get
    //check the ids are correct types.
    typedBaseId shouldBe a[BaseEntryId.Value.FullyCompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueOffset.FullyCompressed]
    typedBaseId shouldBe a[BaseEntryId.ValueLength.FullyCompressed]
    //read the key-value giving it the previous key-value.
    val readLastKeyValue =
      EntryReader.read(
        indexReader = Reader(keyValues(2).indexEntryBytes),
        mightBeCompressed = true,
        valueCache = Some(buildSingleValueCache(keyValues.head.valueEntryBytes.head)),
        indexOffset = 0,
        nextIndexOffset = 0,
        nextIndexSize = 0,
        accessPosition = 0,
        previous = Some(readNextKeyValue)
      ).get
    readLastKeyValue shouldBe a[Persistent.Function]
    readLastKeyValue.key shouldBe keyValues(2).key
    readLastKeyValue.asInstanceOf[Persistent.Function].getOrFetchFunction.get shouldBe keyValues(2).values.head
  }

  "write no value" in {
    val keyValues =
      Slice(
        Memory.put(1),
        Memory.update(2),
        Memory.remove(3)
      ).toTransient(
        valuesConfig =
          ValuesBlock.Config(
            compressDuplicateValues = randomBoolean(),
            compressDuplicateRangeValues = randomBoolean(),
            blockIO = _ => randomIOAccess(),
            compressions = _ => randomCompressionsOrEmpty()
          ),
        sortedIndexConfig =
          SortedIndexBlock.Config.random.copy(
            prefixCompressionResetCount = 0
          )
      )

    //HEAD KEY-VALUE
    keyValues.head.valueEntryBytes.headOption shouldBe empty
    var readKeyValueId = keyValues.head.indexEntryBytes.readIntUnsigned().get
    TransientToKeyValueIdBinder.PutBinder.keyValueId.hasKeyValueId(readKeyValueId) shouldBe true
    var baseId = TransientToKeyValueIdBinder.PutBinder.keyValueId.adjustKeyValueIdToBaseId(readKeyValueId)
    var typedBaseId = BaseEntryIdFormatA.baseIds.find(_.baseId == baseId).get
    //check the ids are correct types.
    typedBaseId shouldBe a[BaseEntryId.Value.NoValue]
    typedBaseId should not be a[BaseEntryId.ValueOffset.Uncompressed]
    typedBaseId should not be a[BaseEntryId.ValueLength.Uncompressed]
    //read key-value using the persistent information.
    val readHeadKeyValue =
      EntryReader.read(
        indexReader = Reader(keyValues.head.indexEntryBytes),
        mightBeCompressed = randomBoolean(),
        valueCache = None,
        indexOffset = 0,
        nextIndexOffset = keyValues.head.indexEntryBytes.size,
        nextIndexSize = keyValues(1).indexEntryBytes.size,
        accessPosition = 0,
        previous = None
      ).get
    readHeadKeyValue shouldBe a[Persistent.Put]
    readHeadKeyValue.key shouldBe keyValues.head.key
    readHeadKeyValue.asInstanceOf[Persistent.Put].getOrFetchValue.get shouldBe empty

    //SECOND KEY-VALUE
    keyValues(1).valueEntryBytes shouldBe empty
    readKeyValueId = keyValues(1).indexEntryBytes.readIntUnsigned().get
    TransientToKeyValueIdBinder.UpdateBinder.keyValueId.hasKeyValueId(readKeyValueId) shouldBe true
    baseId = TransientToKeyValueIdBinder.UpdateBinder.keyValueId.adjustKeyValueIdToBaseId(readKeyValueId)
    typedBaseId = BaseEntryIdFormatA.baseIds.find(_.baseId == baseId).get
    //check the ids are correct types.
    typedBaseId shouldBe a[BaseEntryId.Value.NoValue]
    typedBaseId should not be a[BaseEntryId.ValueOffset.Uncompressed]
    typedBaseId should not be a[BaseEntryId.ValueLength.Uncompressed]
    //read the key-value giving it the previous key-value.
    val readNextKeyValue =
      EntryReader.read(
        indexReader = Reader(keyValues(1).indexEntryBytes),
        mightBeCompressed = randomBoolean(),
        valueCache = None,
        indexOffset = 0,
        nextIndexOffset = keyValues(1).indexEntryBytes.size,
        nextIndexSize = keyValues(2).indexEntryBytes.size,
        accessPosition = 0,
        previous = Some(readHeadKeyValue)
      ).get
    readNextKeyValue shouldBe a[Persistent.Update]
    readNextKeyValue.key shouldBe keyValues(1).key
    readNextKeyValue.asInstanceOf[Persistent.Update].getOrFetchValue.get shouldBe empty

    //THIRD KEY-VALUE
    keyValues(2).valueEntryBytes shouldBe empty
    readKeyValueId = keyValues(2).indexEntryBytes.readIntUnsigned().get
    TransientToKeyValueIdBinder.RemoveBinder.keyValueId.hasKeyValueId(readKeyValueId) shouldBe true
    baseId = TransientToKeyValueIdBinder.RemoveBinder.keyValueId.adjustKeyValueIdToBaseId(readKeyValueId)
    typedBaseId = BaseEntryIdFormatA.baseIds.find(_.baseId == baseId).get
    //check the ids are correct types.
    typedBaseId shouldBe a[BaseEntryId.Value.NoValue]
    typedBaseId should not be a[BaseEntryId.ValueOffset.Uncompressed]
    typedBaseId should not be a[BaseEntryId.ValueLength.Uncompressed]
    //read the key-value giving it the previous key-value.
    val readLastKeyValue =
      EntryReader.read(
        indexReader = Reader(keyValues(2).indexEntryBytes),
        mightBeCompressed = randomBoolean(),
        valueCache = None,
        indexOffset = 0,
        nextIndexOffset = 0,
        nextIndexSize = 0,
        accessPosition = 0,
        previous = Some(readNextKeyValue)
      ).get
    readLastKeyValue shouldBe a[Persistent.Remove]
    readLastKeyValue.key shouldBe keyValues(2).key
  }
}
