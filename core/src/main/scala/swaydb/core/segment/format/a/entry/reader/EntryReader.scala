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

package swaydb.core.segment.format.a.entry.reader

import swaydb.core.data.{Persistent, PersistentOptional}
import swaydb.core.data.Persistent.Partial
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.block.{SortedIndexBlock, ValuesBlock}
import swaydb.core.segment.format.a.entry.id._
import swaydb.core.segment.format.a.entry.reader.base._
import swaydb.core.util.{Bytes, NullOps}
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.data.util.Maybe

trait EntryReader[E] {
  def apply[T <: BaseEntryId](baseId: T,
                              keyValueId: Int,
                              sortedIndexEndOffset: Int,
                              sortedIndexAccessPosition: Int,
                              headerKeyBytes: Slice[Byte],
                              indexReader: ReaderBase,
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                              indexOffset: Int,
                              normalisedByteSize: Int,
                              previous: PersistentOptional)(implicit timeReader: TimeReader[T],
                                                            deadlineReader: DeadlineReader[T],
                                                            valueOffsetReader: ValueOffsetReader[T],
                                                            valueLengthReader: ValueLengthReader[T],
                                                            valueBytesReader: ValueReader[T]): E
}

object EntryReader {

  val readers: Array[BaseEntryReader] =
    Array(
      BaseEntryReader1,
      BaseEntryReader2,
      BaseEntryReader3,
      BaseEntryReader4
    ) sortBy (_.minID)

  val zeroValueOffsetAndLength = (-1, 0)

  def findReaderNullable(baseId: Int,
                         mightBeCompressed: Boolean,
                         keyCompressionOnly: Boolean): BaseEntryReader =
    if (mightBeCompressed && !keyCompressionOnly)
      NullOps.find[BaseEntryReader](readers, _.maxID >= baseId)
    else
      BaseEntryReaderUncompressed

  private def parse[T](baseId: Int,
                       keyValueId: Int,
                       sortedIndexEndOffset: Int,
                       sortedIndexAccessPosition: Int,
                       headerKeyBytes: Slice[Byte],
                       mightBeCompressed: Boolean,
                       keyCompressionOnly: Boolean,
                       indexReader: ReaderBase,
                       valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                       indexOffset: Int,
                       normalisedByteSize: Int,
                       previous: PersistentOptional,
                       entryReader: EntryReader[T]): T = {
    val baseEntryReaderNullable =
      findReaderNullable(
        baseId = baseId,
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly
      )

    if (baseEntryReaderNullable == null)
      throw swaydb.Exception.InvalidKeyValueId(baseId)
    else
      baseEntryReaderNullable.read(
        baseId = baseId,
        keyValueId = keyValueId,
        sortedIndexEndOffset = sortedIndexEndOffset,
        sortedIndexAccessPosition = sortedIndexAccessPosition,
        headerKeyBytes = headerKeyBytes,
        indexReader = indexReader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        normalisedByteSize = normalisedByteSize,
        previous = previous,
        reader = entryReader
      )
  }

  def parse(headerInteger: Int,
            indexEntry: Slice[Byte], //does not contain headerInteger bytes.
            mightBeCompressed: Boolean,
            keyCompressionOnly: Boolean,
            sortedIndexEndOffset: Int,
            valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
            indexOffset: Int,
            hasAccessPositionIndex: Boolean,
            normalisedByteSize: Int,
            previous: PersistentOptional): Persistent = {
    //check if de-normalising is required.
    val reader = Reader(indexEntry)

    val key: Slice[Byte] = reader.read(headerInteger)

    val keyValueId = reader.readUnsignedInt()

    val sortedIndexAccessPosition =
      if (hasAccessPositionIndex)
        reader.readUnsignedInt()
      else
        0

    if (KeyValueId.Put hasKeyValueId keyValueId)
      EntryReader.parse(
        baseId = KeyValueId.Put.adjustKeyValueIdToBaseId(keyValueId),
        keyValueId = keyValueId,
        sortedIndexEndOffset = sortedIndexEndOffset,
        sortedIndexAccessPosition = sortedIndexAccessPosition,
        headerKeyBytes = key,
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        normalisedByteSize = normalisedByteSize,
        previous = previous,
        entryReader = PutReader
      )
    else if (KeyValueId.Range hasKeyValueId keyValueId)
      EntryReader.parse(
        baseId = KeyValueId.Range.adjustKeyValueIdToBaseId(keyValueId),
        keyValueId = keyValueId,
        sortedIndexEndOffset = sortedIndexEndOffset,
        sortedIndexAccessPosition = sortedIndexAccessPosition,
        headerKeyBytes = key,
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        normalisedByteSize = normalisedByteSize,
        previous = previous,
        entryReader = RangeReader
      )
    else if (KeyValueId.Remove hasKeyValueId keyValueId)
      EntryReader.parse(
        baseId = KeyValueId.Remove.adjustKeyValueIdToBaseId(keyValueId),
        keyValueId = keyValueId,
        sortedIndexEndOffset = sortedIndexEndOffset,
        sortedIndexAccessPosition = sortedIndexAccessPosition,
        headerKeyBytes = key,
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        normalisedByteSize = normalisedByteSize,
        previous = previous,
        entryReader = RemoveReader
      )
    else if (KeyValueId.Update hasKeyValueId keyValueId)
      EntryReader.parse(
        baseId = KeyValueId.Update.adjustKeyValueIdToBaseId(keyValueId),
        keyValueId = keyValueId,
        sortedIndexEndOffset = sortedIndexEndOffset,
        sortedIndexAccessPosition = sortedIndexAccessPosition,
        headerKeyBytes = key,
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        normalisedByteSize = normalisedByteSize,
        previous = previous,
        entryReader = UpdateReader
      )
    else if (KeyValueId.Function hasKeyValueId keyValueId)
      EntryReader.parse(
        baseId = KeyValueId.Function.adjustKeyValueIdToBaseId(keyValueId),
        keyValueId = keyValueId,
        sortedIndexEndOffset = sortedIndexEndOffset,
        sortedIndexAccessPosition = sortedIndexAccessPosition,
        headerKeyBytes = key,
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        normalisedByteSize = normalisedByteSize,
        previous = previous,
        entryReader = FunctionReader
      )
    else if (KeyValueId.PendingApply hasKeyValueId keyValueId)
      EntryReader.parse(
        baseId = KeyValueId.PendingApply.adjustKeyValueIdToBaseId(keyValueId),
        keyValueId = keyValueId,
        sortedIndexEndOffset = sortedIndexEndOffset,
        sortedIndexAccessPosition = sortedIndexAccessPosition,
        headerKeyBytes = key,
        mightBeCompressed = mightBeCompressed,
        keyCompressionOnly = keyCompressionOnly,
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        normalisedByteSize = normalisedByteSize,
        previous = previous,
        entryReader = PendingApplyReader
      )
    else
      throw swaydb.Exception.InvalidKeyValueId(keyValueId)
  }

  def parsePartial(offset: Int,
                   headerInteger: Int,
                   indexEntry: ReaderBase,
                   sortedIndex: UnblockedReader[SortedIndexBlock.Offset, SortedIndexBlock],
                   valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]]): Persistent.Partial = {

    val entryKey: Slice[Byte] = indexEntry.read(headerInteger)

    val keyValueId = indexEntry.readUnsignedInt()

    if (KeyValueId isFixedId keyValueId)
      new Partial.Fixed {
        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          entryKey

        override def toPersistent: Persistent =
          SortedIndexBlock.read(
            fromOffset = offset,
            sortedIndexReader = sortedIndex,
            valuesReader = valuesReader
          )
      }
    else if (KeyValueId.Range hasKeyValueId keyValueId)
      new Partial.Range {
        val (fromKey, toKey) = Bytes.decompressJoin(entryKey)

        override def indexOffset: Int =
          offset

        override def key: Slice[Byte] =
          fromKey

        override def toPersistent: Persistent =
          SortedIndexBlock.read(
            fromOffset = offset,
            sortedIndexReader = sortedIndex,
            valuesReader = valuesReader
          )
      }
    else
      throw new Exception(s"Invalid keyType: $keyValueId, offset: $offset, headerInteger: $headerInteger")
  }

  /**
   * Given enough information about the currently parsed key-value calculates next key-value indexOffset and also the header
   * integer (key-size).
   *
   * @param sortedIndexEndOffset           end offset of the sorted index block only (starts from 0). Does not include file offset.
   * @param previousKeyValueHeaderKeyBytes header key bytes already read.
   * @param previousKeyValueIndexReader    reader for the current entry.
   * @param previousKeyValueIndexOffset    this key-values index offset used to calculate next key-values indexOffset and header key byte size.
   * @param normalisedByteSize             normalised size for entry sorted index entry. 0 if not normalised.
   * @return [[Tuple2]] that contains the indexOffset of next key-value and next key-values size.
   */
  def calculateNextKeyValueOffsetAndSize(sortedIndexEndOffset: Int,
                                         previousKeyValueHeaderKeyBytes: Slice[Byte],
                                         previousKeyValueIndexReader: ReaderBase,
                                         previousKeyValueIndexOffset: Int,
                                         normalisedByteSize: Int): (Int, Int) = {
    val bytesRead =
      Bytes.sizeOfUnsignedInt(previousKeyValueHeaderKeyBytes.size) +
        previousKeyValueIndexReader.getPosition

    val nextIndexOffsetMaybe =
      if (normalisedByteSize > 0)
        previousKeyValueIndexOffset + normalisedByteSize - 1 //skip the zeroes if the indexEntry was normalised.
      else
        previousKeyValueIndexOffset + bytesRead - 1

    val (nextIndexOffset, nextKeySize) =
      if (nextIndexOffsetMaybe == sortedIndexEndOffset) {
        EntryReader.zeroValueOffsetAndLength //(-1, 0): -1 indicates last key-value.
      } else {
        val nextIndexSize: Int =
          if (normalisedByteSize > 0)
            previousKeyValueIndexReader //skip the zeroes if the indexEntry was normalised.
              .skip(normalisedByteSize - bytesRead)
              .readUnsignedInt()
          else
            previousKeyValueIndexReader.readUnsignedInt()

        (nextIndexOffsetMaybe + 1, nextIndexSize)
      }

    //temporary check to ensure that only the required bytes are read.
    assert(previousKeyValueIndexOffset + bytesRead - 1 <= sortedIndexEndOffset, s"Read more: ${previousKeyValueIndexOffset + bytesRead - 1} not <= $sortedIndexEndOffset")

    (nextIndexOffset, nextKeySize)
  }
}
