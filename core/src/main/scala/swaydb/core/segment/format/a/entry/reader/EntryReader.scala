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

import swaydb.core.data.Persistent
import swaydb.core.io.reader.Reader
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id._
import swaydb.core.segment.format.a.entry.reader.base._
import swaydb.data.slice.{ReaderBase, Slice}
import swaydb.data.util.Maybe
import swaydb.data.util.Maybe._

trait EntryReader[E] {
  def apply[T <: BaseEntryId](baseId: T,
                              keyValueId: Int,
                              sortedIndexEndOffset: Int,
                              sortedIndexAccessPosition: Int,
                              headerKeyBytes: Slice[Byte],
                              indexReader: ReaderBase,
                              valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                              indexOffset: Int,
                              previous: Option[Persistent])(implicit timeReader: TimeReader[T],
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

  val someUncompressedReader = Maybe.some(BaseEntryReaderUncompressed: BaseEntryReader)

  val zeroValueOffsetAndLength = (0, -1)

  def findReader(baseId: Int, mightBeCompressed: Boolean): Maybe[BaseEntryReader] =
    if (mightBeCompressed)
      readers.findMaybe(_.maxID >= baseId)
    else
      someUncompressedReader

  private def parse[T](baseId: Int,
                       keyValueId: Int,
                       sortedIndexEndOffset: Int,
                       sortedIndexAccessPosition: Int,
                       headerKeyBytes: Slice[Byte],
                       mightBeCompressed: Boolean,
                       indexReader: ReaderBase,
                       valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
                       indexOffset: Int,
                       previous: Option[Persistent],
                       entryReader: EntryReader[T]): T = {
    val baseEntryReaderMaybe = findReader(baseId = baseId, mightBeCompressed = mightBeCompressed)
    if (baseEntryReaderMaybe.isNone)
      throw swaydb.Exception.InvalidKeyValueId(baseId)
    else
      baseEntryReaderMaybe.read(
        baseId = baseId,
        keyValueId = keyValueId,
        sortedIndexEndOffset = sortedIndexEndOffset,
        sortedIndexAccessPosition = sortedIndexAccessPosition,
        headerKeyBytes = headerKeyBytes,
        indexReader = indexReader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        previous = previous,
        reader = entryReader
      )
  }

  def parse(headerInteger: Int,
            indexEntry: Slice[Byte],
            mightBeCompressed: Boolean,
            sortedIndexEndOffset: Int,
            valuesReader: Option[UnblockedReader[ValuesBlock.Offset, ValuesBlock]],
            indexOffset: Int,
            hasAccessPositionIndex: Boolean,
            previous: Option[Persistent]): Persistent = {
    //check if de-normalising is required.
    val reader = Reader(indexEntry)

    val key: Slice[Byte] = reader.read(headerInteger)

    val sortedIndexAccessPosition =
      if (hasAccessPositionIndex)
        reader.readUnsignedInt()
      else
        0

    val keyValueId = reader.readUnsignedInt()

    if (KeyValueId.Put hasKeyValueId keyValueId)
      EntryReader.parse(
        baseId = KeyValueId.Put.adjustKeyValueIdToBaseId(keyValueId),
        keyValueId = keyValueId,
        sortedIndexEndOffset = sortedIndexEndOffset,
        sortedIndexAccessPosition = sortedIndexAccessPosition,
        headerKeyBytes = key,
        mightBeCompressed = mightBeCompressed,
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
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
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
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
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
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
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
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
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
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
        indexReader = reader,
        valuesReader = valuesReader,
        indexOffset = indexOffset,
        previous = previous,
        entryReader = PendingApplyReader
      )
    else
      throw swaydb.Exception.InvalidKeyValueId(keyValueId)
  }
}
