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

package swaydb.core.segment.format.a.entry.reader

import swaydb.IO
import swaydb.core.data.Persistent
import swaydb.core.segment.format.a.block.ValuesBlock
import swaydb.core.segment.format.a.block.reader.UnblockedReader
import swaydb.core.segment.format.a.entry.id._
import swaydb.core.segment.format.a.entry.reader.base._
import swaydb.core.util.cache.Cache
import swaydb.data.io.Core
import swaydb.data.slice.Reader

trait EntryReader[E] {
  def apply[T <: BaseEntryId](baseId: T,
                              keyValueId: Int,
                              indexReader: Reader[Core.Error.Segment],
                              valueCache: Option[Cache[Core.Error.Segment, ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]]],
                              indexOffset: Int,
                              nextIndexOffset: Int,
                              nextIndexSize: Int,
                              accessPosition: Int,
                              previous: Option[Persistent])(implicit timeReader: TimeReader[T],
                                                            deadlineReader: DeadlineReader[T],
                                                            valueOffsetReader: ValueOffsetReader[T],
                                                            valueLengthReader: ValueLengthReader[T],
                                                            valueBytesReader: ValueReader[T]): IO[Core.Error.Segment, E]
}

object EntryReader {

  val readers: List[BaseEntryReader] =
    List(BaseEntryReader1, BaseEntryReader2, BaseEntryReader3, BaseEntryReader4) sortBy (_.minID)

  val someUncompressedReader = Some(BaseEntryReaderUncompressed)

  def findReader(baseId: Int, mightBeCompressed: Boolean): Option[BaseEntryReader] =
    if (mightBeCompressed)
      readers.find(_.maxID >= baseId)
    else
      someUncompressedReader

  def read[T](baseId: Int,
              keyValueId: Int,
              mightBeCompressed: Boolean,
              indexReader: Reader[Core.Error.Segment],
              valueCache: Option[Cache[Core.Error.Segment, ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]]],
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              accessPosition: Int,
              previous: Option[Persistent],
              entryReader: EntryReader[T]): IO[Core.Error.Segment, T] =
    findReader(baseId = baseId, mightBeCompressed = mightBeCompressed) flatMap {
      entry =>
        entry.read(
          baseId = baseId,
          keyValueId = keyValueId,
          indexReader = indexReader,
          valueCache = valueCache,
          indexOffset = indexOffset,
          nextIndexOffset = nextIndexOffset,
          nextIndexSize = nextIndexSize,
          accessPosition = accessPosition,
          previous = previous,
          reader = entryReader
        )
    } getOrElse IO.failed(Core.Exception.InvalidKeyValueId(baseId))

  def read(indexReader: Reader[Core.Error.Segment],
           mightBeCompressed: Boolean,
           valueCache: Option[Cache[Core.Error.Segment, ValuesBlock.Offset, UnblockedReader[ValuesBlock.Offset, ValuesBlock]]],
           indexOffset: Int,
           nextIndexOffset: Int,
           nextIndexSize: Int,
           accessPosition: Int,
           previous: Option[Persistent]): IO[Core.Error.Segment, Persistent] =
    indexReader.readIntUnsigned() flatMap {
      keyValueId =>
        if (KeyValueId.Put.hasKeyValueId(keyValueId))
          EntryReader.read(
            baseId = KeyValueId.Put.adjustKeyValueIdToBaseId(keyValueId),
            keyValueId = keyValueId,
            mightBeCompressed = mightBeCompressed,
            indexReader = indexReader,
            valueCache = valueCache,
            indexOffset = indexOffset,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            accessPosition = accessPosition,
            previous = previous,
            entryReader = PutReader
          )
        else if (KeyValueId.Group.hasKeyValueId(keyValueId))
          EntryReader.read(
            baseId = KeyValueId.Group.adjustKeyValueIdToBaseId(keyValueId),
            keyValueId = keyValueId,
            mightBeCompressed = mightBeCompressed,
            indexReader = indexReader,
            valueCache = valueCache,
            indexOffset = indexOffset,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            accessPosition = accessPosition,
            previous = previous,
            entryReader = GroupReader
          )
        else if (KeyValueId.Range.hasKeyValueId(keyValueId))
          EntryReader.read(
            baseId = KeyValueId.Range.adjustKeyValueIdToBaseId(keyValueId),
            keyValueId = keyValueId,
            mightBeCompressed = mightBeCompressed,
            indexReader = indexReader,
            valueCache = valueCache,
            indexOffset = indexOffset,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            accessPosition = accessPosition,
            previous = previous,
            entryReader = RangeReader
          )
        else if (KeyValueId.Remove.hasKeyValueId(keyValueId))
          EntryReader.read(
            baseId = KeyValueId.Remove.adjustKeyValueIdToBaseId(keyValueId),
            keyValueId = keyValueId,
            mightBeCompressed = mightBeCompressed,
            indexReader = indexReader,
            valueCache = valueCache,
            indexOffset = indexOffset,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            accessPosition = accessPosition,
            previous = previous,
            entryReader = RemoveReader
          )
        else if (KeyValueId.Update.hasKeyValueId(keyValueId))
          EntryReader.read(
            baseId = KeyValueId.Update.adjustKeyValueIdToBaseId(keyValueId),
            keyValueId = keyValueId,
            mightBeCompressed = mightBeCompressed,
            indexReader = indexReader,
            valueCache = valueCache,
            indexOffset = indexOffset,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            accessPosition = accessPosition,
            previous = previous,
            entryReader = UpdateReader
          )
        else if (KeyValueId.Function.hasKeyValueId(keyValueId))
          EntryReader.read(
            baseId = KeyValueId.Function.adjustKeyValueIdToBaseId(keyValueId),
            keyValueId = keyValueId,
            mightBeCompressed = mightBeCompressed,
            indexReader = indexReader,
            valueCache = valueCache,
            indexOffset = indexOffset,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            accessPosition = accessPosition,
            previous = previous,
            entryReader = FunctionReader
          )
        else if (KeyValueId.PendingApply.hasKeyValueId(keyValueId))
          EntryReader.read(
            baseId = KeyValueId.PendingApply.adjustKeyValueIdToBaseId(keyValueId),
            keyValueId = keyValueId,
            mightBeCompressed = mightBeCompressed,
            indexReader = indexReader,
            valueCache = valueCache,
            indexOffset = indexOffset,
            nextIndexOffset = nextIndexOffset,
            nextIndexSize = nextIndexSize,
            accessPosition = accessPosition,
            previous = previous,
            entryReader = PendingApplyReader
          )
        else
          IO.failed[Core.Error.Segment, Persistent](Core.Exception.InvalidKeyValueId(keyValueId))
    }
}
