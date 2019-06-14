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
import swaydb.core.segment.SegmentException
import swaydb.core.segment.format.a.entry.id._
import swaydb.core.segment.format.a.entry.reader.base._
import swaydb.data.IO
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}

trait EntryReader[E] {
  def apply[T <: BaseEntryId](id: T,
                              indexReader: Reader,
                              valueReader: Reader,
                              indexOffset: Int,
                              nextIndexOffset: Int,
                              nextIndexSize: Int,
                              previous: Option[Persistent])(implicit timeReader: TimeReader[T],
                                                        deadlineReader: DeadlineReader[T],
                                                        valueOffsetReader: ValueOffsetReader[T],
                                                        valueLengthReader: ValueLengthReader[T],
                                                        valueBytesReader: ValueReader[T]): IO[E]
}

object EntryReader {

  val readers: List[BaseEntryReader] =
    List(BaseEntryReader1, BaseEntryReader2, BaseEntryReader3, BaseEntryReader4) sortBy (_.minID)

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              entryReader: EntryReader[T])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[T] =
    readers.find(_.maxID >= id) flatMap {
      entry =>
        entry.read(
          id = id,
          indexReader = indexReader,
          valueReader = valueReader,
          indexOffset = indexOffset,
          nextIndexOffset = nextIndexOffset,
          nextIndexSize = nextIndexSize,
          previous = previous,
          reader = entryReader
        )
    } getOrElse IO.Failure(IO.Error.Fatal(SegmentException.InvalidEntryId(id)))

  def read(indexReader: Reader,
           valueReader: Reader,
           indexOffset: Int,
           nextIndexOffset: Int,
           nextIndexSize: Int,
           previous: Option[Persistent])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Persistent] =
    indexReader.readIntUnsigned() flatMap {
      id =>
        if (KeyValueId.Put.hasKeyValueId(id))
          EntryReader.read(KeyValueId.Put.adjustKeyValueIdToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, PutReader)
        else if (KeyValueId.Group.hasKeyValueId(id))
          EntryReader.read(KeyValueId.Group.adjustKeyValueIdToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, GroupReader)
        else if (KeyValueId.Range.hasKeyValueId(id))
          EntryReader.read(KeyValueId.Range.adjustKeyValueIdToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, RangeReader)
        else if (KeyValueId.Remove.hasKeyValueId(id))
          EntryReader.read(KeyValueId.Remove.adjustKeyValueIdToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, RemoveReader)
        else if (KeyValueId.Update.hasKeyValueId(id))
          EntryReader.read(KeyValueId.Update.adjustKeyValueIdToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, UpdateReader)
        else if (KeyValueId.Function.hasKeyValueId(id))
          EntryReader.read(KeyValueId.Function.adjustKeyValueIdToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, FunctionReader)
        else if (KeyValueId.PendingApply.hasKeyValueId(id))
          EntryReader.read(KeyValueId.PendingApply.adjustKeyValueIdToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, PendingApplyReader)
        else
          IO.Failure(IO.Error.Fatal(SegmentException.InvalidEntryId(id)))
    }
}
