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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.a.entry.reader

import swaydb.data.io.IO
import swaydb.core.data.Persistent
import swaydb.core.segment.SegmentException
import swaydb.core.segment.format.a.entry.id._
import swaydb.core.segment.format.a.entry.reader.base._

import swaydb.data.io.IO._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Reader, Slice}

trait EntryReader[E] {
  def apply[T <: EntryId](id: T,
                          indexReader: Reader,
                          valueReader: Reader,
                          indexOffset: Int,
                          nextIndexOffset: Int,
                          nextIndexSize: Int,
                          previous: Option[Persistent])(implicit keyReader: KeyReader[T],
                                                        timeReader: TimeReader[T],
                                                        deadlineReader: DeadlineReader[T],
                                                        valueOffsetReader: ValueOffsetReader[T],
                                                        valueLengthReader: ValueLengthReader[T],
                                                        valueBytesReader: ValueReader[T]): IO[E]
}

object EntryReader {

  val readers =
    Seq(
      BaseEntryReader1, BaseEntryReader2, BaseEntryReader3,
      BaseEntryReader4, BaseEntryReader5, BaseEntryReader6,
      BaseEntryReader7, BaseEntryReader8, BaseEntryReader9,
      BaseEntryReader10, BaseEntryReader11, BaseEntryReader12,
      BaseEntryReader13, BaseEntryReader14, BaseEntryReader15
    )

  def read[T](id: Int,
              indexReader: Reader,
              valueReader: Reader,
              indexOffset: Int,
              nextIndexOffset: Int,
              nextIndexSize: Int,
              previous: Option[Persistent],
              entryReader: EntryReader[T])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[T] =
    readers.foldLeftIO(None) {
      case (_, reader) =>
        reader.read(
          id = id,
          indexReader = indexReader,
          valueReader = valueReader,
          indexOffset = indexOffset,
          nextIndexOffset = nextIndexOffset,
          nextIndexSize = nextIndexSize,
          previous = previous,
          reader = entryReader
        ) match {
          case Some(value) =>
            return value
          case None =>
            IO.none
        }
    } flatMap {
      _ =>
        IO.Failure(IO.Error.Fatal(SegmentException.InvalidEntryId(id)))
    }

  def read(indexReader: Reader,
           valueReader: Reader,
           indexOffset: Int,
           nextIndexOffset: Int,
           nextIndexSize: Int,
           previous: Option[Persistent])(implicit keyOrder: KeyOrder[Slice[Byte]]): IO[Persistent] =
    indexReader.readIntUnsigned() flatMap {
      id =>
        if (EntryId.Put.hasId(id))
          EntryReader.read(EntryId.Put.adjustToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, PutReader)
        else if (EntryId.Group.hasId(id))
          EntryReader.read(EntryId.Group.adjustToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, GroupReader)
        else if (EntryId.Range.hasId(id))
          EntryReader.read(EntryId.Range.adjustToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, RangeReader)
        else if (EntryId.Remove.hasId(id))
          EntryReader.read(EntryId.Remove.adjustToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, RemoveReader)
        else if (EntryId.Update.hasId(id))
          EntryReader.read(EntryId.Update.adjustToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, UpdateReader)
        else if (EntryId.Function.hasId(id))
          EntryReader.read(EntryId.Function.adjustToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, FunctionReader)
        else if (EntryId.PendingApply.hasId(id))
          EntryReader.read(EntryId.PendingApply.adjustToBaseId(id), indexReader, valueReader, indexOffset, nextIndexOffset, nextIndexSize, previous, PendingApplyReader)
        else
          IO.Failure(IO.Error.Fatal(SegmentException.InvalidEntryId(id)))
    }
}
