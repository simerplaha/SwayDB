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

package swaydb.core.map.serializer

import java.util.concurrent.TimeUnit

import swaydb.IO
import swaydb.core.data.{Memory, Time, Value}
import swaydb.core.map.MapEntry
import swaydb.data.io.Core
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.io.Core.IO.Error.ErrorHandler

import scala.concurrent.duration.Deadline

object LevelZeroMapEntryReader {

  implicit object Level0RemoveReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Remove]] {

    override def read(reader: Reader[Core.IO.Error]): IO[Core.IO.Error, Option[MapEntry.Put[Slice[Byte], Memory.Remove]]] =
      for {
        keyLength <- reader.readInt()
        key <- reader.read(keyLength).map(_.unslice())
        timeLength <- reader.readInt()
        time <- reader.read(timeLength).map(_.unslice())
        deadlineLong <- reader.readLong()
      } yield {
        val deadline = if (deadlineLong == 0) None else Some(Deadline(deadlineLong, TimeUnit.NANOSECONDS))
        Some(MapEntry.Put(key, Memory.Remove(key, deadline, Time(time)))(LevelZeroMapEntryWriter.Level0RemoveWriter))
      }
  }

  implicit object Level0PutReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Put]] {

    override def read(reader: Reader[Core.IO.Error]): IO[Core.IO.Error, Option[MapEntry.Put[Slice[Byte], Memory.Put]]] =
      for {
        keyLength <- reader.readInt()
        key <- reader.read(keyLength).map(_.unslice())
        timeLength <- reader.readInt()
        time <- reader.read(timeLength).map(_.unslice())
        valueLength <- reader.readInt()
        value <- if (valueLength == 0) IO.none else reader.read(valueLength).map(Some(_))
        deadlineLong <- reader.readLong()
      } yield {
        val deadline = if (deadlineLong == 0) None else Some(Deadline(deadlineLong, TimeUnit.NANOSECONDS))
        Some(MapEntry.Put(key, Memory.Put(key, value, deadline, Time(time)))(LevelZeroMapEntryWriter.Level0PutWriter))
      }
  }

  implicit object Level0UpdateReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Update]] {

    override def read(reader: Reader[Core.IO.Error]): IO[Core.IO.Error, Option[MapEntry.Put[Slice[Byte], Memory.Update]]] =
      for {
        keyLength <- reader.readInt()
        key <- reader.read(keyLength).map(_.unslice())
        timeLength <- reader.readInt()
        time <- reader.read(timeLength).map(_.unslice())
        valueLength <- reader.readInt()
        value <- if (valueLength == 0) IO.none else reader.read(valueLength).map(Some(_))
        deadlineLong <- reader.readLong()
      } yield {
        val deadline = if (deadlineLong == 0) None else Some(Deadline(deadlineLong, TimeUnit.NANOSECONDS))
        Some(MapEntry.Put(key, Memory.Update(key, value, deadline, Time(time)))(LevelZeroMapEntryWriter.Level0UpdateWriter))
      }
  }

  implicit object Level0FunctionReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Function]] {

    override def read(reader: Reader[Core.IO.Error]): IO[Core.IO.Error, Option[MapEntry.Put[Slice[Byte], Memory.Function]]] =
      for {
        keyLength <- reader.readInt()
        key <- reader.read(keyLength).map(_.unslice())
        timeLength <- reader.readInt()
        time <- reader.read(timeLength).map(_.unslice())
        functionLength <- reader.readInt()
        value <- reader.read(functionLength)
      } yield {
        Some(MapEntry.Put(key, Memory.Function(key, value, Time(time)))(LevelZeroMapEntryWriter.Level0FunctionWriter))
      }
  }

  implicit object Level0RangeReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Range]] {

    override def read(reader: Reader[Core.IO.Error]): IO[Core.IO.Error, Option[MapEntry.Put[Slice[Byte], Memory.Range]]] =
      for {
        fromKeyLength <- reader.readInt()
        fromKey <- reader.read(fromKeyLength).map(_.unslice())
        toKeyLength <- reader.readInt()
        toKey <- reader.read(toKeyLength).map(_.unslice())
        valueLength <- reader.readInt()
        valueBytes <- if (valueLength == 0) IO.emptyBytes else reader.read(valueLength)
        (fromValue, rangeValue) <- RangeValueSerializer.read(valueBytes)
      } yield {
        Some(MapEntry.Put(fromKey, Memory.Range(fromKey, toKey, fromValue, rangeValue))(LevelZeroMapEntryWriter.Level0RangeWriter))
      }
  }

  implicit object Level0PendingApplyReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.PendingApply]] {

    override def read(reader: Reader[Core.IO.Error]): IO[Core.IO.Error, Option[MapEntry.Put[Slice[Byte], Memory.PendingApply]]] =
      for {
        keyLength <- reader.readInt()
        key <- reader.read(keyLength).map(_.unslice())
        valueLength <- reader.readInt()
        valueBytes <- reader.read(valueLength)
        applies <- ValueSerializer.read[Slice[Value.Apply]](valueBytes)
      } yield {
        Some(MapEntry.Put(key, Memory.PendingApply(key, applies))(LevelZeroMapEntryWriter.Level0PendingApplyWriter))
      }
  }

  implicit object Level0Reader extends MapEntryReader[MapEntry[Slice[Byte], Memory.SegmentResponse]] {
    private def merge(nextEntry: Option[MapEntry[Slice[Byte], Memory.SegmentResponse]],
                      previousEntry: Option[MapEntry[Slice[Byte], Memory.SegmentResponse]]) =
      nextEntry flatMap {
        nextEntry =>
          previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
      }

    override def read(reader: Reader[Core.IO.Error]): IO[Core.IO.Error, Option[MapEntry[Slice[Byte], Memory.SegmentResponse]]] =
      reader.foldLeftIO(Option.empty[MapEntry[Slice[Byte], Memory.SegmentResponse]]) {
        case (previousEntry, reader) =>
          reader.readInt() flatMap {
            entryId =>
              if (entryId == LevelZeroMapEntryWriter.Level0PutWriter.id)
                Level0PutReader.read(reader) map (merge(_, previousEntry))

              else if (entryId == LevelZeroMapEntryWriter.Level0RemoveWriter.id)
                Level0RemoveReader.read(reader) map (merge(_, previousEntry))

              else if (entryId == LevelZeroMapEntryWriter.Level0FunctionWriter.id)
                Level0FunctionReader.read(reader) map (merge(_, previousEntry))

              else if (entryId == LevelZeroMapEntryWriter.Level0PendingApplyWriter.id)
                Level0PendingApplyReader.read(reader) map (merge(_, previousEntry))

              else if (entryId == LevelZeroMapEntryWriter.Level0UpdateWriter.id)
                Level0UpdateReader.read(reader) map (merge(_, previousEntry))

              else if (entryId == LevelZeroMapEntryWriter.Level0RangeWriter.id)
                Level0RangeReader.read(reader) map (merge(_, previousEntry))

              else
                IO.failed(new IllegalArgumentException(s"Invalid entry type $entryId."))
          }
      }
  }
}
