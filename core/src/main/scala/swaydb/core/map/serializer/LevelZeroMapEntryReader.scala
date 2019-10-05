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

import swaydb.Error.Map.ExceptionHandler
import swaydb.IO
import swaydb.core.data.{Memory, Time, Value}
import swaydb.core.map.MapEntry
import swaydb.data.slice.{ReaderBase, Slice}

import scala.concurrent.duration.Deadline

object LevelZeroMapEntryReader {

  implicit object Level0RemoveReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Remove]] {

    override def read(reader: ReaderBase): IO[swaydb.Error.Map, Option[MapEntry.Put[Slice[Byte], Memory.Remove]]] =
      IO {
        val keyLength = reader.readInt()
        val key = reader.read(keyLength).unslice()
        val timeLength = reader.readInt()
        val time = reader.read(timeLength).unslice()
        val deadlineLong = reader.readLong()
        val deadline = if (deadlineLong == 0) None else Some(Deadline(deadlineLong, TimeUnit.NANOSECONDS))
        Some(MapEntry.Put(key, Memory.Remove(key, deadline, Time(time)))(LevelZeroMapEntryWriter.Level0RemoveWriter))
      }
  }

  implicit object Level0PutReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Put]] {

    override def read(reader: ReaderBase): IO[swaydb.Error.Map, Option[MapEntry.Put[Slice[Byte], Memory.Put]]] =
      IO {
        val keyLength = reader.readInt()
        val key = reader.read(keyLength).unslice()
        val timeLength = reader.readInt()
        val time = reader.read(timeLength).unslice()
        val valueLength = reader.readInt()
        val value = if (valueLength == 0) None else Some(reader.read(valueLength))
        val deadlineLong = reader.readLong()

        val deadline = if (deadlineLong == 0) None else Some(Deadline(deadlineLong, TimeUnit.NANOSECONDS))
        Some(MapEntry.Put(key, Memory.Put(key, value, deadline, Time(time)))(LevelZeroMapEntryWriter.Level0PutWriter))
      }
  }

  implicit object Level0UpdateReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Update]] {

    override def read(reader: ReaderBase): IO[swaydb.Error.Map, Option[MapEntry.Put[Slice[Byte], Memory.Update]]] =
      IO {
        val keyLength = reader.readInt()
        val key = reader.read(keyLength).unslice()
        val timeLength = reader.readInt()
        val time = reader.read(timeLength).unslice()
        val valueLength = reader.readInt()
        val value = if (valueLength == 0) None else Some(reader.read(valueLength))
        val deadlineLong = reader.readLong()

        val deadline = if (deadlineLong == 0) None else Some(Deadline(deadlineLong, TimeUnit.NANOSECONDS))
        Some(MapEntry.Put(key, Memory.Update(key, value, deadline, Time(time)))(LevelZeroMapEntryWriter.Level0UpdateWriter))
      }
  }

  implicit object Level0FunctionReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Function]] {

    override def read(reader: ReaderBase): IO[swaydb.Error.Map, Option[MapEntry.Put[Slice[Byte], Memory.Function]]] =
      IO {
        val keyLength = reader.readInt()
        val key = reader.read(keyLength).unslice()
        val timeLength = reader.readInt()
        val time = reader.read(timeLength).unslice()
        val functionLength = reader.readInt()
        val value = reader.read(functionLength)

        Some(MapEntry.Put(key, Memory.Function(key, value, Time(time)))(LevelZeroMapEntryWriter.Level0FunctionWriter))
      }
  }

  implicit object Level0RangeReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Range]] {

    override def read(reader: ReaderBase): IO[swaydb.Error.Map, Option[MapEntry.Put[Slice[Byte], Memory.Range]]] =
      IO {
        val fromKeyLength = reader.readInt()
        val fromKey = reader.read(fromKeyLength).unslice()
        val toKeyLength = reader.readInt()
        val toKey = reader.read(toKeyLength).unslice()
        val valueLength = reader.readInt()
        val valueBytes = if (valueLength == 0) Slice.emptyBytes else reader.read(valueLength)
        val (fromValue, rangeValue) = RangeValueSerializer.read(valueBytes)

        Some(MapEntry.Put(fromKey, Memory.Range(fromKey, toKey, fromValue, rangeValue))(LevelZeroMapEntryWriter.Level0RangeWriter))
      }
  }

  implicit object Level0PendingApplyReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.PendingApply]] {

    override def read(reader: ReaderBase): IO[swaydb.Error.Map, Option[MapEntry.Put[Slice[Byte], Memory.PendingApply]]] =
      IO {
        val keyLength = reader.readInt()
        val key = reader.read(keyLength).unslice()
        val valueLength = reader.readInt()
        val valueBytes = reader.read(valueLength)
        val applies = ValueSerializer.read[Slice[Value.Apply]](valueBytes)

        Some(MapEntry.Put(key, Memory.PendingApply(key, applies))(LevelZeroMapEntryWriter.Level0PendingApplyWriter))
      }
  }

  implicit object Level0Reader extends MapEntryReader[MapEntry[Slice[Byte], Memory]] {
    private def merge(nextEntry: Option[MapEntry[Slice[Byte], Memory]],
                      previousEntry: Option[MapEntry[Slice[Byte], Memory]]) =
      nextEntry flatMap {
        nextEntry =>
          previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
      }

    override def read(reader: ReaderBase): IO[swaydb.Error.Map, Option[MapEntry[Slice[Byte], Memory]]] =
      reader.foldLeftIO(Option.empty[MapEntry[Slice[Byte], Memory]]) {
        case (previousEntry, reader) =>
          IO(reader.readInt()) flatMap {
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
