/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.core.map.serializer

import java.util.concurrent.TimeUnit

import swaydb.core.data.Memory
import swaydb.core.map.MapEntry
import swaydb.core.util.TryUtil
import swaydb.data.slice.{Reader, Slice}

import scala.concurrent.duration.Deadline
import scala.util.{Failure, Success, Try}

object LevelZeroMapEntryReader {

  implicit object Level0RemoveReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Remove]] {

    override def read(reader: Reader): Try[Option[MapEntry.Put[Slice[Byte], Memory.Remove]]] =
      for {
        keyLength <- reader.readInt()
        key <- reader.read(keyLength).map(_.unslice())
        after <- reader.readLong()
      } yield {
        val deadline = if (after == 0) None else Some(Deadline(after, TimeUnit.NANOSECONDS))
        Some(MapEntry.Put(key, Memory.Remove(key, deadline))(LevelZeroMapEntryWriter.Level0RemoveWriter))
      }
  }

  implicit object Level0PutReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Put]] {

    override def read(reader: Reader): Try[Option[MapEntry.Put[Slice[Byte], Memory.Put]]] =
      for {
        keyLength <- reader.readInt()
        key <- reader.read(keyLength).map(_.unslice())
        valueLength <- reader.readInt()
        value <- if (valueLength == 0) TryUtil.successNone else reader.read(valueLength).map(Some(_))
        after <- reader.readLong()
      } yield {
        val deadline = if (after == 0) None else Some(Deadline(after, TimeUnit.NANOSECONDS))
        Some(MapEntry.Put(key, Memory.Put(key, value, deadline))(LevelZeroMapEntryWriter.Level0PutWriter))
      }
  }

  implicit object Level0UpdateReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Update]] {

    override def read(reader: Reader): Try[Option[MapEntry.Put[Slice[Byte], Memory.Update]]] =
      for {
        keyLength <- reader.readInt()
        key <- reader.read(keyLength).map(_.unslice())
        valueLength <- reader.readInt()
        value <- if (valueLength == 0) TryUtil.successNone else reader.read(valueLength).map(Some(_))
        after <- reader.readLong()
      } yield {
        val deadline = if (after == 0) None else Some(Deadline(after, TimeUnit.NANOSECONDS))
        Some(MapEntry.Put(key, Memory.Update(key, value, deadline))(LevelZeroMapEntryWriter.Level0UpdateWriter))
      }
  }

  implicit object Level0RangeReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Range]] {

    override def read(reader: Reader): Try[Option[MapEntry.Put[Slice[Byte], Memory.Range]]] =
      for {
        fromKeyLength <- reader.readInt()
        fromKey <- reader.read(fromKeyLength).map(_.unslice())
        toKeyLength <- reader.readInt()
        toKey <- reader.read(toKeyLength).map(_.unslice())
        valueLength <- reader.readInt()
        valueBytes <- if (valueLength == 0) Success(Slice.emptyBytes) else reader.read(valueLength)
        (fromValue, rangeValue) <- RangeValueSerializer.read(valueBytes)
      } yield {
        Some(MapEntry.Put(fromKey, Memory.Range(fromKey, toKey, fromValue, rangeValue))(LevelZeroMapEntryWriter.Level0RangeWriter))
      }
  }

  implicit object Level0Reader extends MapEntryReader[MapEntry[Slice[Byte], Memory.Response]] {
    override def read(reader: Reader): Try[Option[MapEntry[Slice[Byte], Memory.Response]]] =
      reader.foldLeftTry(Option.empty[MapEntry[Slice[Byte], Memory.Response]]) {
        case (previousEntry, reader) =>
          reader.readInt() flatMap {
            entryId =>
              if (entryId == LevelZeroMapEntryWriter.Level0PutWriter.id)
                Level0PutReader.read(reader) map {
                  nextEntry =>
                    nextEntry flatMap {
                      nextEntry =>
                        previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
                    }
                }
              else if (entryId == LevelZeroMapEntryWriter.Level0RemoveWriter.id)
                Level0RemoveReader.read(reader) map {
                  nextEntry =>
                    nextEntry flatMap {
                      nextEntry =>
                        previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
                    }
                }
              else if (entryId == LevelZeroMapEntryWriter.Level0RangeWriter.id)
                Level0RangeReader.read(reader) map {
                  nextEntry =>
                    nextEntry flatMap {
                      nextEntry =>
                        previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
                    }
                }
              else if (entryId == LevelZeroMapEntryWriter.Level0UpdateWriter.id)
                Level0UpdateReader.read(reader) map {
                  nextEntry =>
                    nextEntry flatMap {
                      nextEntry =>
                        previousEntry.map(_ ++ nextEntry) orElse Some(nextEntry)
                    }
                }
              else
                Failure(new IllegalArgumentException(s"Invalid entry type $entryId."))
          }
      }
  }
}