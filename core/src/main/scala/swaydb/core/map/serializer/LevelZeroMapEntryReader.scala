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

import swaydb.core.data.Value
import swaydb.core.map.MapEntry
import swaydb.data.slice.{Reader, Slice}

import scala.util.{Failure, Try}

object LevelZeroMapEntryReader {

  import ValueSerializers._

  implicit object Level0AddReader extends MapEntryReader[MapEntry.Add[Slice[Byte], Value.Put]] {

    override def read(reader: Reader): Try[Option[MapEntry.Add[Slice[Byte], Value.Put]]] =
      for {
        keyLength <- reader.readInt()
        key <- reader.read(keyLength).map(_.unslice())
        value <- ValueSerializer.read[Value.Put](reader)
      } yield {
        Some(MapEntry.Add(key, value)(LevelZeroMapEntryWriter.Level0AddWriter))
      }
  }

  implicit object Level0RemoveReader extends MapEntryReader[MapEntry.Add[Slice[Byte], Value.Remove]] {

    override def read(reader: Reader): Try[Option[MapEntry.Add[Slice[Byte], Value.Remove]]] =
      for {
        keyLength <- reader.readInt()
        key <- reader.read(keyLength).map(_.unslice())
      } yield {
        Some(MapEntry.Add[Slice[Byte], Value.Remove](key, Value.Remove)(LevelZeroMapEntryWriter.Level0RemoveWriter))
      }
  }

  implicit object Level0Reader extends MapEntryReader[MapEntry[Slice[Byte], Value]] {
    override def read(reader: Reader): Try[Option[MapEntry[Slice[Byte], Value]]] =
      reader.foldLeftTry(Option.empty[MapEntry[Slice[Byte], Value]]) {
        case (previousEntry, reader) =>
          reader.readInt() flatMap {
            entryId =>
              if (entryId == LevelZeroMapEntryWriter.Level0AddWriter.id)
                Level0AddReader.read(reader) map {
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
              else
                Failure(new IllegalArgumentException(s"Invalid entry type $entryId."))
          }
      }
  }
}