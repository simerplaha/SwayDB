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
import swaydb.core.data.Value._
import swaydb.core.map.MapEntry
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

object LevelZeroMapEntryWriter {

  import ValueSerializers._

  implicit object Level0AddWriter extends MapEntryWriter[MapEntry.Add[Slice[Byte], Value.Put]] {
    override val id: Int = 1

    override def write(entry: MapEntry.Add[Slice[Byte], Value.Put], bytes: Slice[Byte]): Unit =
      ValueSerializer.write(entry.value) {
        bytes
          .addInt(id)
          .addInt(entry.key.size)
          .addAll(entry.key)
      }

    override def bytesRequired(entry: MapEntry.Add[Slice[Byte], Value.Put]): Int =
      if (entry.key.isEmpty)
        0
      else
        ByteSizeOf.int +
          ByteSizeOf.int +
          entry.key.size +
          ValueSerializer.bytesRequired(entry.value)

  }

  implicit object Level0RemoveWriter extends MapEntryWriter[MapEntry.Add[Slice[Byte], Value.Remove]] {
    override val id: Int = 2

    override def write(entry: MapEntry.Add[Slice[Byte], Value.Remove], bytes: Slice[Byte]): Unit =
      bytes
        .addInt(id)
        .addInt(entry.key.size)
        .addAll(entry.key)

    override def bytesRequired(entry: MapEntry.Add[Slice[Byte], Value.Remove]): Int =
      ByteSizeOf.int +
        ByteSizeOf.int +
        entry.key.size
  }

  implicit object Level0AddValueWriter extends MapEntryWriter[MapEntry.Add[Slice[Byte], Value]] {
    override val id: Int = -1

    override def write(entry: MapEntry.Add[Slice[Byte], Value], bytes: Slice[Byte]): Unit =
      entry match {
        case entry @ MapEntry.Add(_, Value.Put(_)) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Add[Slice[Byte], Value.Put]], bytes)
        case entry @ MapEntry.Add(_, Value.Remove) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Add[Slice[Byte], Value.Remove]], bytes)
      }

    override def bytesRequired(entry: MapEntry.Add[Slice[Byte], Value]): Int =
      entry match {
        case entry @ MapEntry.Add(_, Value.Put(_)) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Add[Slice[Byte], Value.Put]])
        case entry @ MapEntry.Add(_, Value.Remove) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Add[Slice[Byte], Value.Remove]])
      }
  }

  implicit object Level0MapEntryWriter extends MapEntryWriter[MapEntry[Slice[Byte], Value]] {
    override val id: Int = -1

    override def write(entry: MapEntry[Slice[Byte], Value], bytes: Slice[Byte]): Unit =
      entry.writeTo(bytes)

    override def bytesRequired(entry: MapEntry[Slice[Byte], Value]): Int =
      entry.entryBytesSize
  }
}
