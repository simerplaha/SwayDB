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
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

object LevelZeroMapEntryWriter {

  implicit object Level0RemoveWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Value.Remove]] {
    val id: Int = 0

    override def write(entry: MapEntry.Put[Slice[Byte], Value.Remove], bytes: Slice[Byte]): Unit =
      bytes
        .addInt(id)
        .addInt(entry.key.size)
        .addAll(entry.key)

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Value.Remove]): Int =
      ByteSizeOf.int +
        ByteSizeOf.int +
        entry.key.size

    override val isRange: Boolean = false
  }

  implicit object Level0PutWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Value.Put]] {
    val id: Int = 1

    implicit val putSerializer = ValueSerializers.PutSerializerWithSize

    override def write(entry: MapEntry.Put[Slice[Byte], Value.Put], bytes: Slice[Byte]): Unit =
      ValueSerializer.write(entry.value) {
        bytes
          .addInt(id)
          .addInt(entry.key.size)
          .addAll(entry.key)
      }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Value.Put]): Int =
      if (entry.key.isEmpty)
        0
      else
        ByteSizeOf.int +
          ByteSizeOf.int +
          entry.key.size +
          ValueSerializer.bytesRequired(entry.value)

    override val isRange: Boolean = false
  }

  implicit object Level0PutRangeWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Value.Range]] {
    val id = 2

    implicit val putSerializer = ValueSerializers.PutSerializerWithSize
    implicit val rangeSerializer = RangeValueSerializers.OptionRangeValueSerializer

    override def write(entry: MapEntry.Put[Slice[Byte], Value.Range], bytes: Slice[Byte]): Unit = {
      val (valueBytesRequired, rangeId) = RangeValueSerializer.bytesRequiredAndRangeId(entry.value.fromValue, entry.value.rangeValue)
      RangeValueSerializer.write(entry.value.fromValue, entry.value.rangeValue) {
        bytes
          .addInt(id)
          .addInt(entry.key.size)
          .addAll(entry.key)
          .addInt(entry.value.toKey.size)
          .addAll(entry.value.toKey)
          .addInt(rangeId)
          .addInt(valueBytesRequired)
      }
    }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Value.Range]): Int =
      if (entry.key.isEmpty)
        0
      else
        ByteSizeOf.int +
          ByteSizeOf.int +
          entry.key.size +
          ByteSizeOf.int +
          entry.value.toKey.size +
          ByteSizeOf.int +
          ByteSizeOf.int +
          RangeValueSerializer.bytesRequiredAndRangeId(entry.value.fromValue, entry.value.rangeValue)._1

    override val isRange: Boolean = true
  }

  implicit object Level0PutValueWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Value]] {

    override def write(entry: MapEntry.Put[Slice[Byte], Value], bytes: Slice[Byte]): Unit =
      entry match {
        case entry @ MapEntry.Put(_, Value.Put(_)) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Value.Put]], bytes)
        case entry @ MapEntry.Put(_, Value.Remove) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Value.Remove]], bytes)
        case entry @ MapEntry.Put(_, Value.Range(_, _, _)) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Value.Range]], bytes)
      }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Value]): Int =
      entry match {
        case entry @ MapEntry.Put(_, Value.Put(_)) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Value.Put]])
        case entry @ MapEntry.Put(_, Value.Remove) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Value.Remove]])
        case entry @ MapEntry.Put(_, Value.Range(_, _, _)) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Value.Range]])
      }

    override val isRange: Boolean = false
  }
}
