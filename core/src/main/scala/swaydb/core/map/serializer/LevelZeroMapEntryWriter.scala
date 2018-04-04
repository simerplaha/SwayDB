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

import swaydb.core.data.Memory
import swaydb.core.map.MapEntry
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

object LevelZeroMapEntryWriter {

  implicit object Level0RemoveWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Remove]] {
    val id: Int = 0

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Remove], bytes: Slice[Byte]): Unit =
      bytes
        .addInt(id)
        .addInt(entry.value.key.size)
        .addAll(entry.value.key)

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Remove]): Int =
      ByteSizeOf.int +
        ByteSizeOf.int +
        entry.value.key.size

    override val isRange: Boolean = false
  }

  implicit object Level0PutWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Put]] {
    val id: Int = 1

    implicit val putSerializer = ValueSerializers.PutSerializerWithSize

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Put], bytes: Slice[Byte]): Unit =
      bytes
        .addInt(id)
        .addInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addInt(entry.value.value.map(_.size).getOrElse(0))
        .addAll(entry.value.value.getOrElse(Slice.emptyByteSlice))

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Put]): Int =
      if (entry.value.key.isEmpty)
        0
      else
        ByteSizeOf.int +
          ByteSizeOf.int +
          entry.value.key.size +
          ByteSizeOf.int +
          entry.value.value.map(_.size).getOrElse(0)

    override val isRange: Boolean = false
  }

  implicit object Level0PutRangeWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Range]] {
    val id = 2

    implicit val putSerializer = ValueSerializers.PutSerializerWithSize
    implicit val rangeSerializer = RangeValueSerializers.OptionRangeValueSerializer

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Range], bytes: Slice[Byte]): Unit = {
      val (valueBytesRequired, rangeId) = RangeValueSerializer.bytesRequiredAndRangeId(entry.value.fromValue, entry.value.rangeValue)
      RangeValueSerializer.write(entry.value.fromValue, entry.value.rangeValue) {
        bytes
          .addInt(id)
          .addInt(entry.value.fromKey.size)
          .addAll(entry.value.fromKey)
          .addInt(entry.value.toKey.size)
          .addAll(entry.value.toKey)
          .addInt(rangeId)
          .addInt(valueBytesRequired)
      }
    }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Range]): Int =
      if (entry.value.key.isEmpty)
        0
      else
        ByteSizeOf.int +
          ByteSizeOf.int +
          entry.value.key.size +
          ByteSizeOf.int +
          entry.value.toKey.size +
          ByteSizeOf.int +
          ByteSizeOf.int +
          RangeValueSerializer.bytesRequiredAndRangeId(entry.value.fromValue, entry.value.rangeValue)._1

    override val isRange: Boolean = true
  }

  implicit object Level0PutValueWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory]] {

    override def write(entry: MapEntry.Put[Slice[Byte], Memory], bytes: Slice[Byte]): Unit =
      entry match {
        case entry @ MapEntry.Put(_, _: Memory.Put) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Put]], bytes)

        case entry @ MapEntry.Put(_, _: Memory.Remove) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Remove]], bytes)

        case entry @ MapEntry.Put(_, _: Memory.Range) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Range]], bytes)
      }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory]): Int =
      entry match {
        case entry @ MapEntry.Put(_, _: Memory.Put) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Put]])

        case entry @ MapEntry.Put(_, _: Memory.Remove) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Remove]])

        case entry @ MapEntry.Put(_, _: Memory.Range) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Range]])
      }

    override val isRange: Boolean = false
  }
}
