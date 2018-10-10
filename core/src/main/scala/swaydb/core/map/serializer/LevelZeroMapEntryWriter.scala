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

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Remove], bytes: Slice[Byte]): Unit =
      bytes
        .addInt(id)
        .addInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Remove]): Int =
      ByteSizeOf.int +
        ByteSizeOf.int +
        entry.value.key.size +
        ByteSizeOf.long
  }

  implicit object Level0PutWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Put]] {
    val id: Int = 1

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Put], bytes: Slice[Byte]): Unit =
      bytes
        .addInt(id)
        .addInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addInt(entry.value.value.map(_.size).getOrElse(0))
        .addAll(entry.value.value.getOrElse(Slice.emptyBytes))
        .addLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Put]): Int =
      if (entry.value.key.isEmpty)
        0
      else
        ByteSizeOf.int +
          ByteSizeOf.int +
          entry.value.key.size +
          ByteSizeOf.int +
          entry.value.value.map(_.size).getOrElse(0) +
          ByteSizeOf.long
  }

  implicit object Level0UpdateWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Update]] {
    val id: Int = 2

    override val isRange: Boolean = false
    override val isUpdate: Boolean = true

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Update], bytes: Slice[Byte]): Unit =
      bytes
        .addInt(id)
        .addInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addInt(entry.value.value.map(_.size).getOrElse(0))
        .addAll(entry.value.value.getOrElse(Slice.emptyBytes))
        .addLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Update]): Int =
      if (entry.value.key.isEmpty)
        0
      else
        ByteSizeOf.int +
          ByteSizeOf.int +
          entry.value.key.size +
          ByteSizeOf.int +
          entry.value.value.map(_.size).getOrElse(0) +
          ByteSizeOf.long
  }

  implicit object Level0RangeWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Range]] {
    val id = 4

    override val isRange: Boolean = true
    override val isUpdate: Boolean = false

    import RangeValueSerializers._

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Range], bytes: Slice[Byte]): Unit = {
      val valueBytesRequired = RangeValueSerializer.bytesRequired(entry.value.fromValue, entry.value.rangeValue)
      RangeValueSerializer.write(entry.value.fromValue, entry.value.rangeValue) {
        bytes
          .addInt(id)
          .addInt(entry.value.fromKey.size)
          .addAll(entry.value.fromKey)
          .addInt(entry.value.toKey.size)
          .addAll(entry.value.toKey)
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
          RangeValueSerializer.bytesRequired(entry.value.fromValue, entry.value.rangeValue)
  }

  implicit object Level0PutValueWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.SegmentResponse]] {

    override val isRange: Boolean = true
    override val isUpdate: Boolean = true

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.SegmentResponse], bytes: Slice[Byte]): Unit =
      entry match {
        case entry @ MapEntry.Put(_, _: Memory.Put) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Put]], bytes)

        case entry @ MapEntry.Put(_, _: Memory.Update) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Update]], bytes)

        case entry @ MapEntry.Put(_, _: Memory.Remove) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Remove]], bytes)

        case entry @ MapEntry.Put(_, _: Memory.Range) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Range]], bytes)
      }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.SegmentResponse]): Int =
      entry match {
        case entry @ MapEntry.Put(_, _: Memory.Put) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Put]])

        case entry @ MapEntry.Put(_, _: Memory.Update) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Update]])

        case entry @ MapEntry.Put(_, _: Memory.Remove) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Remove]])

        case entry @ MapEntry.Put(_, _: Memory.Range) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Range]])
      }

  }
}
