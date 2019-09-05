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

import swaydb.core.data.Memory
import swaydb.core.map.MapEntry
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

/**
 * TODO move to using varints and see if that makes a difference in performance.
 */
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
        .addInt(entry.value.time.size)
        .addAll(entry.value.time.time)
        .addLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Remove]): Int =
      ByteSizeOf.int +
        ByteSizeOf.int +
        entry.value.key.size +
        ByteSizeOf.int +
        entry.value.time.time.size +
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
        .addInt(entry.value.time.size)
        .addAll(entry.value.time.time)
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
          entry.value.time.time.size +
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
        .addInt(entry.value.time.size)
        .addAll(entry.value.time.time)
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
          entry.value.time.time.size +
          ByteSizeOf.int +
          entry.value.value.map(_.size).getOrElse(0) +
          ByteSizeOf.long
  }

  implicit object Level0FunctionWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Function]] {
    val id: Int = 3

    override val isRange: Boolean = false
    override val isUpdate: Boolean = true

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Function], bytes: Slice[Byte]): Unit =
      bytes
        .addInt(id)
        .addInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addInt(entry.value.time.size)
        .addAll(entry.value.time.time)
        .addInt(entry.value.function.size)
        .addAll(entry.value.function)

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Function]): Int =
      if (entry.value.key.isEmpty)
        0
      else
        ByteSizeOf.int +
          ByteSizeOf.int +
          entry.value.key.size +
          ByteSizeOf.int +
          entry.value.time.time.size +
          ByteSizeOf.int +
          entry.value.function.size
  }

  implicit object Level0RangeWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Range]] {
    val id = 4

    override val isRange: Boolean = true
    override val isUpdate: Boolean = false

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

  implicit object Level0PendingApplyWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.PendingApply]] {
    val id = 5

    override val isRange: Boolean = true
    override val isUpdate: Boolean = true

    /**
     * No need to write time since it can be computed from applies.
     */
    override def write(entry: MapEntry.Put[Slice[Byte], Memory.PendingApply], bytes: Slice[Byte]): Unit = {
      val appliesBytesRequired = ValueSerializer.bytesRequired(entry.value.applies)
      ValueSerializer.write(entry.value.applies) {
        bytes
          .addInt(id)
          .addInt(entry.value.key.size)
          .addAll(entry.value.key)
          .addInt(appliesBytesRequired)
      }
    }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.PendingApply]): Int =
      if (entry.value.key.isEmpty)
        0
      else
        ByteSizeOf.int +
          ByteSizeOf.int +
          entry.value.key.size +
          ByteSizeOf.int +
          ValueSerializer.bytesRequired(entry.value.applies)
  }

  implicit object Level0MapEntryPutWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.SegmentResponse]] {

    override val isRange: Boolean = true
    override val isUpdate: Boolean = true

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.SegmentResponse], bytes: Slice[Byte]): Unit =
      entry match {
        case entry @ MapEntry.Put(_, _: Memory.Put) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Put]], bytes)

        case entry @ MapEntry.Put(_, _: Memory.Update) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Update]], bytes)

        case entry @ MapEntry.Put(_, _: Memory.Function) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Function]], bytes)

        case entry @ MapEntry.Put(_, _: Memory.Remove) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Remove]], bytes)

        case entry @ MapEntry.Put(_, _: Memory.Range) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Range]], bytes)

        case entry @ MapEntry.Put(_, _: Memory.PendingApply) =>
          MapEntryWriter.write(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.PendingApply]], bytes)
      }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.SegmentResponse]): Int =
      entry match {
        case entry @ MapEntry.Put(_, _: Memory.Put) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Put]])

        case entry @ MapEntry.Put(_, _: Memory.Update) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Update]])

        case entry @ MapEntry.Put(_, _: Memory.Function) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Function]])

        case entry @ MapEntry.Put(_, _: Memory.Remove) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Remove]])

        case entry @ MapEntry.Put(_, _: Memory.Range) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.Range]])

        case entry @ MapEntry.Put(_, _: Memory.PendingApply) =>
          MapEntryWriter.bytesRequired(entry.asInstanceOf[MapEntry.Put[Slice[Byte], Memory.PendingApply]])
      }
  }
}
