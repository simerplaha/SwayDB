/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.map.serializer

import swaydb.core.data.Memory
import swaydb.core.map.MapEntry
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.utils.ByteSizeOf

/**
 * TODO move to using varints and see if that makes a difference in performance.
 */
private[core] object LevelZeroMapEntryWriter {

  implicit object Level0RemoveWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Remove]] {
    val id: Byte = 0

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Remove], bytes: Slice[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addUnsignedInt(entry.value.time.size)
        .addAll(entry.value.time.time)
        .addUnsignedLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Remove]): Int =
      ByteSizeOf.byte +
        Bytes.sizeOfUnsignedInt(entry.value.key.size) +
        entry.value.key.size +
        Bytes.sizeOfUnsignedInt(entry.value.time.time.size) +
        entry.value.time.time.size +
        Bytes.sizeOfUnsignedLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))
  }

  implicit object Level0PutWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Put]] {
    val id: Byte = 1

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Put], bytes: Slice[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addUnsignedInt(entry.value.time.size)
        .addAll(entry.value.time.time)
        .addUnsignedInt(entry.value.value.valueOrElseC(_.size, 0))
        .addAll(entry.value.value.getOrElseC(Slice.emptyBytes))
        .addUnsignedLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Put]): Int =
      if (entry.value.key.isEmpty)
        0
      else
        ByteSizeOf.byte +
          Bytes.sizeOfUnsignedInt(entry.value.key.size) +
          entry.value.key.size +
          Bytes.sizeOfUnsignedInt(entry.value.time.size) +
          entry.value.time.time.size +
          Bytes.sizeOfUnsignedInt(entry.value.value.valueOrElseC(_.size, 0)) +
          entry.value.value.valueOrElseC(_.size, 0) +
          Bytes.sizeOfUnsignedLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))
  }

  implicit object Level0UpdateWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Update]] {
    val id: Byte = 2

    override val isRange: Boolean = false
    override val isUpdate: Boolean = true

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Update], bytes: Slice[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addUnsignedInt(entry.value.time.size)
        .addAll(entry.value.time.time)
        .addUnsignedInt(entry.value.value.valueOrElseC(_.size, 0))
        .addAll(entry.value.value.getOrElseC(Slice.emptyBytes))
        .addUnsignedLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Update]): Int =
      if (entry.value.key.isEmpty)
        0
      else
        ByteSizeOf.byte +
          Bytes.sizeOfUnsignedInt(entry.value.key.size) +
          entry.value.key.size +
          Bytes.sizeOfUnsignedInt(entry.value.time.size) +
          entry.value.time.time.size +
          Bytes.sizeOfUnsignedInt(entry.value.value.valueOrElseC(_.size, 0)) +
          entry.value.value.valueOrElseC(_.size, 0) +
          Bytes.sizeOfUnsignedLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))
  }

  implicit object Level0FunctionWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Function]] {
    val id: Byte = 3

    override val isRange: Boolean = false
    override val isUpdate: Boolean = true

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Function], bytes: Slice[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addUnsignedInt(entry.value.time.size)
        .addAll(entry.value.time.time)
        .addUnsignedInt(entry.value.function.size)
        .addAll(entry.value.function)

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Function]): Int =
      if (entry.value.key.isEmpty)
        0
      else
        ByteSizeOf.byte +
          Bytes.sizeOfUnsignedInt(entry.value.key.size) +
          entry.value.key.size +
          Bytes.sizeOfUnsignedInt(entry.value.time.size) +
          entry.value.time.time.size +
          Bytes.sizeOfUnsignedInt(entry.value.function.size) +
          entry.value.function.size
  }

  implicit object Level0RangeWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.Range]] {
    val id: Byte = 4

    override val isRange: Boolean = true
    override val isUpdate: Boolean = false

    override def write(entry: MapEntry.Put[Slice[Byte], Memory.Range], bytes: Slice[Byte]): Unit = {
      val valueBytesRequired = RangeValueSerializer.bytesRequired(entry.value.fromValue, entry.value.rangeValue)
      RangeValueSerializer.write(entry.value.fromValue, entry.value.rangeValue) {
        bytes
          .add(id)
          .addUnsignedInt(entry.value.fromKey.size)
          .addAll(entry.value.fromKey)
          .addUnsignedInt(entry.value.toKey.size)
          .addAll(entry.value.toKey)
          .addUnsignedInt(valueBytesRequired)
      }
    }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.Range]): Int =
      if (entry.value.key.isEmpty) {
        0
      } else {
        val bytesRequired = RangeValueSerializer.bytesRequired(entry.value.fromValue, entry.value.rangeValue)
        ByteSizeOf.byte +
          Bytes.sizeOfUnsignedInt(entry.value.fromKey.size) +
          entry.value.key.size +
          Bytes.sizeOfUnsignedInt(entry.value.toKey.size) +
          entry.value.toKey.size +
          Bytes.sizeOfUnsignedInt(bytesRequired) +
          bytesRequired
      }
  }

  implicit object Level0PendingApplyWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory.PendingApply]] {
    val id: Byte = 5

    override val isRange: Boolean = true
    override val isUpdate: Boolean = true

    /**
     * No need to write time since it can be computed from applies.
     */
    override def write(entry: MapEntry.Put[Slice[Byte], Memory.PendingApply], bytes: Slice[Byte]): Unit = {
      val appliesBytesRequired = ValueSerializer.bytesRequired(entry.value.applies)
      ValueSerializer.write(entry.value.applies) {
        bytes
          .add(id)
          .addUnsignedInt(entry.value.key.size)
          .addAll(entry.value.key)
          .addUnsignedInt(appliesBytesRequired)
      }
    }

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory.PendingApply]): Int =
      if (entry.value.key.isEmpty) {
        0
      } else {
        val bytesRequired = ValueSerializer.bytesRequired(entry.value.applies)
        ByteSizeOf.byte +
          Bytes.sizeOfUnsignedInt(entry.value.key.size) +
          entry.value.key.size +
          Bytes.sizeOfUnsignedInt(bytesRequired) +
          bytesRequired
      }
  }

  implicit object Level0MapEntryPutWriter extends MapEntryWriter[MapEntry.Put[Slice[Byte], Memory]] {

    override val isRange: Boolean = true
    override val isUpdate: Boolean = true

    override def write(entry: MapEntry.Put[Slice[Byte], Memory], bytes: Slice[Byte]): Unit =
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

    override def bytesRequired(entry: MapEntry.Put[Slice[Byte], Memory]): Int =
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
