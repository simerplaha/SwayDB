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

package swaydb.core.log.serialiser

import swaydb.core.log.LogEntry
import swaydb.core.segment.data.Memory
import swaydb.core.segment.serialiser.{RangeValueSerialiser, ValueSerialiser}
import swaydb.core.util.Bytes
import swaydb.slice.{Slice, SliceMut}
import swaydb.utils.ByteSizeOf

/**
 * TODO move to using varints and see if that makes a difference in performance.
 */
private[core] object LevelZeroLogEntryWriter {

  implicit object Level0RemoveWriter extends LogEntryWriter[LogEntry.Put[Slice[Byte], Memory.Remove]] {
    val id: Byte = 0

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: LogEntry.Put[Slice[Byte], Memory.Remove], bytes: SliceMut[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addUnsignedInt(entry.value.time.size)
        .addAll(entry.value.time.time)
        .addUnsignedLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))

    override def bytesRequired(entry: LogEntry.Put[Slice[Byte], Memory.Remove]): Int =
      ByteSizeOf.byte +
        Bytes.sizeOfUnsignedInt(entry.value.key.size) +
        entry.value.key.size +
        Bytes.sizeOfUnsignedInt(entry.value.time.time.size) +
        entry.value.time.time.size +
        Bytes.sizeOfUnsignedLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))
  }

  implicit object Level0PutWriter extends LogEntryWriter[LogEntry.Put[Slice[Byte], Memory.Put]] {
    val id: Byte = 1

    override val isRange: Boolean = false
    override val isUpdate: Boolean = false

    override def write(entry: LogEntry.Put[Slice[Byte], Memory.Put], bytes: SliceMut[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addUnsignedInt(entry.value.time.size)
        .addAll(entry.value.time.time)
        .addUnsignedInt(entry.value.value.valueOrElseC(_.size, 0))
        .addAll(entry.value.value.getOrElseC(Slice.emptyBytes))
        .addUnsignedLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))

    override def bytesRequired(entry: LogEntry.Put[Slice[Byte], Memory.Put]): Int =
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

  implicit object Level0UpdateWriter extends LogEntryWriter[LogEntry.Put[Slice[Byte], Memory.Update]] {
    val id: Byte = 2

    override val isRange: Boolean = false
    override val isUpdate: Boolean = true

    override def write(entry: LogEntry.Put[Slice[Byte], Memory.Update], bytes: SliceMut[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addUnsignedInt(entry.value.time.size)
        .addAll(entry.value.time.time)
        .addUnsignedInt(entry.value.value.valueOrElseC(_.size, 0))
        .addAll(entry.value.value.getOrElseC(Slice.emptyBytes))
        .addUnsignedLong(entry.value.deadline.map(_.time.toNanos).getOrElse(0))

    override def bytesRequired(entry: LogEntry.Put[Slice[Byte], Memory.Update]): Int =
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

  implicit object Level0FunctionWriter extends LogEntryWriter[LogEntry.Put[Slice[Byte], Memory.Function]] {
    val id: Byte = 3

    override val isRange: Boolean = false
    override val isUpdate: Boolean = true

    override def write(entry: LogEntry.Put[Slice[Byte], Memory.Function], bytes: SliceMut[Byte]): Unit =
      bytes
        .add(id)
        .addUnsignedInt(entry.value.key.size)
        .addAll(entry.value.key)
        .addUnsignedInt(entry.value.time.size)
        .addAll(entry.value.time.time)
        .addUnsignedInt(entry.value.function.size)
        .addAll(entry.value.function)

    override def bytesRequired(entry: LogEntry.Put[Slice[Byte], Memory.Function]): Int =
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

  implicit object Level0RangeWriter extends LogEntryWriter[LogEntry.Put[Slice[Byte], Memory.Range]] {
    val id: Byte = 4

    override val isRange: Boolean = true
    override val isUpdate: Boolean = false

    override def write(entry: LogEntry.Put[Slice[Byte], Memory.Range], bytes: SliceMut[Byte]): Unit = {
      val valueBytesRequired = RangeValueSerialiser.bytesRequired(entry.value.fromValue, entry.value.rangeValue)
      RangeValueSerialiser.write(entry.value.fromValue, entry.value.rangeValue) {
        bytes
          .add(id)
          .addUnsignedInt(entry.value.fromKey.size)
          .addAll(entry.value.fromKey)
          .addUnsignedInt(entry.value.toKey.size)
          .addAll(entry.value.toKey)
          .addUnsignedInt(valueBytesRequired)
      }
    }

    override def bytesRequired(entry: LogEntry.Put[Slice[Byte], Memory.Range]): Int =
      if (entry.value.key.isEmpty) {
        0
      } else {
        val bytesRequired = RangeValueSerialiser.bytesRequired(entry.value.fromValue, entry.value.rangeValue)
        ByteSizeOf.byte +
          Bytes.sizeOfUnsignedInt(entry.value.fromKey.size) +
          entry.value.key.size +
          Bytes.sizeOfUnsignedInt(entry.value.toKey.size) +
          entry.value.toKey.size +
          Bytes.sizeOfUnsignedInt(bytesRequired) +
          bytesRequired
      }
  }

  implicit object Level0PendingApplyWriter extends LogEntryWriter[LogEntry.Put[Slice[Byte], Memory.PendingApply]] {
    val id: Byte = 5

    override val isRange: Boolean = true
    override val isUpdate: Boolean = true

    /**
     * No need to write time since it can be computed from applies.
     */
    override def write(entry: LogEntry.Put[Slice[Byte], Memory.PendingApply], bytes: SliceMut[Byte]): Unit = {
      val appliesBytesRequired = ValueSerialiser.bytesRequired(entry.value.applies)
      ValueSerialiser.write(entry.value.applies) {
        bytes
          .add(id)
          .addUnsignedInt(entry.value.key.size)
          .addAll(entry.value.key)
          .addUnsignedInt(appliesBytesRequired)
      }
    }

    override def bytesRequired(entry: LogEntry.Put[Slice[Byte], Memory.PendingApply]): Int =
      if (entry.value.key.isEmpty) {
        0
      } else {
        val bytesRequired = ValueSerialiser.bytesRequired(entry.value.applies)
        ByteSizeOf.byte +
          Bytes.sizeOfUnsignedInt(entry.value.key.size) +
          entry.value.key.size +
          Bytes.sizeOfUnsignedInt(bytesRequired) +
          bytesRequired
      }
  }

  implicit object Level0LogEntryPutWriter extends LogEntryWriter[LogEntry.Put[Slice[Byte], Memory]] {

    override val isRange: Boolean = true
    override val isUpdate: Boolean = true

    override def write(entry: LogEntry.Put[Slice[Byte], Memory], bytes: SliceMut[Byte]): Unit =
      entry match {
        case entry @ LogEntry.Put(_, _: Memory.Put) =>
          LogEntryWriter.write(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.Put]], bytes)

        case entry @ LogEntry.Put(_, _: Memory.Update) =>
          LogEntryWriter.write(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.Update]], bytes)

        case entry @ LogEntry.Put(_, _: Memory.Function) =>
          LogEntryWriter.write(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.Function]], bytes)

        case entry @ LogEntry.Put(_, _: Memory.Remove) =>
          LogEntryWriter.write(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.Remove]], bytes)

        case entry @ LogEntry.Put(_, _: Memory.Range) =>
          LogEntryWriter.write(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.Range]], bytes)

        case entry @ LogEntry.Put(_, _: Memory.PendingApply) =>
          LogEntryWriter.write(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.PendingApply]], bytes)
      }

    override def bytesRequired(entry: LogEntry.Put[Slice[Byte], Memory]): Int =
      entry match {
        case entry @ LogEntry.Put(_, _: Memory.Put) =>
          LogEntryWriter.bytesRequired(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.Put]])

        case entry @ LogEntry.Put(_, _: Memory.Update) =>
          LogEntryWriter.bytesRequired(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.Update]])

        case entry @ LogEntry.Put(_, _: Memory.Function) =>
          LogEntryWriter.bytesRequired(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.Function]])

        case entry @ LogEntry.Put(_, _: Memory.Remove) =>
          LogEntryWriter.bytesRequired(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.Remove]])

        case entry @ LogEntry.Put(_, _: Memory.Range) =>
          LogEntryWriter.bytesRequired(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.Range]])

        case entry @ LogEntry.Put(_, _: Memory.PendingApply) =>
          LogEntryWriter.bytesRequired(entry.asInstanceOf[LogEntry.Put[Slice[Byte], Memory.PendingApply]])
      }
  }
}
