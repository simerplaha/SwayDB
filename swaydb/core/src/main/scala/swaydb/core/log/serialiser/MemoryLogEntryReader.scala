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
import swaydb.core.segment.data.{Memory, Time, Value}
import swaydb.core.segment.serialiser.{RangeValueSerialiser, ValueSerialiser}
import swaydb.slice.{Slice, SliceReader}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Deadline

private[core] object MemoryLogEntryReader {

  implicit object RemoveLogEntryPutReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.Remove]] {

    override def read(reader: SliceReader): LogEntry.Put[Slice[Byte], Memory.Remove] = {
      val keyLength = reader.readUnsignedInt()
      val key: Slice[Byte] = reader.read(keyLength).cut()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).cut()
      val deadlineLong = reader.readUnsignedLong()
      val deadline = if (deadlineLong == 0) None else Some(Deadline((deadlineLong, TimeUnit.NANOSECONDS)))
      LogEntry.Put(key, Memory.Remove(key, deadline, Time(time)))(MemoryLogEntryWriter.RemoveLogEntryPutWriter)
    }
  }

  implicit object PutLogEntryPutReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.Put]] {

    override def read(reader: SliceReader): LogEntry.Put[Slice[Byte], Memory.Put] = {
      val keyLength = reader.readUnsignedInt()
      val key: Slice[Byte] = reader.read(keyLength).cut()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).cut()
      val valueLength = reader.readUnsignedInt()
      val value = if (valueLength == 0) Slice.Null else reader.read(valueLength) // TODO - cut not needed?
      val deadlineLong = reader.readUnsignedLong()

      val deadline = if (deadlineLong == 0) None else Some(Deadline((deadlineLong, TimeUnit.NANOSECONDS)))
      LogEntry.Put(key, Memory.Put(key, value, deadline, Time(time)))(MemoryLogEntryWriter.PutLogEntryPutWriter)
    }
  }

  implicit object UpdateLogEntryPutReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.Update]] {

    override def read(reader: SliceReader): LogEntry.Put[Slice[Byte], Memory.Update] = {
      val keyLength = reader.readUnsignedInt()
      val key: Slice[Byte] = reader.read(keyLength).cut()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).cut()
      val valueLength = reader.readUnsignedInt()
      val value = if (valueLength == 0) Slice.Null else reader.read(valueLength)
      val deadlineLong = reader.readUnsignedLong()

      val deadline = if (deadlineLong == 0) None else Some(Deadline((deadlineLong, TimeUnit.NANOSECONDS)))
      LogEntry.Put(key, Memory.Update(key, value, deadline, Time(time)))(MemoryLogEntryWriter.UpdateLogEntryPutWriter)
    }
  }

  implicit object FunctionLogEntryPutReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.Function]] {

    override def read(reader: SliceReader): LogEntry.Put[Slice[Byte], Memory.Function] = {
      val keyLength = reader.readUnsignedInt()
      val key: Slice[Byte] = reader.read(keyLength).cut()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).cut()
      val functionLength = reader.readUnsignedInt()
      val value = reader.read(functionLength)

      LogEntry.Put(key, Memory.Function(key, value, Time(time)))(MemoryLogEntryWriter.FunctionLogEntryPutWriter)
    }
  }

  implicit object RangeLogEntryPutReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.Range]] {

    override def read(reader: SliceReader): LogEntry.Put[Slice[Byte], Memory.Range] = {
      val fromKeyLength = reader.readUnsignedInt()
      val fromKey: Slice[Byte] = reader.read(fromKeyLength).cut()
      val toKeyLength = reader.readUnsignedInt()
      val toKey = reader.read(toKeyLength).cut()
      val valueLength = reader.readUnsignedInt()
      val valueBytes = if (valueLength == 0) Slice.emptyBytes else reader.read(valueLength)
      val (fromValue, rangeValue) = RangeValueSerialiser.read(valueBytes)

      LogEntry.Put(fromKey, Memory.Range(fromKey, toKey, fromValue, rangeValue))(MemoryLogEntryWriter.RangeLogEntryPutWriter)
    }
  }

  implicit object PendingApplyLogEntryPutReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.PendingApply]] {

    override def read(reader: SliceReader): LogEntry.Put[Slice[Byte], Memory.PendingApply] = {
      val keyLength = reader.readUnsignedInt()
      val key: Slice[Byte] = reader.read(keyLength).cut()
      val valueLength = reader.readUnsignedInt()
      val valueBytes = reader.read(valueLength)
      val applies = ValueSerialiser.read[Slice[Value.Apply]](valueBytes)

      LogEntry.Put(key, Memory.PendingApply(key, applies))(MemoryLogEntryWriter.PendingApplyLogEntryPutWriter)
    }
  }

  implicit object KeyValueLogEntryPutReader extends LogEntryReader[LogEntry[Slice[Byte], Memory]] {
    private def merge(nextEntry: LogEntry[Slice[Byte], Memory],
                      previousEntryOrNull: LogEntry[Slice[Byte], Memory]) =
      if (previousEntryOrNull == null)
        nextEntry
      else
        previousEntryOrNull ++ nextEntry

    override def read(reader: SliceReader): LogEntry[Slice[Byte], Memory] =
      reader.foldLeft(null: LogEntry[Slice[Byte], Memory]) {
        case (previousEntry, reader) =>
          val entryId = reader.get()

          if (entryId == MemoryLogEntryWriter.PutLogEntryPutWriter.id)
            merge(
              nextEntry = PutLogEntryPutReader.read(reader),
              previousEntryOrNull = previousEntry
            )
          else if (entryId == MemoryLogEntryWriter.RemoveLogEntryPutWriter.id)
            merge(
              nextEntry = RemoveLogEntryPutReader.read(reader),
              previousEntryOrNull = previousEntry
            )
          else if (entryId == MemoryLogEntryWriter.FunctionLogEntryPutWriter.id)
            merge(
              nextEntry = FunctionLogEntryPutReader.read(reader),
              previousEntryOrNull = previousEntry
            )
          else if (entryId == MemoryLogEntryWriter.PendingApplyLogEntryPutWriter.id)
            merge(
              nextEntry = PendingApplyLogEntryPutReader.read(reader),
              previousEntryOrNull = previousEntry
            )
          else if (entryId == MemoryLogEntryWriter.UpdateLogEntryPutWriter.id)
            merge(
              nextEntry = UpdateLogEntryPutReader.read(reader),
              previousEntryOrNull = previousEntry
            )
          else if (entryId == MemoryLogEntryWriter.RangeLogEntryPutWriter.id)
            merge(
              nextEntry = RangeLogEntryPutReader.read(reader),
              previousEntryOrNull = previousEntry
            )
          else
            throw new IllegalArgumentException(s"Invalid entry type $entryId.")
      }
  }
}
