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
import swaydb.slice.{ReaderBase, Slice}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Deadline

private[core] object LevelZeroLogEntryReader {

  implicit object Level0RemoveReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.Remove]] {

    override def read(reader: ReaderBase[Byte]): LogEntry.Put[Slice[Byte], Memory.Remove] = {
      val keyLength = reader.readUnsignedInt()
      val key: Slice[Byte] = reader.read(keyLength).cut()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).cut()
      val deadlineLong = reader.readUnsignedLong()
      val deadline = if (deadlineLong == 0) None else Some(Deadline((deadlineLong, TimeUnit.NANOSECONDS)))
      LogEntry.Put(key, Memory.Remove(key, deadline, Time(time)))(LevelZeroLogEntryWriter.Level0RemoveWriter)
    }
  }

  implicit object Level0PutReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.Put]] {

    override def read(reader: ReaderBase[Byte]): LogEntry.Put[Slice[Byte], Memory.Put] = {
      val keyLength = reader.readUnsignedInt()
      val key: Slice[Byte] = reader.read(keyLength).cut()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).cut()
      val valueLength = reader.readUnsignedInt()
      val value = if (valueLength == 0) Slice.Null else reader.read(valueLength) // TODO - cut not needed?
      val deadlineLong = reader.readUnsignedLong()

      val deadline = if (deadlineLong == 0) None else Some(Deadline((deadlineLong, TimeUnit.NANOSECONDS)))
      LogEntry.Put(key, Memory.Put(key, value, deadline, Time(time)))(LevelZeroLogEntryWriter.Level0PutWriter)
    }
  }

  implicit object Level0UpdateReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.Update]] {

    override def read(reader: ReaderBase[Byte]): LogEntry.Put[Slice[Byte], Memory.Update] = {
      val keyLength = reader.readUnsignedInt()
      val key: Slice[Byte] = reader.read(keyLength).cut()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).cut()
      val valueLength = reader.readUnsignedInt()
      val value = if (valueLength == 0) Slice.Null else reader.read(valueLength)
      val deadlineLong = reader.readUnsignedLong()

      val deadline = if (deadlineLong == 0) None else Some(Deadline((deadlineLong, TimeUnit.NANOSECONDS)))
      LogEntry.Put(key, Memory.Update(key, value, deadline, Time(time)))(LevelZeroLogEntryWriter.Level0UpdateWriter)
    }
  }

  implicit object Level0FunctionReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.Function]] {

    override def read(reader: ReaderBase[Byte]): LogEntry.Put[Slice[Byte], Memory.Function] = {
      val keyLength = reader.readUnsignedInt()
      val key: Slice[Byte] = reader.read(keyLength).cut()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).cut()
      val functionLength = reader.readUnsignedInt()
      val value = reader.read(functionLength)

      LogEntry.Put(key, Memory.Function(key, value, Time(time)))(LevelZeroLogEntryWriter.Level0FunctionWriter)
    }
  }

  implicit object Level0RangeReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.Range]] {

    override def read(reader: ReaderBase[Byte]): LogEntry.Put[Slice[Byte], Memory.Range] = {
      val fromKeyLength = reader.readUnsignedInt()
      val fromKey: Slice[Byte] = reader.read(fromKeyLength).cut()
      val toKeyLength = reader.readUnsignedInt()
      val toKey = reader.read(toKeyLength).cut()
      val valueLength = reader.readUnsignedInt()
      val valueBytes = if (valueLength == 0) Slice.emptyBytes else reader.read(valueLength)
      val (fromValue, rangeValue) = RangeValueSerialiser.read(valueBytes)

      LogEntry.Put(fromKey, Memory.Range(fromKey, toKey, fromValue, rangeValue))(LevelZeroLogEntryWriter.Level0RangeWriter)
    }
  }

  implicit object Level0PendingApplyReader extends LogEntryReader[LogEntry.Put[Slice[Byte], Memory.PendingApply]] {

    override def read(reader: ReaderBase[Byte]): LogEntry.Put[Slice[Byte], Memory.PendingApply] = {
      val keyLength = reader.readUnsignedInt()
      val key: Slice[Byte] = reader.read(keyLength).cut()
      val valueLength = reader.readUnsignedInt()
      val valueBytes = reader.read(valueLength)
      val applies = ValueSerialiser.read[Slice[Value.Apply]](valueBytes)

      LogEntry.Put(key, Memory.PendingApply(key, applies))(LevelZeroLogEntryWriter.Level0PendingApplyWriter)
    }
  }

  implicit object Level0Reader extends LogEntryReader[LogEntry[Slice[Byte], Memory]] {
    private def merge(nextEntry: LogEntry[Slice[Byte], Memory],
                      previousEntryOrNull: LogEntry[Slice[Byte], Memory]) =
      if (previousEntryOrNull == null)
        nextEntry
      else
        previousEntryOrNull ++ nextEntry

    override def read(reader: ReaderBase[Byte]): LogEntry[Slice[Byte], Memory] =
      reader.foldLeft(null: LogEntry[Slice[Byte], Memory]) {
        case (previousEntry, reader) => {
          val entryId = reader.get()
          if (entryId == LevelZeroLogEntryWriter.Level0PutWriter.id)
            merge(
              nextEntry = Level0PutReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else if (entryId == LevelZeroLogEntryWriter.Level0RemoveWriter.id)
            merge(
              nextEntry = Level0RemoveReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else if (entryId == LevelZeroLogEntryWriter.Level0FunctionWriter.id)
            merge(
              nextEntry = Level0FunctionReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else if (entryId == LevelZeroLogEntryWriter.Level0PendingApplyWriter.id)
            merge(
              nextEntry = Level0PendingApplyReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else if (entryId == LevelZeroLogEntryWriter.Level0UpdateWriter.id)
            merge(
              nextEntry = Level0UpdateReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else if (entryId == LevelZeroLogEntryWriter.Level0RangeWriter.id)
            merge(
              nextEntry = Level0RangeReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else
            throw new IllegalArgumentException(s"Invalid entry type $entryId.")
        }
      }
  }
}
