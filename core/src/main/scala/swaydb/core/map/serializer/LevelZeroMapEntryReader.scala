/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.map.serializer

import swaydb.core.data.{Memory, Time, Value}
import swaydb.core.map.MapEntry
import swaydb.data.slice.{ReaderBase, Slice}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Deadline

private[core] object LevelZeroMapEntryReader {

  implicit object Level0RemoveReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Remove]] {

    override def read(reader: ReaderBase[Byte]): MapEntry.Put[Slice[Byte], Memory.Remove] = {
      val keyLength = reader.readUnsignedInt()
      val key = reader.read(keyLength).unslice()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).unslice()
      val deadlineLong = reader.readUnsignedLong()
      val deadline = if (deadlineLong == 0) None else Some(Deadline((deadlineLong, TimeUnit.NANOSECONDS)))
      MapEntry.Put(key, Memory.Remove(key, deadline, Time(time)))(LevelZeroMapEntryWriter.Level0RemoveWriter)
    }
  }

  implicit object Level0PutReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Put]] {

    override def read(reader: ReaderBase[Byte]): MapEntry.Put[Slice[Byte], Memory.Put] = {
      val keyLength = reader.readUnsignedInt()
      val key = reader.read(keyLength).unslice()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).unslice()
      val valueLength = reader.readUnsignedInt()
      val value = if (valueLength == 0) Slice.Null else reader.read(valueLength)
      val deadlineLong = reader.readUnsignedLong()

      val deadline = if (deadlineLong == 0) None else Some(Deadline((deadlineLong, TimeUnit.NANOSECONDS)))
      MapEntry.Put(key, Memory.Put(key, value, deadline, Time(time)))(LevelZeroMapEntryWriter.Level0PutWriter)
    }
  }

  implicit object Level0UpdateReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Update]] {

    override def read(reader: ReaderBase[Byte]): MapEntry.Put[Slice[Byte], Memory.Update] = {
      val keyLength = reader.readUnsignedInt()
      val key = reader.read(keyLength).unslice()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).unslice()
      val valueLength = reader.readUnsignedInt()
      val value = if (valueLength == 0) Slice.Null else reader.read(valueLength)
      val deadlineLong = reader.readUnsignedLong()

      val deadline = if (deadlineLong == 0) None else Some(Deadline((deadlineLong, TimeUnit.NANOSECONDS)))
      MapEntry.Put(key, Memory.Update(key, value, deadline, Time(time)))(LevelZeroMapEntryWriter.Level0UpdateWriter)
    }
  }

  implicit object Level0FunctionReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Function]] {

    override def read(reader: ReaderBase[Byte]): MapEntry.Put[Slice[Byte], Memory.Function] = {
      val keyLength = reader.readUnsignedInt()
      val key = reader.read(keyLength).unslice()
      val timeLength = reader.readUnsignedInt()
      val time = reader.read(timeLength).unslice()
      val functionLength = reader.readUnsignedInt()
      val value = reader.read(functionLength)

      MapEntry.Put(key, Memory.Function(key, value, Time(time)))(LevelZeroMapEntryWriter.Level0FunctionWriter)
    }
  }

  implicit object Level0RangeReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.Range]] {

    override def read(reader: ReaderBase[Byte]): MapEntry.Put[Slice[Byte], Memory.Range] = {
      val fromKeyLength = reader.readUnsignedInt()
      val fromKey = reader.read(fromKeyLength).unslice()
      val toKeyLength = reader.readUnsignedInt()
      val toKey = reader.read(toKeyLength).unslice()
      val valueLength = reader.readUnsignedInt()
      val valueBytes = if (valueLength == 0) Slice.emptyBytes else reader.read(valueLength)
      val (fromValue, rangeValue) = RangeValueSerializer.read(valueBytes)

      MapEntry.Put(fromKey, Memory.Range(fromKey, toKey, fromValue, rangeValue))(LevelZeroMapEntryWriter.Level0RangeWriter)
    }
  }

  implicit object Level0PendingApplyReader extends MapEntryReader[MapEntry.Put[Slice[Byte], Memory.PendingApply]] {

    override def read(reader: ReaderBase[Byte]): MapEntry.Put[Slice[Byte], Memory.PendingApply] = {
      val keyLength = reader.readUnsignedInt()
      val key = reader.read(keyLength).unslice()
      val valueLength = reader.readUnsignedInt()
      val valueBytes = reader.read(valueLength)
      val applies = ValueSerializer.read[Slice[Value.Apply]](valueBytes)

      MapEntry.Put(key, Memory.PendingApply(key, applies))(LevelZeroMapEntryWriter.Level0PendingApplyWriter)
    }
  }

  implicit object Level0Reader extends MapEntryReader[MapEntry[Slice[Byte], Memory]] {
    private def merge(nextEntry: MapEntry[Slice[Byte], Memory],
                      previousEntryOrNull: MapEntry[Slice[Byte], Memory]) =
      if (previousEntryOrNull == null)
        nextEntry
      else
        previousEntryOrNull ++ nextEntry

    override def read(reader: ReaderBase[Byte]): MapEntry[Slice[Byte], Memory] =
      reader.foldLeft(null: MapEntry[Slice[Byte], Memory]) {
        case (previousEntry, reader) => {
          val entryId = reader.get()
          if (entryId == LevelZeroMapEntryWriter.Level0PutWriter.id)
            merge(
              nextEntry = Level0PutReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else if (entryId == LevelZeroMapEntryWriter.Level0RemoveWriter.id)
            merge(
              nextEntry = Level0RemoveReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else if (entryId == LevelZeroMapEntryWriter.Level0FunctionWriter.id)
            merge(
              nextEntry = Level0FunctionReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else if (entryId == LevelZeroMapEntryWriter.Level0PendingApplyWriter.id)
            merge(
              nextEntry = Level0PendingApplyReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else if (entryId == LevelZeroMapEntryWriter.Level0UpdateWriter.id)
            merge(
              nextEntry = Level0UpdateReader.read(reader),
              previousEntryOrNull = previousEntry
            )

          else if (entryId == LevelZeroMapEntryWriter.Level0RangeWriter.id)
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
