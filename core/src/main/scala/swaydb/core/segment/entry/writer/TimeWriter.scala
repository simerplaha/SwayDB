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

package swaydb.core.segment.entry.writer

import swaydb.core.data.{Memory, Time}
import swaydb.core.segment.entry.id.{BaseEntryId, MemoryToKeyValueIdBinder}
import swaydb.core.util.Bytes._
import swaydb.data.util.Options._

private[segment] trait TimeWriter {
  private[segment] def write[T <: Memory](current: T,
                                    entryId: BaseEntryId.Key,
                                    builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                  valueWriter: ValueWriter,
                                                                  keyWriter: KeyWriter,
                                                                  deadlineWriter: DeadlineWriter): Unit
}

private[segment] object TimeWriter extends TimeWriter {

  private[segment] def write[T <: Memory](current: T,
                                    entryId: BaseEntryId.Key,
                                    builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                  valueWriter: ValueWriter,
                                                                  keyWriter: KeyWriter,
                                                                  deadlineWriter: DeadlineWriter): Unit =
    if (current.persistentTime.nonEmpty)
      when(builder.enablePrefixCompressionForCurrentWrite && !builder.prefixCompressKeysOnly)(builder.previous.mapS(getTime)) flatMap {
        previousTime =>
          //need to compress at least 4 bytes because the meta data required after compression is minimum 2 bytes.
          writePartiallyCompressed(
            previousTime = previousTime,
            current = current,
            entryId = entryId,
            builder = builder
          )
      } getOrElse {
        //no common prefixes or no previous write without compression
        writeUncompressed(
          current = current,
          entryId = entryId,
          builder = builder
        )
      }
    else
      noTime(
        current = current,
        entryId = entryId,
        builder = builder
      )

  private[segment] def getTime(keyValue: Memory): Time =
    keyValue match {
      case keyValue: Memory.Fixed =>
        keyValue match {
          case keyValue: Memory.Remove =>
            keyValue.time

          case keyValue: Memory.Put =>
            keyValue.time

          case keyValue: Memory.Function =>
            keyValue.time

          case keyValue: Memory.Update =>
            keyValue.time

          case _: Memory.PendingApply =>
            keyValue.time
        }
      case _: Memory.Range =>
        Time.empty
    }

  private def writePartiallyCompressed[T <: Memory](current: T,
                                                    previousTime: Time,
                                                    entryId: BaseEntryId.Key,
                                                    builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                                  valueWriter: ValueWriter,
                                                                                  keyWriter: KeyWriter,
                                                                                  deadlineWriter: DeadlineWriter): Option[Unit] =
    compress(
      previous = previousTime.time,
      next = current.persistentTime.time,
      minimumCommonBytes = 3 //minimum 3 required because commonBytes & uncompressedByteSize requires 2 bytes.
    ) map {
      case (commonBytes, remainingBytes) =>

        builder.setSegmentHasPrefixCompression()

        valueWriter.write(
          current = current,
          entryId = entryId.timePartiallyCompressed,
          builder = builder
        )

        builder
          .bytes
          .addUnsignedInt(commonBytes)
          .addUnsignedInt(remainingBytes.size)
          .addAll(remainingBytes)
    }

  private def writeUncompressed[T <: Memory](current: T,
                                             entryId: BaseEntryId.Key,
                                             builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                           valueWriter: ValueWriter,
                                                                           keyWriter: KeyWriter,
                                                                           deadlineWriter: DeadlineWriter): Unit = {
    //no common prefixes or no previous write without compression
    valueWriter.write(
      current = current,
      entryId = entryId.timeUncompressed,
      builder = builder
    )

    builder
      .bytes
      .addUnsignedInt(current.persistentTime.size)
      .addAll(current.persistentTime.time)
  }

  private def noTime[T <: Memory](current: T,
                                  entryId: BaseEntryId.Key,
                                  builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                valueWriter: ValueWriter,
                                                                keyWriter: KeyWriter,
                                                                deadlineWriter: DeadlineWriter): Unit =
    valueWriter.write(
      current = current,
      entryId = entryId.noTime,
      builder = builder
    )
}
