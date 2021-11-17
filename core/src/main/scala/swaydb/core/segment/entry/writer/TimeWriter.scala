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

package swaydb.core.segment.entry.writer

import swaydb.core.segment.data.{Memory, Time}
import swaydb.core.segment.entry.id.{BaseEntryId, MemoryToKeyValueIdBinder}
import swaydb.core.util.Bytes._
import swaydb.utils.Options.when

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
