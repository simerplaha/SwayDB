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

package swaydb.core.segment.format.a.entry.writer

import swaydb.core.data.KeyValue.WriteOnly
import swaydb.core.data.{KeyValue, Time, Transient}
import swaydb.core.segment.format.a.entry.id.{TransientToEntryId, EntryId}
import swaydb.core.util.Bytes._
import swaydb.data.slice.Slice

object TimeWriter {

  private def getTime(keyValue: KeyValue.WriteOnly): Time =
    keyValue match {
      case keyValue: WriteOnly.Fixed =>
        keyValue match {
          case keyValue: Transient.Remove =>
            keyValue.time
          case keyValue: Transient.Put =>
            keyValue.time
          case keyValue: Transient.Function =>
            keyValue.time
          case keyValue: Transient.Update =>
            keyValue.time
          case _: Transient.PendingApply =>
            Time.empty
        }
      case _: WriteOnly.Range =>
        Time.empty
      case _: WriteOnly.Group =>
        Time.empty
    }

  def write(current: KeyValue.WriteOnly,
            currentTime: Time,
            compressDuplicateValues: Boolean,
            entryId: EntryId.Key,
            plusSize: Int)(implicit id: TransientToEntryId[_]): (Slice[Byte], Option[Slice[Byte]], Int, Int) =
    if (currentTime.time.nonEmpty)
      current.previous.map(getTime) flatMap {
        previousTime =>
          //need to compress at least 4 bytes because the meta data required after compression is minimum 2 bytes.
          compress(previous = previousTime.time, next = currentTime.time, minimumCommonBytes = 4) map {
            case (_, remainingBytes) if remainingBytes.isEmpty =>

              val (indexBytes, valueBytes, valueStartOffset, valueEndOffset) =
                ValueWriter.write(
                  current = current,
                  compressDuplicateValues = compressDuplicateValues,
                  entryId = entryId.timeFullyCompressed,
                  plusSize = plusSize + sizeOf(currentTime.time.size)
                )

              val bytes =
                indexBytes
                  .addIntUnsigned(currentTime.time.size)

              (bytes, valueBytes, valueStartOffset, valueEndOffset)

            case (commonBytes, remainingBytes) =>
              val (indexBytes, valueBytes, valueStartOffset, valueEndOffset) =
                ValueWriter.write(
                  current = current,
                  compressDuplicateValues = compressDuplicateValues,
                  entryId = entryId.timePartiallyCompressed,
                  plusSize = plusSize + sizeOf(commonBytes) + sizeOf(remainingBytes.size) + remainingBytes.size
                )

              val bytes =
                indexBytes
                  .addIntUnsigned(commonBytes)
                  .addIntUnsigned(remainingBytes.size)
                  .addAll(remainingBytes)

              (bytes, valueBytes, valueStartOffset, valueEndOffset)
          }
      } getOrElse {
        //no common prefixes or no previous write without compression
        val (indexBytes, valueBytes, valueStartOffset, valueEndOffset) =
          ValueWriter.write(
            current = current,
            compressDuplicateValues = compressDuplicateValues,
            entryId = entryId.timeUncompressed,
            plusSize = plusSize + sizeOf(currentTime.time.size) + currentTime.time.size
          )

        val bytes =
          indexBytes
            .addIntUnsigned(currentTime.time.size)
            .addAll(currentTime.time)

        (bytes, valueBytes, valueStartOffset, valueEndOffset)
      }
    else {
      val (indexBytes, valueBytes, valueStartOffset, valueEndOffset) =
        ValueWriter.write(
          current = current,
          compressDuplicateValues = compressDuplicateValues,
          entryId = entryId.noTime,
          plusSize = plusSize
        )

      (indexBytes, valueBytes, valueStartOffset, valueEndOffset)
    }
}
