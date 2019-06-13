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

  private def writePartiallyCompressed(currentTime: Time,
                                       previousTime: Time,
                                       current: KeyValue.WriteOnly,
                                       compressDuplicateValues: Boolean,
                                       entryId: EntryId.Key,
                                       plusSize: Int)(implicit id: TransientToEntryId[_]) =
  //need to compress at least 4 bytes because the meta data required after compression is minimum 2 bytes.
    compress(previous = previousTime.time, next = currentTime.time, minimumCommonBytes = 4) map {
      case (commonBytes, remainingBytes) =>
        val writeResult =
          ValueWriter.write(
            current = current,
            compressDuplicateValues = compressDuplicateValues,
            entryId = entryId.timePartiallyCompressed,
            plusSize = plusSize + sizeOf(commonBytes) + sizeOf(remainingBytes.size) + remainingBytes.size
          )

        val indexBytes =
          writeResult
            .indexBytes
            .addIntUnsigned(commonBytes)
            .addIntUnsigned(remainingBytes.size)
            .addAll(remainingBytes)

        EntryWriter.Result(
          indexBytes = indexBytes,
          valueBytes = writeResult.valueBytes,
          valueStartOffset = writeResult.valueStartOffset,
          valueEndOffset = writeResult.valueEndOffset
        )
    }

  private def writeUncompressed(currentTime: Time,
                                current: KeyValue.WriteOnly,
                                compressDuplicateValues: Boolean,
                                entryId: EntryId.Key,
                                plusSize: Int)(implicit id: TransientToEntryId[_]) = {
    //no common prefixes or no previous write without compression
    val writeResult =
      ValueWriter.write(
        current = current,
        compressDuplicateValues = compressDuplicateValues,
        entryId = entryId.timeUncompressed,
        plusSize = plusSize + sizeOf(currentTime.time.size) + currentTime.time.size
      )

    val indexBytes =
      writeResult
        .indexBytes
        .addIntUnsigned(currentTime.time.size)
        .addAll(currentTime.time)

    EntryWriter.Result(
      indexBytes = indexBytes,
      valueBytes = writeResult.valueBytes,
      valueStartOffset = writeResult.valueStartOffset,
      valueEndOffset = writeResult.valueEndOffset
    )
  }

  def writeNoTime(current: KeyValue.WriteOnly,
                  compressDuplicateValues: Boolean,
                  entryId: EntryId.Key,
                  plusSize: Int)(implicit id: TransientToEntryId[_]) =
    ValueWriter.write(
      current = current,
      compressDuplicateValues = compressDuplicateValues,
      entryId = entryId.noTime,
      plusSize = plusSize
    )

  def write(current: KeyValue.WriteOnly,
            currentTime: Time,
            compressDuplicateValues: Boolean,
            entryId: EntryId.Key,
            plusSize: Int)(implicit id: TransientToEntryId[_]) =
    if (currentTime.time.nonEmpty)
      current.previous.map(getTime) flatMap {
        previousTime =>
          //need to compress at least 4 bytes because the meta data required after compression is minimum 2 bytes.
          writePartiallyCompressed(
            currentTime = currentTime,
            previousTime = previousTime,
            current = current,
            compressDuplicateValues = compressDuplicateValues,
            entryId = entryId,
            plusSize = plusSize
          )
      } getOrElse {
        //no common prefixes or no previous write without compression
        writeUncompressed(
          currentTime = currentTime,
          current = current,
          compressDuplicateValues = compressDuplicateValues,
          entryId = entryId,
          plusSize = plusSize
        )
      }
    else
      writeNoTime(
        current = current,
        compressDuplicateValues = compressDuplicateValues,
        entryId = entryId,
        plusSize = plusSize
      )
}
