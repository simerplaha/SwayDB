/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
 *
 * This file is a part of SwayDB.
 *
 * SwayDB is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
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
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, TransientToKeyValueIdBinder}
import swaydb.core.util.Bytes._

private[writer] object TimeWriter {

  private[writer] def write[T](current: KeyValue.WriteOnly,
                               currentTime: Time,
                               compressDuplicateValues: Boolean,
                               entryId: BaseEntryId.Key,
                               enablePrefixCompression: Boolean,
                               plusSize: Int,
                               isKeyCompressed: Boolean,
                               hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[T]) =
    if (currentTime.time.nonEmpty)
      (if (enablePrefixCompression) current.previous.map(getTime) else None) flatMap {
        previousTime =>
          //need to compress at least 4 bytes because the meta data required after compression is minimum 2 bytes.
          writePartiallyCompressed(
            currentTime = currentTime,
            previousTime = previousTime,
            current = current,
            compressDuplicateValues = compressDuplicateValues,
            entryId = entryId,
            plusSize = plusSize,
            enablePrefixCompression = enablePrefixCompression,
            isKeyUncompressed = isKeyCompressed
          )
      } getOrElse {
        //no common prefixes or no previous write without compression
        writeUncompressed(
          currentTime = currentTime,
          current = current,
          compressDuplicateValues = compressDuplicateValues,
          entryId = entryId,
          plusSize = plusSize,
          enablePrefixCompression = enablePrefixCompression,
          isKeyUncompressed = isKeyCompressed,
          hasPrefixCompressed = hasPrefixCompressed
        )
      }
    else
      noTime(
        current = current,
        compressDuplicateValues = compressDuplicateValues,
        entryId = entryId,
        plusSize = plusSize,
        enablePrefixCompression = enablePrefixCompression,
        isKeyUncompressed = isKeyCompressed,
        hasPrefixCompressed = hasPrefixCompressed
      )

  private[writer] def getTime(keyValue: KeyValue.WriteOnly): Time =
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
            keyValue.time
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
                                       entryId: BaseEntryId.Key,
                                       plusSize: Int,
                                       enablePrefixCompression: Boolean,
                                       isKeyUncompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): Option[KeyValueWriter.Result] =
    compress(
      previous = previousTime.time,
      next = currentTime.time,
      minimumCommonBytes = 1
    ) map {
      case (commonBytes, remainingBytes) =>
        val writeResult =
          ValueWriter.write(
            current = current,
            compressDuplicateValues = compressDuplicateValues,
            entryId = entryId.timePartiallyCompressed,
            plusSize = plusSize + sizeOf(commonBytes) + sizeOf(remainingBytes.size) + remainingBytes.size,
            enablePrefixCompression = enablePrefixCompression,
            isKeyUncompressed = isKeyUncompressed,
            hasPrefixCompressed = true
          )

        writeResult
          .indexBytes
          .addIntUnsigned(commonBytes)
          .addIntUnsigned(remainingBytes.size)
          .addAll(remainingBytes)

        writeResult
    }

  private def writeUncompressed(currentTime: Time,
                                current: KeyValue.WriteOnly,
                                compressDuplicateValues: Boolean,
                                entryId: BaseEntryId.Key,
                                plusSize: Int,
                                enablePrefixCompression: Boolean,
                                isKeyUncompressed: Boolean,
                                hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]) = {
    //no common prefixes or no previous write without compression
    val writeResult =
      ValueWriter.write(
        current = current,
        compressDuplicateValues = compressDuplicateValues,
        entryId = entryId.timeUncompressed,
        plusSize = plusSize + sizeOf(currentTime.time.size) + currentTime.time.size,
        enablePrefixCompression = enablePrefixCompression,
        isKeyUncompressed = isKeyUncompressed,
        hasPrefixCompressed = hasPrefixCompressed
      )

    writeResult
      .indexBytes
      .addIntUnsigned(currentTime.time.size)
      .addAll(currentTime.time)

    writeResult
  }

  private def noTime(current: KeyValue.WriteOnly,
                     compressDuplicateValues: Boolean,
                     entryId: BaseEntryId.Key,
                     plusSize: Int,
                     enablePrefixCompression: Boolean,
                     isKeyUncompressed: Boolean,
                     hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]) =
    ValueWriter.write(
      current = current,
      compressDuplicateValues = compressDuplicateValues,
      entryId = entryId.noTime,
      plusSize = plusSize,
      enablePrefixCompression = enablePrefixCompression,
      isKeyUncompressed = isKeyUncompressed,
      hasPrefixCompressed = hasPrefixCompressed
    )
}
