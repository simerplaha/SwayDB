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

import swaydb.core.data.{Time, Transient}
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, TransientToKeyValueIdBinder}
import swaydb.core.util.Bytes._
import swaydb.core.util.Options._

private[writer] object TimeWriter {

  private[writer] def write(current: Transient,
                            currentTime: Time,
                            compressDuplicateValues: Boolean,
                            entryId: BaseEntryId.Key,
                            enablePrefixCompression: Boolean,
                            normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]): EntryWriter.WriteResult =
    if (currentTime.time.nonEmpty)
      when(enablePrefixCompression && !current.sortedIndexConfig.prefixCompressKeysOnly)(current.previous.map(getTime)) flatMap {
        previousTime =>
          //need to compress at least 4 bytes because the meta data required after compression is minimum 2 bytes.
          writePartiallyCompressed(
            currentTime = currentTime,
            previousTime = previousTime,
            current = current,
            compressDuplicateValues = compressDuplicateValues,
            entryId = entryId,
            enablePrefixCompression = enablePrefixCompression,
            normaliseToSize = normaliseToSize
          )
      } getOrElse {
        //no common prefixes or no previous write without compression
        writeUncompressed(
          currentTime = currentTime,
          current = current,
          compressDuplicateValues = compressDuplicateValues,
          entryId = entryId,
          enablePrefixCompression = enablePrefixCompression,
          normaliseToSize = normaliseToSize
        )
      }
    else
      noTime(
        current = current,
        compressDuplicateValues = compressDuplicateValues,
        entryId = entryId,
        enablePrefixCompression = enablePrefixCompression,
        normaliseToSize = normaliseToSize
      )

  private[writer] def getTime(keyValue: Transient): Time =
    keyValue match {
      case keyValue: Transient.Fixed =>
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
      case _: Transient.Range =>
        Time.empty
    }

  private def writePartiallyCompressed(currentTime: Time,
                                       previousTime: Time,
                                       current: Transient,
                                       compressDuplicateValues: Boolean,
                                       entryId: BaseEntryId.Key,
                                       enablePrefixCompression: Boolean,
                                       normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]): Option[EntryWriter.WriteResult] =
    compress(
      previous = previousTime.time,
      next = currentTime.time,
      minimumCommonBytes = 3 //minimum 3 required because commonBytes & uncompressedByteSize requires 2 bytes.
    ) map {
      case (commonBytes, remainingBytes) =>
        val writeResult =
          ValueWriter.write(
            current = current,
            enablePrefixCompression = enablePrefixCompression,
            compressDuplicateValues = compressDuplicateValues,
            entryId = entryId.timePartiallyCompressed,
            plusSize = sizeOfUnsignedInt(commonBytes) + sizeOfUnsignedInt(remainingBytes.size) + remainingBytes.size,
            hasPrefixCompression = true,
            normaliseToSize = normaliseToSize
          )

        writeResult
          .indexBytes
          .addUnsignedInt(commonBytes)
          .addUnsignedInt(remainingBytes.size)
          .addAll(remainingBytes)

        writeResult
    }

  private def writeUncompressed(currentTime: Time,
                                current: Transient,
                                compressDuplicateValues: Boolean,
                                entryId: BaseEntryId.Key,
                                enablePrefixCompression: Boolean,
                                normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]) = {
    //no common prefixes or no previous write without compression
    val writeResult =
      ValueWriter.write(
        current = current,
        enablePrefixCompression = enablePrefixCompression,
        compressDuplicateValues = compressDuplicateValues,
        entryId = entryId.timeUncompressed,
        plusSize = sizeOfUnsignedInt(currentTime.time.size) + currentTime.time.size,
        hasPrefixCompression = false,
        normaliseToSize = normaliseToSize
      )

    writeResult
      .indexBytes
      .addUnsignedInt(currentTime.time.size)
      .addAll(currentTime.time)

    writeResult
  }

  private def noTime(current: Transient,
                     compressDuplicateValues: Boolean,
                     entryId: BaseEntryId.Key,
                     enablePrefixCompression: Boolean,
                     normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]) =
    ValueWriter.write(
      current = current,
      enablePrefixCompression = enablePrefixCompression,
      compressDuplicateValues = compressDuplicateValues,
      entryId = entryId.noTime,
      plusSize = 0,
      hasPrefixCompression = false,
      normaliseToSize = normaliseToSize
    )
}
