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

import swaydb.core.data.Memory
import swaydb.core.segment.entry.id.{BaseEntryId, MemoryToKeyValueIdBinder}
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice

trait KeyWriter {
  def write[T <: Memory](current: T,
                         builder: EntryWriter.Builder,
                         deadlineId: BaseEntryId.Deadline)(implicit binder: MemoryToKeyValueIdBinder[T]): Unit
}

private[segment] object KeyWriter extends KeyWriter {

  /**
   * Format - keySize|key|keyValueId|accessIndex?|deadline|valueOffset|valueLength|time
   */
  def write[T <: Memory](current: T,
                         builder: EntryWriter.Builder,
                         deadlineId: BaseEntryId.Deadline)(implicit binder: MemoryToKeyValueIdBinder[T]): Unit = {
    val writtenCompressed =
      if (builder.enablePrefixCompressionForCurrentWrite && builder.previous.isSomeS)
        writeCompressed(
          current = current,
          builder = builder,
          deadlineId = deadlineId,
          previous = builder.previous.getS
        )
      else
        None

    if (writtenCompressed.isEmpty)
      writeUncompressed(
        current = current,
        builder = builder,
        deadlineId = deadlineId
      )
  }

  private def writeCompressed[T <: Memory](current: T,
                                           builder: EntryWriter.Builder,
                                           deadlineId: BaseEntryId.Deadline,
                                           previous: Memory)(implicit binder: MemoryToKeyValueIdBinder[T]): Option[Unit] =
    Bytes.compress(key = current.mergedKey, previous = previous, minimumCommonBytes = 3) map {
      case (commonBytes, remainingBytes) =>
        write(
          current = current,
          builder = builder,
          commonBytes = commonBytes,
          headerBytes = remainingBytes,
          deadlineId = deadlineId,
          isKeyCompressed = true
        )
    }

  private def writeUncompressed[T <: Memory](current: T,
                                             builder: EntryWriter.Builder,
                                             deadlineId: BaseEntryId.Deadline)(implicit binder: MemoryToKeyValueIdBinder[T]): Unit =

    write(
      current = current,
      builder = builder,
      commonBytes = -1,
      headerBytes = current.mergedKey,
      deadlineId = deadlineId,
      isKeyCompressed = false
    )

  private def write[T <: Memory](current: T,
                                 builder: EntryWriter.Builder,
                                 commonBytes: Int,
                                 headerBytes: Slice[Byte],
                                 deadlineId: BaseEntryId.Deadline,
                                 isKeyCompressed: Boolean)(implicit binder: MemoryToKeyValueIdBinder[T]): Unit = {
    val id =
      binder.keyValueId.adjustBaseIdToKeyValueIdKey(
        baseId = deadlineId.baseId,
        isKeyCompressed = isKeyCompressed
      )

    if (isKeyCompressed)
      builder.setSegmentHasPrefixCompression()

    val sortedIndexAccessPosition =
      if (builder.enableAccessPositionIndex)
        if (builder.isCurrentPrefixCompressed)
          builder.accessPositionIndex
        else
          builder.accessPositionIndex + 1
      else
        -1

    if (isKeyCompressed) {
      //keySize includes the size of the commonBytes and the key. This is so that when reading key-value in
      //SortedIndexBlock and estimating max entry size the commonBytes are also accounted. This also makes it
      //easy parsing key in KeyReader.
      val byteSizeOfCommonBytes = Bytes.sizeOfUnsignedInt(commonBytes)
      builder.bytes addUnsignedInt (headerBytes.size + byteSizeOfCommonBytes)
      builder.bytes addUnsignedInt commonBytes
    } else {
      builder.bytes addUnsignedInt headerBytes.size
    }

    builder.bytes addAll headerBytes

    builder.bytes addUnsignedInt id

    if (sortedIndexAccessPosition > 0) {
      builder.bytes addUnsignedInt sortedIndexAccessPosition
      builder.accessPositionIndex = sortedIndexAccessPosition
    }

    if (builder.optimiseForReverseIteration)
      builder.bytes addUnsignedInt builder.previousIndexOffset
  }
}
