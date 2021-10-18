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
