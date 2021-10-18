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

import swaydb.core.data.{Memory, MemoryOption}
import swaydb.core.segment.entry.id.{BaseEntryIdFormatA, KeyValueId, MemoryToKeyValueIdBinder}
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.utils.ByteSizeOf

private[core] object EntryWriter {

  object Builder {
    def apply(prefixCompressKeysOnly: Boolean,
              compressDuplicateValues: Boolean,
              enableAccessPositionIndex: Boolean,
              optimiseForReverseIteration: Boolean,
              bytes: Slice[Byte]): Builder =
      new Builder(
        enablePrefixCompressionForCurrentWrite = false,
        prefixCompressKeysOnly = prefixCompressKeysOnly,
        compressDuplicateValues = compressDuplicateValues,
        isValueFullyCompressed = false,
        enableAccessPositionIndex = enableAccessPositionIndex,
        optimiseForReverseIteration = optimiseForReverseIteration,
        bytes = bytes,
        startValueOffset = 0,
        endValueOffset = -1,
        accessPositionIndex = 0,
        previous = Memory.Null,
        previousIndexOffset = 0,
        isCurrentPrefixCompressed = false,
        _segmentHasPrefixCompression = false
      )
  }

  class Builder(var enablePrefixCompressionForCurrentWrite: Boolean,
                val prefixCompressKeysOnly: Boolean,
                var compressDuplicateValues: Boolean,
                //this should be reset to false once the entry is written
                var isValueFullyCompressed: Boolean,
                val enableAccessPositionIndex: Boolean,
                val optimiseForReverseIteration: Boolean,
                val bytes: Slice[Byte],
                var startValueOffset: Int,
                var endValueOffset: Int,
                var accessPositionIndex: Int,
                var previous: MemoryOption,
                var previousIndexOffset: Int,
                //this should be reset to false once the entry is written
                var isCurrentPrefixCompressed: Boolean,
                private var _segmentHasPrefixCompression: Boolean) {

    def segmentHasPrefixCompression = _segmentHasPrefixCompression

    def setSegmentHasPrefixCompression() = {
      //this flag is an indicator for SortedIndex that current write was prefix compressed.
      //this should be reset with every write by SortedIndex.
      this.isCurrentPrefixCompressed = true
      this._segmentHasPrefixCompression = true
    }

    def nextStartValueOffset: Int =
      if (endValueOffset == -1)
        0
      else
        endValueOffset + 1
  }

  private val tailBytes =
    KeyValueId.maxKeyValueIdByteSize + //keyValueId
      ByteSizeOf.varLong + //deadline
      ByteSizeOf.varInt + //valueOffset
      ByteSizeOf.varInt + //valueLength
      ByteSizeOf.int + //timeLength
      ByteSizeOf.varLong //time

  /**
   * Format - keySize|key|keyValueId|accessIndex?|deadline|valueOffset|valueLength|time
   *
   * Returns the index bytes and value bytes for the key-value and also the used
   * value offset information for writing the next key-value.
   *
   * Each key also has a meta block which can be used to backward compatibility to store
   * more information for that key in the future that does not fit the current key format.
   *
   * Currently all keys are being stored under EmptyMeta.
   *
   * Note: No extra bytes are required to differentiate between a key that has meta or no meta block.
   *
   * @param binder [[BaseEntryIdFormat]] for this key-value's type.
   * @return indexEntry, valueBytes, valueOffsetBytes, nextValuesOffsetPosition
   */
  def write[T <: Memory](current: T,
                         builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                       timeWriter: TimeWriter,
                                                       valueWriter: ValueWriter,
                                                       keyWriter: KeyWriter,
                                                       deadlineWriter: DeadlineWriter): Unit =
    timeWriter.write(
      current = current,
      entryId = BaseEntryIdFormatA.format.start,
      builder = builder
    )

  @inline def maxEntrySize(keySize: Int,
                           hasAccessIndexPosition: Boolean,
                           optimiseForReverseIteration: Boolean): Int =
    Bytes.sizeOfUnsignedInt(keySize) + //size of key
      keySize + //key itself
      maxEntrySize(
        hasAccessIndexPosition = hasAccessIndexPosition,
        optimiseForReverseIteration = optimiseForReverseIteration
      )

  @inline def maxEntrySize(hasAccessIndexPosition: Boolean,
                           optimiseForReverseIteration: Boolean): Int = {
    var size = tailBytes
    if (hasAccessIndexPosition) size += ByteSizeOf.varInt
    if (optimiseForReverseIteration) size += ByteSizeOf.varInt
    size
  }
}
