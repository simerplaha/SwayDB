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
import swaydb.core.segment.format.a.entry.id.BaseEntryId.BaseEntryIdFormat
import swaydb.core.segment.format.a.entry.id.{BaseEntryIdFormatA, KeyValueId, TransientToKeyValueIdBinder}
import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

private[core] object EntryWriter {

  case class WriteResult(indexBytes: Slice[Byte],
                         valueBytes: Option[Slice[Byte]],
                         valueStartOffset: Int,
                         valueEndOffset: Int,
                         thisKeyValueAccessIndexPosition: Int,
                         isPrefixCompressed: Boolean) {
    //TODO check if companion object function unapply returning an Option[Result] is cheaper than this unapply function.
    def unapply =
      (indexBytes, valueBytes, valueStartOffset, valueEndOffset, thisKeyValueAccessIndexPosition, isPrefixCompressed)
  }

  private val tailBytes =
    KeyValueId.maxByteSize + //keyValueId
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
   * @param binder                  [[BaseEntryIdFormat]] for this key-value's type.
   * @param compressDuplicateValues Compresses duplicate values if set to true.
   * @return indexEntry, valueBytes, valueOffsetBytes, nextValuesOffsetPosition
   */
  def write[T <: Transient](current: T,
                            currentTime: Time,
                            normaliseToSize: Option[Int],
                            compressDuplicateValues: Boolean,
                            enablePrefixCompression: Boolean)(implicit binder: TransientToKeyValueIdBinder[T]): EntryWriter.WriteResult =
    TimeWriter.write(
      current = current,
      currentTime = currentTime,
      compressDuplicateValues = compressDuplicateValues,
      entryId = BaseEntryIdFormatA.format.start,
      enablePrefixCompression = enablePrefixCompression,
      normaliseToSize = normaliseToSize
    )

  def maxEntrySize(keySize: Int,
                   hasAccessIndexPosition: Boolean): Int =
    Bytes.sizeOfUnsignedInt(keySize) + //size of key
      keySize + //key itself
      (if (hasAccessIndexPosition) ByteSizeOf.varInt else 0) + //accessIndexPosition
      tailBytes

  def maxEntrySize(hasAccessIndexPosition: Boolean): Int =
    (if (hasAccessIndexPosition) ByteSizeOf.varInt else 0) + //accessIndexPosition
      tailBytes
}
