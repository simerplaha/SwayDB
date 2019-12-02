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
import swaydb.core.segment.format.a.entry.id.{BaseEntryIdFormatA, TransientToKeyValueIdBinder}
import swaydb.core.util.Bytes._
import swaydb.core.util.Options._
import swaydb.core.util.{Bytes, Options}
import swaydb.data.slice.Slice
import swaydb.data.util.ByteSizeOf

import scala.beans.BeanProperty

private[core] object EntryWriter {

  case class WriteResult(@BeanProperty var indexBytes: Slice[Byte],
                         valueBytes: Option[Slice[Byte]],
                         valueStartOffset: Int,
                         valueEndOffset: Int,
                         @BeanProperty var thisKeyValueAccessIndexPosition: Int,
                         @BeanProperty var keyOffset: Int,
                         isPrefixCompressed: Boolean) {
    //TODO check if companion object function unapply returning an Option[Result] is cheaper than this unapply function.
    def unapply =
      (indexBytes, valueBytes, valueStartOffset, valueEndOffset, thisKeyValueAccessIndexPosition, keyOffset, isPrefixCompressed)
  }

  /**
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
    when(enablePrefixCompression)(current.previous) flatMap {
      previous =>
        writeCompressed(
          current = current,
          previous = previous,
          normaliseToSize = normaliseToSize,
          currentTime = currentTime,
          compressDuplicateValues = compressDuplicateValues
        )
    } getOrElse {
      writeUncompressed(
        current = current,
        currentTime = currentTime,
        normaliseToSize = normaliseToSize,
        compressDuplicateValues = compressDuplicateValues,
        enablePrefixCompression = enablePrefixCompression
      )
    }

  private def writeCompressed[T <: Transient](current: T,
                                              previous: Transient,
                                              currentTime: Time,
                                              normaliseToSize: Option[Int],
                                              compressDuplicateValues: Boolean)(implicit binder: TransientToKeyValueIdBinder[T]) =
    compress(key = current.mergedKey, previous = previous, minimumCommonBytes = 2) map {
      case (commonBytes, remainingBytes) =>

        val commonByteSize = sizeOfUnsignedInt(commonBytes)
        val keySize = commonByteSize + remainingBytes.size

        val writeResult =
          TimeWriter.write(
            current = current,
            currentTime = currentTime,
            compressDuplicateValues = compressDuplicateValues,
            entryId = BaseEntryIdFormatA.format.start,
            enablePrefixCompression = true,
            isKeyCompressed = true,
            hasPrefixCompressed = true,
            plusSize = keySize + ByteSizeOf.varInt //write the size of keys compressed.
          )

        writeResult
          .indexBytes
          .addUnsignedInt(commonBytes)
          .addAll(remainingBytes)

        close(
          normaliseToSize = normaliseToSize,
          writeResult = writeResult,
          keySize = keySize,
          current = current
        )

        writeResult
    }

  private def writeUncompressed[T <: Transient](current: T,
                                                currentTime: Time,
                                                normaliseToSize: Option[Int],
                                                compressDuplicateValues: Boolean,
                                                enablePrefixCompression: Boolean)(implicit binder: TransientToKeyValueIdBinder[T]): WriteResult = {
    val bytesRequired =
      if (normaliseToSize.isDefined)
        0
      else
        current.mergedKey.size

    val writeResult =
      TimeWriter.write(
        current = current,
        currentTime = currentTime,
        compressDuplicateValues = compressDuplicateValues,
        entryId = BaseEntryIdFormatA.format.start,
        enablePrefixCompression = enablePrefixCompression,
        isKeyCompressed = false,
        hasPrefixCompressed = false,
        plusSize = bytesRequired
      )

    if (normaliseToSize.isEmpty)
      writeResult
        .indexBytes
        .addAll(current.mergedKey)

    close(
      normaliseToSize = normaliseToSize,
      writeResult = writeResult,
      keySize = current.mergedKey.size,
      current = current
    )

    writeResult
  }

  //default format    - indexSize|accessIndex?|keyValueId|valueOffset|valueLength|deadline|key
  //normalised format - indexSize|accessIndex?|keySize|key|keyValueId|valueOffset|valueLength|deadline|normalisedBytes
  def close[T <: Transient](normaliseToSize: Option[Int],
                            writeResult: WriteResult,
                            keySize: Int,
                            current: Transient): Unit = {

    val sortedIndexAccessPosition = getAccessIndexPosition(current, writeResult.isPrefixCompressed)
    //println(s"Access position: $sortedIndexAccessPosition for key: ${current.key.readInt()}")

    val (closedBytes, keyOffset) =
      normaliseToSize match {
        case Some(toSize) =>
          val indexSize = toSize - Bytes.sizeOfUnsignedInt(toSize)
          val bytes = Slice.create[Byte](toSize)

          val (normalisedBytes, keyOffset) = {
            bytes addUnsignedInt indexSize
            sortedIndexAccessPosition foreach bytes.addUnsignedInt
            bytes addUnsignedInt keySize
            val keyOffset = bytes.currentWritePosition
            bytes addAll current.mergedKey
            bytes addAll writeResult.indexBytes
            (bytes, keyOffset)
          }

//          val keyOffset = normalisedBytes.currentWritePosition - keySize
          normalisedBytes moveWritePosition toSize
          (normalisedBytes, keyOffset)

        case None =>
          val indexSize = writeResult.indexBytes.size + sortedIndexAccessPosition.map(Bytes.sizeOfUnsignedInt).getOrElse(0)
          val bytes = Slice.create[Byte](Bytes.sizeOfUnsignedInt(indexSize) + indexSize)
          bytes addUnsignedInt indexSize
          sortedIndexAccessPosition foreach bytes.addUnsignedInt
          bytes addAll writeResult.indexBytes
          val keyOffset = bytes.currentWritePosition - keySize
          (bytes, keyOffset)
      }

    assert(closedBytes.isOriginalFullSlice)

    writeResult setKeyOffset keyOffset //the offset of the key within the current closedBytes.
    writeResult setIndexBytes closedBytes
    sortedIndexAccessPosition foreach writeResult.setThisKeyValueAccessIndexPosition
  }

  def getAccessIndexPosition(current: Transient, isPrefixCompressed: Boolean): Option[Int] =
    if (current.sortedIndexConfig.enableAccessPositionIndex)
      if (isPrefixCompressed)
        current.previous.map(_.thisKeyValueAccessIndexPosition) orElse Options.one
      else
        current.previous.map(_.thisKeyValueAccessIndexPosition + 1) orElse Options.one
    else
      None
}
