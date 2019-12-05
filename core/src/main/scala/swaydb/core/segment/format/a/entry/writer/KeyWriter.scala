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

import swaydb.core.data.Transient
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, TransientToKeyValueIdBinder}
import swaydb.core.util.Options._
import swaydb.core.util.{Bytes, Options}
import swaydb.data.slice.Slice

private[core] object KeyWriter {

  /**
   * Format - keySize|key|keyValueId|accessIndex?|deadline|valueOffset|valueLength|time
   */
  def write(current: Transient,
            plusSize: Int,
            deadlineId: BaseEntryId.Deadline,
            enablePrefixCompression: Boolean,
            hasPrefixCompression: Boolean,
            normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean, Option[Int]) =
    when(enablePrefixCompression)(current.previous) flatMap {
      previous =>
        writeCompressed(
          current = current,
          plusSize = plusSize,
          deadlineId = deadlineId,
          previous = previous,
          hasPrefixCompression = hasPrefixCompression,
          normaliseToSize = normaliseToSize
        )
    } getOrElse {
      writeUncompressed(
        current = current,
        plusSize = plusSize,
        deadlineId = deadlineId,
        hasPrefixCompression = hasPrefixCompression,
        normaliseToSize = normaliseToSize
      )
    }

  private def writeCompressed(current: Transient,
                              plusSize: Int,
                              deadlineId: BaseEntryId.Deadline,
                              previous: Transient,
                              hasPrefixCompression: Boolean,
                              normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]): Option[(Slice[Byte], Boolean, Option[Int])] =
    Bytes.compress(key = current.mergedKey, previous = previous, minimumCommonBytes = 3) map {
      case (commonBytes, remainingBytes) =>
        write(
          current = current,
          commonBytes = commonBytes,
          headerBytes = remainingBytes,
          plusSize = plusSize,
          deadlineId = deadlineId,
          isKeyCompressed = true,
          hasPrefixCompression = hasPrefixCompression,
          normaliseToSize = normaliseToSize
        )
    }

  private def writeUncompressed(current: Transient,
                                plusSize: Int,
                                deadlineId: BaseEntryId.Deadline,
                                hasPrefixCompression: Boolean,
                                normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean, Option[Int]) = {

    write(
      current = current,
      commonBytes = -1,
      headerBytes = current.mergedKey,
      plusSize = plusSize,
      deadlineId = deadlineId,
      isKeyCompressed = false,
      hasPrefixCompression = hasPrefixCompression,
      normaliseToSize = normaliseToSize
    )
  }

  private def write(current: Transient,
                    commonBytes: Int,
                    headerBytes: Slice[Byte],
                    plusSize: Int,
                    deadlineId: BaseEntryId.Deadline,
                    isKeyCompressed: Boolean,
                    hasPrefixCompression: Boolean,
                    normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean, Option[Int]) = {
    val id =
      binder.keyValueId.adjustBaseIdToKeyValueIdKey(
        baseId = deadlineId.baseId,
        isKeyCompressed = isKeyCompressed
      )

    val isPrefixCompressed = hasPrefixCompression || isKeyCompressed

    val sortedIndexAccessPosition = getAccessIndexPosition(current, isPrefixCompressed)
    val sortedIndexAccessPositionSize = sortedIndexAccessPosition.valueOrElse(Bytes.sizeOfUnsignedInt, 0)

    val byteSizeOfCommonBytes = Bytes.sizeOfUnsignedInt(commonBytes)

    val requiredSpace =
      if (normaliseToSize.isDefined)
        normaliseToSize.get
      else if (isKeyCompressed)
        Bytes.sizeOfUnsignedInt(id) +
          //keySize includes the size of the commonBytes and the key. This is so that when reading key-value in
          //SortedIndexBlock and estimating max entry size the commonBytes are also accounted. This also makes it
          //easy parsing key in KeyReader.
          Bytes.sizeOfUnsignedInt(headerBytes.size + byteSizeOfCommonBytes) +
          byteSizeOfCommonBytes +
          headerBytes.size +
          sortedIndexAccessPositionSize +
          plusSize
      else
        Bytes.sizeOfUnsignedInt(id) +
          Bytes.sizeOfUnsignedInt(headerBytes.size) +
          headerBytes.size +
          sortedIndexAccessPositionSize +
          plusSize

    val bytes = Slice.create[Byte](length = requiredSpace, isFull = normaliseToSize.isDefined)

    if (normaliseToSize.isDefined) bytes moveWritePosition 0

    if (isKeyCompressed) {
      bytes addUnsignedInt (headerBytes.size + byteSizeOfCommonBytes)
      bytes addUnsignedInt commonBytes
    } else {
      bytes addUnsignedInt headerBytes.size
    }

    bytes addAll headerBytes

    bytes addUnsignedInt id

    sortedIndexAccessPosition foreach bytes.addUnsignedInt

    (bytes, isPrefixCompressed, sortedIndexAccessPosition)
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
