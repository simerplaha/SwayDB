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
   * Format - keySize|key|accessIndex?|keyValueId|deadline|valueOffset|valueLength|time
   */
  def write(current: Transient,
            plusSize: Int,
            deadlineId: BaseEntryId.Deadline,
            enablePrefixCompression: Boolean,
            hasPrefixCompression: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean) =
    when(enablePrefixCompression)(current.previous) flatMap {
      previous =>
        writeCompressed(
          current = current,
          plusSize = plusSize,
          deadlineId = deadlineId,
          previous = previous,
          hasPrefixCompression = hasPrefixCompression
        )
    } getOrElse {
      writeUncompressed(
        current = current,
        plusSize = plusSize,
        deadlineId = deadlineId,
        hasPrefixCompression = hasPrefixCompression
      )
    }

  private def writeCompressed(current: Transient,
                              plusSize: Int,
                              deadlineId: BaseEntryId.Deadline,
                              previous: Transient,
                              hasPrefixCompression: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): Option[(Slice[Byte], Boolean)] =
    Bytes.compress(key = current.mergedKey, previous = previous, minimumCommonBytes = 2) map {
      case (commonBytes, remainingBytes) =>
        write(
          current = current,
          headerInteger = commonBytes,
          headerBytes = remainingBytes,
          plusSize = plusSize,
          deadlineId = deadlineId,
          isKeyCompressed = true,
          hasPrefixCompression = hasPrefixCompression
        )
    }

  private def writeUncompressed(current: Transient,
                                plusSize: Int,
                                deadlineId: BaseEntryId.Deadline,
                                hasPrefixCompression: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean) = {

    write(
      current = current,
      headerInteger = current.mergedKey.size,
      headerBytes = current.mergedKey,
      plusSize = plusSize,
      deadlineId = deadlineId,
      isKeyCompressed = false,
      hasPrefixCompression = hasPrefixCompression
    )
  }

  private def write(current: Transient,
                    headerInteger: Int,
                    headerBytes: Slice[Byte],
                    plusSize: Int,
                    deadlineId: BaseEntryId.Deadline,
                    isKeyCompressed: Boolean,
                    hasPrefixCompression: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean) = {
    val id =
      binder.keyValueId.adjustBaseIdToKeyValueIdKey(
        baseId = deadlineId.baseId,
        isKeyCompressed = isKeyCompressed
      )

    val isPrefixCompressed = hasPrefixCompression || isKeyCompressed

    val sortedIndexAccessPosition = getAccessIndexPosition(current, isPrefixCompressed)
    val sortedIndexAccessPositionSize = sortedIndexAccessPosition.valueOrElse(Bytes.sizeOfUnsignedInt, 0)

    val bytes =
      Slice.create[Byte](Bytes.sizeOfUnsignedInt(id) + Bytes.sizeOfUnsignedInt(headerInteger) + headerBytes.size + sortedIndexAccessPositionSize + plusSize)

    bytes
      .addUnsignedInt(headerInteger)
      .addAll(headerBytes)

    sortedIndexAccessPosition foreach bytes.addUnsignedInt

    bytes addUnsignedInt id

    (bytes, isPrefixCompressed)
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
