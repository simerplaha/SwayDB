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

import swaydb.core.segment.format.a.entry.id.BaseEntryId.GetDeadlineId
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, TransientToKeyValueIdBinder}
import swaydb.core.util.Bytes._
import swaydb.core.util.TimeUtil._
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

private[writer] object DeadlineWriter {

  private[writer] def applyDeadlineId(bytesCompressed: Int,
                                      getDeadlineId: GetDeadlineId): BaseEntryId.Deadline =
    if (bytesCompressed == 1)
      getDeadlineId.deadlineOneCompressed
    else if (bytesCompressed == 2)
      getDeadlineId.deadlineTwoCompressed
    else if (bytesCompressed == 3)
      getDeadlineId.deadlineThreeCompressed
    else if (bytesCompressed == 4)
      getDeadlineId.deadlineFourCompressed
    else if (bytesCompressed == 5)
      getDeadlineId.deadlineFiveCompressed
    else if (bytesCompressed == 6)
      getDeadlineId.deadlineSixCompressed
    else if (bytesCompressed == 7)
      getDeadlineId.deadlineSevenCompressed
    else if (bytesCompressed == 8)
      getDeadlineId.deadlineFullyCompressed
    else
      throw new Exception(s"Fatal exception: deadlineBytesCompressed = $bytesCompressed")

  private[writer] def uncompressed(currentDeadline: Deadline,
                                   getDeadlineId: GetDeadlineId,
                                   plusSize: Int,
                                   isKeyCompressed: Boolean)(implicit id: TransientToKeyValueIdBinder[_]): Slice[Byte] = {
    //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
    val currentDeadlineUnsignedBytes = currentDeadline.toLongUnsignedBytes
    val deadlineId = getDeadlineId.deadlineUncompressed.baseId
    val adjustedToEntryIdDeadlineId = id.keyValueId.adjustBaseIdToKeyValueIdKey(deadlineId, isKeyCompressed)

    Slice.create[Byte](sizeOf(adjustedToEntryIdDeadlineId) + currentDeadlineUnsignedBytes.size + plusSize)
      .addIntUnsigned(adjustedToEntryIdDeadlineId)
      .addAll(currentDeadlineUnsignedBytes)
  }

  private[writer] def tryCompress(currentDeadline: Deadline,
                                  previousDeadline: Deadline,
                                  getDeadlineId: GetDeadlineId,
                                  plusSize: Int,
                                  isKeyCompressed: Boolean)(implicit id: TransientToKeyValueIdBinder[_]) =
    compress(
      previous = previousDeadline.toBytes,
      next = currentDeadline.toBytes,
      minimumCommonBytes = 1
    ) map {
      case (deadlineCommonBytes, deadlineCompressedBytes) =>

        val deadlineId = applyDeadlineId(deadlineCommonBytes, getDeadlineId).baseId
        val adjustedToEntryIdDeadlineId = id.keyValueId.adjustBaseIdToKeyValueIdKey(deadlineId, isKeyCompressed)

        Slice.create[Byte](sizeOf(adjustedToEntryIdDeadlineId) + deadlineCompressedBytes.size + plusSize)
          .addIntUnsigned(adjustedToEntryIdDeadlineId)
          .addAll(deadlineCompressedBytes)
    }

  private[writer] def noDeadline(getDeadlineId: GetDeadlineId,
                                 plusSize: Int,
                                 isKeyCompressed: Boolean)(implicit id: TransientToKeyValueIdBinder[_]) = {
    //if current key-value has no deadline.
    val deadlineId = getDeadlineId.noDeadline.baseId
    val adjustedToEntryIdDeadlineId = id.keyValueId.adjustBaseIdToKeyValueIdKey(deadlineId, isKeyCompressed)

    Slice.create[Byte](sizeOf(adjustedToEntryIdDeadlineId) + plusSize)
      .addIntUnsigned(adjustedToEntryIdDeadlineId)
  }

  private[writer] def write(current: Option[Deadline],
                            previous: Option[Deadline],
                            getDeadlineId: GetDeadlineId,
                            enablePrefixCompression: Boolean,
                            plusSize: Int,
                            isKeyCompressed: Boolean)(implicit id: TransientToKeyValueIdBinder[_]): Slice[Byte] =
    current map {
      currentDeadline: Deadline =>
        //fetch the previous deadline bytes
        (if (enablePrefixCompression) previous else None) flatMap {
          previousDeadline =>
            tryCompress(
              currentDeadline = currentDeadline,
              previousDeadline = previousDeadline,
              getDeadlineId = getDeadlineId,
              plusSize = plusSize,
              isKeyCompressed = isKeyCompressed
            )
        } getOrElse {
          //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
          uncompressed(
            currentDeadline = currentDeadline,
            getDeadlineId = getDeadlineId,
            plusSize = plusSize,
            isKeyCompressed = isKeyCompressed
          )
        }
    } getOrElse {
      noDeadline(
        getDeadlineId = getDeadlineId,
        plusSize = plusSize,
        isKeyCompressed = isKeyCompressed
      )
    }
}
