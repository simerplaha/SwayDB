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

  private[writer] def write[T](currentDeadline: Option[Deadline],
                               previousDeadline: Option[Deadline],
                               getDeadlineId: GetDeadlineId,
                               enablePrefixCompression: Boolean,
                               plusSize: Int,
                               isKeyCompressed: Boolean,
                               hasPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[T]): (Slice[Byte], Boolean) =
    currentDeadline map {
      currentDeadline: Deadline =>
        //fetch the previous deadline bytes
        (if (enablePrefixCompression) previousDeadline else None) flatMap {
          previousDeadline =>
            tryCompress(
              currentDeadline = currentDeadline,
              previousDeadline = previousDeadline,
              getDeadlineId = getDeadlineId,
              plusSize = plusSize,
              isKeyCompressed = isKeyCompressed,
              isPrefixCompressed = hasPrefixCompressed
            )
        } getOrElse {
          //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
          uncompressed(
            currentDeadline = currentDeadline,
            getDeadlineId = getDeadlineId,
            plusSize = plusSize,
            isKeyCompressed = isKeyCompressed,
            isPrefixCompressed = hasPrefixCompressed
          )
        }
    } getOrElse {
      noDeadline(
        getDeadlineId = getDeadlineId,
        plusSize = plusSize,
        isKeyCompressed = isKeyCompressed,
        isPrefixCompressed = hasPrefixCompressed
      )
    }

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
                                   isKeyCompressed: Boolean,
                                   isPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean) = {
    //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
    val currentDeadlineUnsignedBytes = currentDeadline.toLongUnsignedBytes
    val deadlineId = getDeadlineId.deadlineUncompressed
    val adjustedToEntryIdDeadlineId =
      binder.keyValueId.adjustBaseIdToKeyValueIdKey(
        baseId = deadlineId.baseId,
        keyCompressed = isKeyCompressed
      )

    val bytes =
      Slice.create[Byte](sizeOf(adjustedToEntryIdDeadlineId) + currentDeadlineUnsignedBytes.size + plusSize)
        .addIntUnsigned(adjustedToEntryIdDeadlineId)
        .addAll(currentDeadlineUnsignedBytes)

    (bytes, isKeyCompressed || isPrefixCompressed)
  }

  private[writer] def tryCompress(currentDeadline: Deadline,
                                  previousDeadline: Deadline,
                                  getDeadlineId: GetDeadlineId,
                                  plusSize: Int,
                                  isKeyCompressed: Boolean,
                                  isPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): Option[(Slice[Byte], Boolean)] =
    compress(
      previous = previousDeadline.toBytes,
      next = currentDeadline.toBytes,
      minimumCommonBytes = 1
    ) map {
      case (deadlineCommonBytes, deadlineCompressedBytes) =>

        val deadlineId = applyDeadlineId(deadlineCommonBytes, getDeadlineId)
        val adjustedToEntryIdDeadlineId = binder.keyValueId.adjustBaseIdToKeyValueIdKey(deadlineId.baseId, isKeyCompressed)
        val bytes =
          Slice.create[Byte](sizeOf(adjustedToEntryIdDeadlineId) + deadlineCompressedBytes.size + plusSize)
            .addIntUnsigned(adjustedToEntryIdDeadlineId)
            .addAll(deadlineCompressedBytes)

        (bytes, true)
    }

  private[writer] def noDeadline(getDeadlineId: GetDeadlineId,
                                 plusSize: Int,
                                 isKeyCompressed: Boolean,
                                 isPrefixCompressed: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean) = {
    //if current key-value has no deadline.
    val deadlineId = getDeadlineId.noDeadline
    val adjustedToEntryIdDeadlineId =
      binder.keyValueId.adjustBaseIdToKeyValueIdKey(
        baseId = deadlineId.baseId,
        keyCompressed = isKeyCompressed
      )
    val bytes =
      Slice.create[Byte](sizeOf(adjustedToEntryIdDeadlineId) + plusSize)
        .addIntUnsigned(adjustedToEntryIdDeadlineId)

    (bytes, isKeyCompressed || isPrefixCompressed)
  }
}
