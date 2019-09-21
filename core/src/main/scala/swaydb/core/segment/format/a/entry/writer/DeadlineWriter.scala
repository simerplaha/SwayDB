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

import swaydb.core.segment.format.a.entry.id.BaseEntryId.DeadlineId
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, TransientToKeyValueIdBinder}
import swaydb.core.util.Bytes
import swaydb.core.util.Times._
import swaydb.data.slice.Slice
import swaydb.core.util.Options._

import scala.concurrent.duration.Deadline

private[writer] object DeadlineWriter {

  private[writer] def write[T](currentDeadline: Option[Deadline],
                               previousDeadline: Option[Deadline],
                               deadlineId: DeadlineId,
                               enablePrefixCompression: Boolean,
                               plusSize: Int,
                               isKeyCompressed: Boolean,
                               hasPrefixCompressed: Boolean,
                               adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[T]): (Slice[Byte], Boolean) =
    currentDeadline map {
      currentDeadline: Deadline =>
        //fetch the previous deadline bytes
        when(enablePrefixCompression)(previousDeadline) flatMap {
          previousDeadline =>
            compress(
              currentDeadline = currentDeadline,
              previousDeadline = previousDeadline,
              deadlineId = deadlineId,
              plusSize = plusSize,
              isKeyCompressed = isKeyCompressed,
              adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
            )
        } getOrElse {
          //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
          uncompressed(
            currentDeadline = currentDeadline,
            deadlineId = deadlineId,
            plusSize = plusSize,
            isKeyCompressed = isKeyCompressed,
            hasPrefixCompressed = hasPrefixCompressed,
            adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
          )
        }
    } getOrElse {
      noDeadline(
        deadlineId = deadlineId,
        plusSize = plusSize,
        isKeyCompressed = isKeyCompressed,
        hasPrefixCompressed = hasPrefixCompressed,
        adjustBaseIdToKeyValueId = adjustBaseIdToKeyValueId
      )
    }

  private[writer] def applyDeadlineId(bytesCompressed: Int,
                                      deadlineId: DeadlineId): BaseEntryId.Deadline =
    if (bytesCompressed == 1)
      deadlineId.deadlineOneCompressed
    else if (bytesCompressed == 2)
      deadlineId.deadlineTwoCompressed
    else if (bytesCompressed == 3)
      deadlineId.deadlineThreeCompressed
    else if (bytesCompressed == 4)
      deadlineId.deadlineFourCompressed
    else if (bytesCompressed == 5)
      deadlineId.deadlineFiveCompressed
    else if (bytesCompressed == 6)
      deadlineId.deadlineSixCompressed
    else if (bytesCompressed == 7)
      deadlineId.deadlineSevenCompressed
    else if (bytesCompressed == 8)
      deadlineId.deadlineFullyCompressed
    else
      throw new Exception(s"Fatal exception: deadlineBytesCompressed = $bytesCompressed")

  private[writer] def uncompressed(currentDeadline: Deadline,
                                   deadlineId: DeadlineId,
                                   plusSize: Int,
                                   isKeyCompressed: Boolean,
                                   hasPrefixCompressed: Boolean,
                                   adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean) = {
    //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
    val currentDeadlineUnsignedBytes = currentDeadline.toBytes
    val deadline = deadlineId.deadlineUncompressed

    val id =
      if (adjustBaseIdToKeyValueId)
        binder.keyValueId.adjustBaseIdToKeyValueIdKey(
          baseId = deadline.baseId,
          isKeyCompressed = isKeyCompressed
        )
      else
        deadline.baseId

    val bytes =
      Slice.create[Byte](Bytes.sizeOfUnsignedInt(id) + currentDeadlineUnsignedBytes.size + plusSize)
        .addUnsignedInt(id)
        .addAll(currentDeadlineUnsignedBytes)

    (bytes, isKeyCompressed || hasPrefixCompressed)
  }

  private[writer] def compress(currentDeadline: Deadline,
                               previousDeadline: Deadline,
                               deadlineId: DeadlineId,
                               plusSize: Int,
                               isKeyCompressed: Boolean,
                               adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): Option[(Slice[Byte], Boolean)] =
    Bytes.compress(
      previous = previousDeadline.toBytes,
      next = currentDeadline.toBytes,
      minimumCommonBytes = 1
    ) map {
      case (deadlineCommonBytes, deadlineCompressedBytes) =>
        val deadline = applyDeadlineId(deadlineCommonBytes, deadlineId)

        val id =
          if (adjustBaseIdToKeyValueId)
            binder.keyValueId.adjustBaseIdToKeyValueIdKey(deadline.baseId, isKeyCompressed)
          else
            deadline.baseId

        val bytes =
          Slice.create[Byte](Bytes.sizeOfUnsignedInt(id) + deadlineCompressedBytes.size + plusSize)
            .addUnsignedInt(id)
            .addAll(deadlineCompressedBytes)

        (bytes, true)
    }

  private[writer] def noDeadline(deadlineId: DeadlineId,
                                 plusSize: Int,
                                 isKeyCompressed: Boolean,
                                 hasPrefixCompressed: Boolean,
                                 adjustBaseIdToKeyValueId: Boolean)(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean) = {
    //if current key-value has no deadline.
    val deadline = deadlineId.noDeadline

    val id =
      if (adjustBaseIdToKeyValueId)
        binder.keyValueId.adjustBaseIdToKeyValueIdKey(
          baseId = deadline.baseId,
          isKeyCompressed = isKeyCompressed
        )
      else
        deadline.baseId

    val bytes =
      Slice.create[Byte](Bytes.sizeOfUnsignedInt(id) + plusSize)
        .addUnsignedInt(id)

    (bytes, isKeyCompressed || hasPrefixCompressed)
  }
}
