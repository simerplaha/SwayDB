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

import swaydb.IO
import swaydb.core.data.Transient
import swaydb.core.segment.format.a.entry.id.BaseEntryId.DeadlineId
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, TransientToKeyValueIdBinder}
import swaydb.core.util.Bytes
import swaydb.core.util.Options._
import swaydb.core.util.Times._
import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

private[writer] object DeadlineWriter {

  private[writer] def write(current: Transient,
                            deadlineId: DeadlineId,
                            enablePrefixCompression: Boolean,
                            plusSize: Int,
                            hasPrefixCompression: Boolean,
                            normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean, Option[Int]) = {
    val currentDeadline = current.deadline
    val previousDeadline = current.previous.flatMap(_.deadline)

    currentDeadline match {
      case Some(currentDeadline) =>
        when(enablePrefixCompression && !current.sortedIndexConfig.prefixCompressKeysOnly)(previousDeadline) flatMap {
          previousDeadline =>
            compress(
              current = current,
              currentDeadline = currentDeadline,
              previousDeadline = previousDeadline,
              deadlineId = deadlineId,
              plusSize = plusSize,
              normaliseToSize = normaliseToSize
            )
        } getOrElse {
          //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
          uncompressed(
            current = current,
            currentDeadline = currentDeadline,
            deadlineId = deadlineId,
            plusSize = plusSize,
            hasPrefixCompression = hasPrefixCompression,
            enablePrefixCompression = enablePrefixCompression,
            normaliseToSize = normaliseToSize
          )
        }

      case None =>
        noDeadline(
          current = current,
          deadlineId = deadlineId,
          plusSize = plusSize,
          hasPrefixCompression = hasPrefixCompression,
          enablePrefixCompression = enablePrefixCompression,
          normaliseToSize = normaliseToSize
        )
    }
  }

  private[writer] def applyDeadlineId(commonBytes: Int,
                                      deadlineId: DeadlineId): BaseEntryId.Deadline =
    if (commonBytes == 1)
      deadlineId.deadlineOneCompressed
    else if (commonBytes == 2)
      deadlineId.deadlineTwoCompressed
    else if (commonBytes == 3)
      deadlineId.deadlineThreeCompressed
    else if (commonBytes == 4)
      deadlineId.deadlineFourCompressed
    else if (commonBytes == 5)
      deadlineId.deadlineFiveCompressed
    else if (commonBytes == 6)
      deadlineId.deadlineSixCompressed
    else if (commonBytes == 7)
      deadlineId.deadlineSevenCompressed
    else if (commonBytes == 8)
      deadlineId.deadlineFullyCompressed
    else
      throw IO.throwable(s"Fatal exception: commonBytes = $commonBytes, deadlineId: ${deadlineId.getClass.getName}")

  private[writer] def uncompressed(current: Transient,
                                   currentDeadline: Deadline,
                                   deadlineId: DeadlineId,
                                   plusSize: Int,
                                   hasPrefixCompression: Boolean,
                                   enablePrefixCompression: Boolean,
                                   normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean, Option[Int]) = {
    //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
    val deadlineLong = currentDeadline.toNanos
    val deadline = deadlineId.deadlineUncompressed

    val (bytes, compressed, accessPosition) =
      KeyWriter.write(
        current = current,
        plusSize = Bytes.sizeOfUnsignedLong(deadlineLong) + plusSize,
        deadlineId = deadline,
        enablePrefixCompression = enablePrefixCompression,
        hasPrefixCompression = hasPrefixCompression,
        normaliseToSize = normaliseToSize
      )

    bytes addUnsignedLong deadlineLong

    (bytes, compressed, accessPosition)
  }

  private[writer] def compress(current: Transient,
                               currentDeadline: Deadline,
                               previousDeadline: Deadline,
                               deadlineId: DeadlineId,
                               plusSize: Int,
                               normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]): Option[(Slice[Byte], Boolean, Option[Int])] =
    Bytes.compress(
      previous = previousDeadline.toBytes,
      next = currentDeadline.toBytes,
      minimumCommonBytes = 1
    ) map {
      case (deadlineCommonBytes, deadlineCompressedBytes) =>
        val deadline = applyDeadlineId(deadlineCommonBytes, deadlineId)

        val (bytes, _, accessPosition) =
          KeyWriter.write(
            current = current,
            plusSize = deadlineCompressedBytes.size + plusSize,
            deadlineId = deadline,
            enablePrefixCompression = true,
            hasPrefixCompression = true,
            normaliseToSize = normaliseToSize
          )

        bytes addAll deadlineCompressedBytes

        (bytes, true, accessPosition)
    }

  private[writer] def noDeadline(current: Transient,
                                 deadlineId: DeadlineId,
                                 plusSize: Int,
                                 hasPrefixCompression: Boolean,
                                 enablePrefixCompression: Boolean,
                                 normaliseToSize: Option[Int])(implicit binder: TransientToKeyValueIdBinder[_]): (Slice[Byte], Boolean, Option[Int]) =
    KeyWriter.write(
      current = current,
      plusSize = plusSize,
      deadlineId = deadlineId.noDeadline,
      enablePrefixCompression = enablePrefixCompression,
      hasPrefixCompression = hasPrefixCompression,
      normaliseToSize = normaliseToSize
    )
}
