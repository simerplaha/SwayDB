/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.segment.format.one.entry.writer

import swaydb.core.data.KeyValue
import swaydb.core.segment.format.one.entry.id.EntryId
import swaydb.core.segment.format.one.entry.id.EntryId.GetDeadlineId
import swaydb.core.util.Bytes._
import swaydb.data.slice.Slice
import swaydb.core.util.TimeUtil._

object DeadlineWriter {

  private def applyDeadlineId(bytesCompressed: Int,
                              getDeadlineId: GetDeadlineId): EntryId.Deadline =
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

  def write(current: KeyValue.WriteOnly,
            getDeadlineId: GetDeadlineId,
            plusSize: Int): Slice[Byte] =
    current.deadline map {
      currentDeadline =>
        //fetch the previous deadline bytes
        current.previous.flatMap(_.deadline) flatMap {
          previousDeadline =>
            val currentDeadlineBytes = currentDeadline.toBytes
            val previousDeadlineBytes = previousDeadline.toBytes
            compress(previous = previousDeadlineBytes, next = currentDeadlineBytes, minimumCommonBytes = 1) map {
              case (deadlineCommonBytes, deadlineUncompressedBytes) =>
                val deadlineId = applyDeadlineId(deadlineCommonBytes, getDeadlineId)
                Slice.create[Byte](sizeOf(deadlineId.id) + deadlineUncompressedBytes.size + plusSize)
                  .addIntUnsigned(deadlineId.id)
                  .addAll(deadlineUncompressedBytes)
            }
        } getOrElse {
          //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
          val currentDeadlineUnsignedBytes = currentDeadline.toLongUnsignedBytes
          Slice.create[Byte](sizeOf(getDeadlineId.deadlineUncompressed.id) + currentDeadlineUnsignedBytes.size + plusSize)
            .addIntUnsigned(getDeadlineId.deadlineUncompressed.id)
            .addAll(currentDeadlineUnsignedBytes)
        }
    } getOrElse {
      //if current key-value has no deadline.
      Slice.create[Byte](sizeOf(getDeadlineId.noDeadline.id) + plusSize)
        .addIntUnsigned(getDeadlineId.noDeadline.id)
    }
}