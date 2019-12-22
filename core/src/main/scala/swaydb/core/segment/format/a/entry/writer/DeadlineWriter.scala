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
import swaydb.core.data.Memory
import swaydb.core.segment.format.a.entry.id.BaseEntryId.DeadlineId
import swaydb.core.segment.format.a.entry.id.{BaseEntryId, MemoryToKeyValueIdBinder}
import swaydb.core.util.Bytes
import swaydb.core.util.Options._
import swaydb.core.util.Times._

import scala.concurrent.duration.Deadline

private[a] trait DeadlineWriter {
  private[a] def write[T <: Memory](current: T,
                                    builder: EntryWriter.Builder,
                                    deadlineId: DeadlineId)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                            keyWriter: KeyWriter): Unit
}

private[a] object DeadlineWriter extends DeadlineWriter {

  private[a] def write[T <: Memory](current: T,
                                    builder: EntryWriter.Builder,
                                    deadlineId: DeadlineId)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                            keyWriter: KeyWriter): Unit =
    current.deadline match {
      case Some(currentDeadline) =>
        when(builder.enablePrefixCompression && !builder.prefixCompressKeysOnly)(builder.previous.flatMapOption(_.deadline)) flatMap {
          previousDeadline =>
            compress(
              current = current,
              currentDeadline = currentDeadline,
              previousDeadline = previousDeadline,
              deadlineId = deadlineId,
              builder = builder
            )
        } getOrElse {
          //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
          uncompressed(
            current = current,
            currentDeadline = currentDeadline,
            deadlineId = deadlineId,
            builder = builder
          )
        }

      case None =>
        noDeadline(
          current = current,
          deadlineId = deadlineId,
          builder = builder
        )
    }

  private[a] def applyDeadlineId(commonBytes: Int,
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

  private[a] def uncompressed[T <: Memory](current: T,
                                           currentDeadline: Deadline,
                                           deadlineId: DeadlineId,
                                           builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                         keyWriter: KeyWriter): Unit = {
    //if previous deadline bytes do not exist or minimum compression was not met then write uncompressed deadline.
    val deadlineLong = currentDeadline.toNanos
    val deadline = deadlineId.deadlineUncompressed

    keyWriter.write(
      current = current,
      builder = builder,
      deadlineId = deadline
    )

    builder.bytes addUnsignedLong deadlineLong
  }

  private[a] def compress[T <: Memory](current: T,
                                       currentDeadline: Deadline,
                                       previousDeadline: Deadline,
                                       deadlineId: DeadlineId,
                                       builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                     keyWriter: KeyWriter): Option[Unit] =
    Bytes.compress(
      previous = previousDeadline.toBytes,
      next = currentDeadline.toBytes,
      minimumCommonBytes = 1
    ) map {
      case (deadlineCommonBytes, deadlineCompressedBytes) =>
        val deadline = applyDeadlineId(deadlineCommonBytes, deadlineId)

        builder.setSegmentHasPrefixCompression()

        keyWriter.write(
          current = current,
          builder = builder,
          deadlineId = deadline
        )

        builder.bytes addAll deadlineCompressedBytes
    }

  private[a] def noDeadline[T <: Memory](current: T,
                                         deadlineId: DeadlineId,
                                         builder: EntryWriter.Builder)(implicit binder: MemoryToKeyValueIdBinder[T],
                                                                       keyWriter: KeyWriter): Unit =
    keyWriter.write(
      current = current,
      builder = builder,
      deadlineId = deadlineId.noDeadline
    )
}
