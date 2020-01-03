/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.core.util

import java.util.concurrent.TimeUnit

import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

private[swaydb] object Times {

  implicit class OptionDeadlineImplicits(deadline: Option[Deadline]) {
    @inline final def toNanos: Long =
      deadline match {
        case Some(deadline) =>
          deadline.time.toNanos

        case None =>
          0L
      }
  }

  implicit class DeadlineImplicits(deadline: Deadline) {
    @inline final def toNanos: Long =
      deadline.time.toNanos

    @inline final def toUnsignedBytes: Slice[Byte] =
      Slice.writeUnsignedLong(toNanos)

    @inline final def toBytes: Slice[Byte] =
      Slice.writeLong(toNanos)
  }

  implicit class LongImplicits(deadline: Long) {

    @inline final def toDeadlineOption: Option[Deadline] =
      if (deadline <= 0L)
        None
      else
        Some(Deadline(deadline, TimeUnit.NANOSECONDS))
  }
}
