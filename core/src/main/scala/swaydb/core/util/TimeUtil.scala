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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.util

import java.util.concurrent.TimeUnit

import swaydb.data.slice.Slice

import scala.concurrent.duration.Deadline

object TimeUtil {

  implicit class OptionDeadlineImplicits(deadline: Option[Deadline]) {
    def toNanos: Long =
      deadline.map(_.time.toNanos).getOrElse(0L)
  }

  implicit class DeadlineImplicits(deadline: Deadline) {
    def toNanos: Long =
      deadline.time.toNanos

    def toLongUnsignedBytes: Slice[Byte] =
      Slice.writeLongUnsigned(toNanos)

    def toBytes: Slice[Byte] =
      Slice.writeLong(toNanos)
  }

  implicit class LongImplicits(deadline: Long) {

    def toDeadlineOption: Option[Deadline] =
      if (deadline <= 0L)
        None
      else
        Some(Deadline(deadline, TimeUnit.NANOSECONDS))
  }

}
