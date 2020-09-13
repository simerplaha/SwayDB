/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
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

    @inline def earlier(other: Deadline): Deadline =
      other.earlier(deadline)

    def earlier(other: Option[Deadline]): Option[Deadline] =
      (deadline, other) match {
        case (left @ Some(leftExpiration), right @ Some(rightExpiration)) =>
          if (leftExpiration.timeLeft <= rightExpiration.timeLeft)
            left
          else
            right

        case (some @ Some(_), None) =>
          some

        case (None, right @ Some(_)) =>
          right

        case (None, None) =>
          None
      }


  }

  implicit class DeadlineImplicits(deadline: Deadline) {
    @inline final def toNanos: Long =
      deadline.time.toNanos

    @inline final def toUnsignedBytes: Slice[Byte] =
      Slice.writeUnsignedLong[Byte](toNanos)

    @inline final def toBytes: Slice[Byte] =
      Slice.writeLong[Byte](toNanos)

    @inline def earlier(other: Deadline): Deadline =
      if (deadline.timeLeft <= other.timeLeft)
        deadline
      else
        other

    @inline def earlier(other: Option[Deadline]): Deadline =
      other match {
        case Some(other) =>
          other.earlier(deadline)

        case None =>
          deadline
      }
  }

  implicit class LongImplicits(deadline: Long) {

    @inline final def toDeadlineOption: Option[Deadline] =
      if (deadline <= 0L)
        None
      else
        Some(Deadline((deadline, TimeUnit.NANOSECONDS)))
  }

}
