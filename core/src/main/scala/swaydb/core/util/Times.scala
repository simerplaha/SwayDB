/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.util

import swaydb.slice.Slice

import java.util.concurrent.TimeUnit
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
      Slice.writeUnsignedLong(toNanos)

    @inline final def toBytes: Slice[Byte] =
      Slice.writeLong(toNanos)

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
