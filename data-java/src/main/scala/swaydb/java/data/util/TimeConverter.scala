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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.java.data.util

import java.time.Duration
import java.util.Optional

import swaydb.Expiration

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.{Deadline, FiniteDuration}

object TimeConverter {

  def toDuration(finiteDuration: FiniteDuration): java.time.Duration =
    finiteDuration.toJava

  def toDuration(finiteDuration: Option[FiniteDuration]): Optional[Duration] =
    finiteDuration match {
      case Some(value) =>
        Optional.of(value.toJava)

      case None =>
        Optional.empty()
    }

  def toExpiration(deadline: Deadline): Expiration =
    new Expiration(deadline)

  def toExpiration(scalaDeadline: Option[Deadline]): Optional[Expiration] =
    scalaDeadline match {
      case Some(deadline) =>
        Optional.of(new Expiration(deadline))

      case None =>
        Optional.empty()
    }
}
