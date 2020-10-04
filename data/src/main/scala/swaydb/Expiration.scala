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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb

import java.time.Duration
import java.util.Optional

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.Deadline

object Expiration {
  def of(scalaDeadline: Deadline) =
    new Expiration(scalaDeadline)

  @inline def apply(scalaDeadline: Option[Deadline]): Optional[Expiration] =
    of(scalaDeadline)

  def of(scalaDeadline: Option[Deadline]): Optional[Expiration] =
    scalaDeadline match {
      case Some(deadline) =>
        Optional.of(new Expiration(deadline))

      case None =>
        Optional.empty()
    }
}

case class Expiration(asScala: Deadline) {

  def timeLeft: Duration =
    asScala.timeLeft.toJava

  def time: Duration =
    asScala.time.toJava

  def plus(time: Duration): Expiration =
    Expiration(asScala + time.toScala)

  def minus(time: Duration): Expiration =
    Expiration(asScala - time.toScala)

  def compare(expiration: Expiration): Int =
    asScala.compare(expiration.asScala)

  def hasTimeLeft: Boolean =
    asScala.hasTimeLeft()

  def isOverdue: Boolean =
    asScala.isOverdue()
}
