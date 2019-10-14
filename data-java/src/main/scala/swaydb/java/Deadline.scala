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

package swaydb.java

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration.{Deadline => ScalaDeadline}

object Deadline {
  def apply(scalaDeadline: ScalaDeadline) =
    new Deadline(scalaDeadline)
}

class Deadline(asScala: ScalaDeadline) {

  def timeLeft =
    asScala.timeLeft.toJava

  def time =
    asScala.time.toJava

  def hasTimeLeft =
    asScala.hasTimeLeft()

  def isOverdue =
    asScala.isOverdue()
}
