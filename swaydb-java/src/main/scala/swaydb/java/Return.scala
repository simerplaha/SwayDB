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

import java.time.Duration
import java.util.Optional

import swaydb.java.data.util.Java._

import scala.compat.java8.DurationConverters._

/**
 * Returns types for [[swaydb.java.PureFunction]]
 */
sealed trait Return[+V]
object Return {

  /**
   * Return types for [[swaydb.java.PureFunction]] used in [[swaydb.java.MapIO]]
   */
  sealed trait Map[V] extends Return[V]

  /**
   * Return types for [[swaydb.java.PureFunction]] used in [[swaydb.java.SetIO]]
   */
  sealed trait Set[V] extends Return[V]

  def nothing[V](): Nothing[V] =
    Nothing[V]()

  def remove[V](): Remove[V] =
    Remove[V]()

  def expire[V](after: Duration): Expire[V] =
    Expire[V](after)

  def update[V](value: V, expireAfter: Duration): Update[V] =
    Update[V](value, Optional.of(expireAfter))

  def update[V](value: V): Update[V] =
    Update[V](value, Optional.empty())

  final case class Nothing[V]() extends Map[V] with Set[V]
  final case class Remove[V]() extends Map[V] with Set[V]
  final case class Expire[V](expireAfter: Duration) extends Map[V] with Set[V]

  final case class Update[V](value: V, expireAfter: Optional[Duration]) extends Map[V]

  def toScalaMap[V](returnValue: Return.Map[V]): swaydb.Apply.Map[V] =
    returnValue match {
      case Nothing() =>
        swaydb.Apply.Nothing

      case Remove() =>
        swaydb.Apply.Remove

      case Expire(expireAfter) =>
        swaydb.Apply.Expire(expireAfter.toScala)

      case Update(value, expireAfter) =>
        new swaydb.Apply.Update(value, expireAfter.asScalaMap(_.toScala.fromNow))
    }

  def toScalaSet(returnValue: Return.Set[java.lang.Void]): swaydb.Apply.Set[scala.Nothing] =
    returnValue match {
      case Nothing() =>
        swaydb.Apply.Nothing

      case Remove() =>
        swaydb.Apply.Remove

      case Expire(expireAfter) =>
        swaydb.Apply.Expire(expireAfter.toScala)
    }
}
