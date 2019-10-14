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

import java.util.Optional

import swaydb.{Apply => ScalaApply}

import scala.compat.java8.DurationConverters._
import scala.compat.java8.OptionConverters._

object Apply {

  def nothing[T](): ScalaApply.Map[T] with ScalaApply.Set[T] =
    ScalaApply.Nothing

  def remove[T](): ScalaApply.Map[T] with ScalaApply.Set[T] =
    ScalaApply.Remove

  def expire[T](after: java.time.Duration): ScalaApply.Map[T] with ScalaApply.Set[T] =
    ScalaApply.Expire(after.toScala.fromNow)

  def update[V](value: V, expire: Optional[java.time.Duration]): ScalaApply.Map[V] =
    ScalaApply.Update(value, expire.asScala.map(_.toScala.fromNow))

}
