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

package swaydb

import scala.concurrent.duration.Deadline

/**
  * Output of functions
  */
sealed trait Apply[+V]
object Apply {

  case object Nothing extends Apply[Nothing]
  case object Remove extends Apply[Nothing]
  case class Expire(deadline: Deadline) extends Apply[Nothing]

  object Update {
    def apply[V](value: V): Update[V] =
      new Update(value, None)

    def apply[V](value: V, deadline: Deadline): Update[V] =
      new Update(value, Some(deadline))
  }

  case class Update[V](value: V, deadline: Option[Deadline]) extends Apply[V]
}