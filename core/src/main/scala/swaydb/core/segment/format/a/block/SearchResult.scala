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

package swaydb.core.segment.format.a.block

import swaydb.{ErrorHandler, IO}

sealed trait SearchResult[+T] {
  def toOption: Option[T]
}
object SearchResult {

  def none[A]: SearchResult.None[A] =
    SearchResult.None(Option.empty[A])

  def noneIO[E: ErrorHandler, A] =
    IO.Right[E, SearchResult[A]](none)

  case class None[T](lower: Option[T]) extends SearchResult[T] {
    override val toOption: Option[T] = scala.None
  }

  case class Some[T](lower: Option[T], value: T) extends SearchResult[T] {
    override def toOption: Option[T] = scala.Some(value)
  }
}
