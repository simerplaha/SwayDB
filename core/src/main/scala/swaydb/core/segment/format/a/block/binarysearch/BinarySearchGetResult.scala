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

package swaydb.core.segment.format.a.block.binarysearch

import swaydb.IO
import swaydb.IO.ExceptionHandler

private[block] sealed trait BinarySearchGetResult[+T] {
  def toOption: Option[T]
  def toIO[E: ExceptionHandler]: IO[E, Option[T]]
}

private[block] object BinarySearchGetResult {

  val none: BinarySearchGetResult.None[Nothing] =
    BinarySearchGetResult.None(Option.empty[Nothing])

  val noneIO =
    IO.Right[Nothing, BinarySearchGetResult[Nothing]](none)(IO.ExceptionHandler.Nothing)

  case class None[T](lower: Option[T]) extends BinarySearchGetResult[T] {
    override val toOption: Option[T] = scala.None

    override def toIO[E: ExceptionHandler]: IO[E, Option[T]] =
      IO.none
  }

  case class Some[T](value: T) extends BinarySearchGetResult[T] {
    override def toOption: Option[T] = scala.Some(value)

    override def toIO[E: ExceptionHandler]: IO[E, Option[T]] =
      IO.Right(scala.Some(value))
  }
}
