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

package swaydb.data.util

private[swaydb] trait SomeOrNone[T, SOME <: T] {

  def none: T

  def isEmpty: Boolean

  def get: SOME

  def isDefined: Boolean =
    !isEmpty

  def flatMap[B <: T](f: SOME => B): T =
    if (isDefined)
      f(get)
    else
      none

  def map[B](noneType: B, operation: SOME => B): B =
    if (isDefined)
      operation(get)
    else
      noneType

  def foreach[B](f: SOME => B): Unit =
    if (isDefined)
      f(get)

  def getOrElse[B <: SOME](other: => B): SOME =
    if (isDefined)
      get
    else
      other

  def orElse[B <: T](other: => B): T =
    if (isDefined)
      get
    else
      other

  def exists(f: SOME => Boolean): Boolean =
    isDefined && f(get)

  def forall(f: SOME => Boolean): Boolean =
    isEmpty || f(get)
}
