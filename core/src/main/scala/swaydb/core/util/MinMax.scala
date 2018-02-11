/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.core.util

private[core] object MinMax {

  private def minimum[T](left: T,
                         right: T)(implicit ordering: Ordering[T]): T =
    if (ordering.compare(left, right) <= 0)
      left
    else
      right

  private def maximum[T](left: T,
                         right: T)(implicit ordering: Ordering[T]): T =
    if (ordering.compare(left, right) >= 0)
      left
    else
      right

  private def pickOne[T](key1: Option[T],
                         key2: Option[T],
                         condition: (T, T) => T): Option[T] =
    (key1, key2) match {
      case (Some(key1), Some(key2)) =>
        Some(condition(key1, key2))

      case (left @ Some(key1), None) =>
        left
      case (None, right @ Some(key2)) =>
        right
      case _ =>
        None
    }

  /**
    * Picks the smallest of the two. Favours left if equal.
    */
  def min[T](left: Option[T],
             right: Option[T])(implicit ordering: Ordering[T]): Option[T] =
    pickOne[T](left, right, minimum)

  /**
    * Picks the largest of the two. Favours left if equal.
    */
  def max[T](left: Option[T],
             right: Option[T])(implicit ordering: Ordering[T]): Option[T] =
    pickOne[T](left, right, maximum)
}