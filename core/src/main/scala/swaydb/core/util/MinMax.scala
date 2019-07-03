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

package swaydb.core.util

import swaydb.core.data.Value
import swaydb.data.slice.Slice

import scala.annotation.tailrec

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

      case (left @ Some(_), None) =>
        left

      case (None, right @ Some(_)) =>
        right

      case (None, None) =>
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

  def contains(key: Slice[Byte], minMax: MinMax)(implicit order: Ordering[Slice[Byte]]) = {
    import order._
    minMax.max map {
      max =>
        key >= minMax.min && key <= max
    } getOrElse key.equiv(minMax.min)
  }

  def minMax(left: Option[MinMax],
             right: Option[Value])(implicit order: Ordering[Slice[Byte]]): Option[MinMax] =
    right flatMap {
      right =>
        minMax(left, right)
    } orElse left

  def minMax(left: Option[MinMax],
             right: Value)(implicit order: Ordering[Slice[Byte]]): Option[MinMax] =
    right match {
      case _: Value.Remove | _: Value.Update | _: Value.Put =>
        left

      case Value.Function(function, _) =>
        Some(minMax(left, function))

      case Value.PendingApply(applies) =>
        minMaxWithValue(left, applies)
    }

  @tailrec
  def minMaxWithValue(left: Option[MinMax],
                      right: Slice[Value])(implicit order: Ordering[Slice[Byte]]): Option[MinMax] =
    right.headOption match {
      case Some(value) =>
        minMaxWithValue(minMax(left, value), right.dropHead())

      case None =>
        left
    }

  def getMinMax(left: Option[MinMax],
                right: Option[MinMax])(implicit order: Ordering[Slice[Byte]]): Option[MinMax] =
    (left, right) match {
      case (Some(left), Some(right)) =>
        Some(
          MinMax(
            min = order.min(left.min, right.min),
            max = MinMax.max(left.max, right.max)(order)
          )
        )

      case (None, right @ Some(_)) =>
        right

      case (left @ Some(_), None) =>
        left

      case (None, None) =>
        None
    }

  def minMax(currentMinMax: Option[MinMax],
             newMinMax: Slice[Byte])(implicit order: Ordering[Slice[Byte]]): MinMax =
    currentMinMax map {
      currentMinMax =>
        minMax(currentMinMax, newMinMax)
    } getOrElse MinMax(min = newMinMax, None)

  def minMax(currentMinMax: MinMax,
             functionId: Slice[Byte])(implicit order: Ordering[Slice[Byte]]): MinMax = {
    import order._
    if (functionId < currentMinMax.min)
      currentMinMax.copy(min = functionId)
    else if (currentMinMax.max.forall(_ < functionId))
      currentMinMax.copy(max = Some(functionId))
    else
      currentMinMax
  }
}

case class MinMax(min: Slice[Byte],
                  max: Option[Slice[Byte]])