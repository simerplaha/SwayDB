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

import swaydb.IO
import swaydb.core.data.KeyValue.ReadOnly
import swaydb.core.data.{KeyValue, Transient, Value}
import swaydb.core.function.FunctionStore
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

  def max[T](left: T,
             right: Option[T])(implicit ordering: Ordering[T]): T =
    right map {
      right =>
        maximum[T](left, right)
    } getOrElse left

  def max[T](left: Option[T],
             right: T)(implicit ordering: Ordering[T]): T =
    left map {
      left =>
        maximum[T](left, right)
    } getOrElse right

  def contains[T](key: T, minMax: MinMax[T])(implicit order: Ordering[T]): Boolean = {
    import order._
    minMax.max map {
      max =>
        key >= minMax.min && key <= max
    } getOrElse key.equiv(minMax.min)
  }

  def minMaxFunction(function: Option[Value],
                     current: Option[MinMax[Slice[Byte]]]): Option[MinMax[Slice[Byte]]] =
    function flatMap {
      function =>
        minMaxFunction(
          function = function,
          current = current
        )
    } orElse current

  def minMaxFunction(function: Value,
                     current: Option[MinMax[Slice[Byte]]]): Option[MinMax[Slice[Byte]]] =
    function match {
      case _: Value.Remove | _: Value.Update | _: Value.Put =>
        current

      case function: Value.Function =>
        Some(
          minMaxFunction(
            function = function,
            current = current
          )
        )

      case Value.PendingApply(applies) =>
        minMaxFunction(
          functions = applies,
          current = current
        )
    }

  def minMaxFunction(function: Value.Function,
                     current: Option[MinMax[Slice[Byte]]]): MinMax[Slice[Byte]] =
    minMax(
      current = current,
      next = function.function
    )(FunctionStore.order)

  def minMaxFunction(function: Transient.Function,
                     current: Option[MinMax[Slice[Byte]]]): MinMax[Slice[Byte]] =
    minMax(
      current = current,
      next = function.function
    )(FunctionStore.order)

  def minMaxFunction(function: ReadOnly.Function,
                     current: Option[MinMax[Slice[Byte]]]): IO[IO.Error, MinMax[Slice[Byte]]] =
    function.getOrFetchFunction map {
      function =>
        minMax(
          current = current,
          next = function
        )(FunctionStore.order)
    }

  def minMaxFunction(range: Transient.Range,
                     current: Option[MinMax[Slice[Byte]]]): Option[MinMax[Slice[Byte]]] =
    minMaxFunction(
      fromValue = range.fromValue,
      rangeValue = range.rangeValue,
      current = current
    )

  def minMaxFunction(fromValue: Option[Value.FromValue],
                     rangeValue: Value.RangeValue,
                     current: Option[MinMax[Slice[Byte]]]): Option[MinMax[Slice[Byte]]] =
    minMaxFunction(
      function = rangeValue,
      current =
        minMaxFunction(
          function = fromValue,
          current = current
        )
    )

  def minMaxFunction(range: KeyValue.ReadOnly.Range,
                     current: Option[MinMax[Slice[Byte]]]): IO[IO.Error, Option[MinMax[Slice[Byte]]]] =
    range.fetchFromAndRangeValue map {
      case (fromValue, rangeValue) =>
        minMaxFunction(
          fromValue = fromValue,
          rangeValue = rangeValue,
          current = current
        )
    }

  @tailrec
  def minMaxFunction(functions: Slice[Value],
                     current: Option[MinMax[Slice[Byte]]]): Option[MinMax[Slice[Byte]]] =
    functions.headOption match {
      case Some(function) =>
        minMaxFunction(
          functions = functions.dropHead(),
          current =
            minMaxFunction(
              function = function,
              current = current
            )
        )

      case None =>
        current
    }

  def minMax[T](left: Option[MinMax[T]],
                right: Option[MinMax[T]])(implicit order: Ordering[T]): Option[MinMax[T]] =
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

  def minMax[T](current: Option[MinMax[T]],
                next: T)(implicit order: Ordering[T]): MinMax[T] =
    current map {
      currentMinMax =>
        minMax(currentMinMax, next)
    } getOrElse MinMax(min = next, None)

  def minMax[T](current: MinMax[T],
                next: T)(implicit order: Ordering[T]): MinMax[T] = {
    import order._
    val minCompare = order.compare(next, current.min)
    if (minCompare == 0)
      current
    else if (minCompare < 0)
      MinMax(
        min = next,
        max = current.max orElse Some(current.min)
      )
    else if (current.max.forall(_ < next))
      current.copy(max = Some(next))
    else
      current
  }
}

case class MinMax[T](min: T,
                     max: Option[T])