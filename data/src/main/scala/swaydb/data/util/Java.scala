/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import java.util.concurrent.ExecutorService
import java.util.{Comparator, Optional}

import swaydb.{KeyVal, Pair}

import scala.compat.java8.DurationConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}

object Java {

  type JavaFunction[T, R] = java.util.function.Function[T, R]
  type ScalaSlice[T] = swaydb.data.slice.Slice[T]

  implicit class ExecutorServiceImplicit(service: ExecutorService) {
    @inline final def asScala: ExecutionContext =
      ExecutionContext.fromExecutorService(service)
  }

  implicit class ComparatorImplicit[T](comparator: Comparator[T]) {
    @inline final def asScala: Ordering[T] =
      Ordering.comparatorToOrdering(comparator)
  }

  implicit class TupleImplicits[K, V](tuple: (K, V)) {
    @inline final def asKeyVal: KeyVal[K, V] =
      KeyVal(tuple._1, tuple._2)

    @inline final def asPair: Pair[K, V] =
      Pair(tuple._1, tuple._2)
  }

  implicit class TupleDurationImplicits[K](tuple: (K, java.time.Duration)) {
    @inline final def asScala: (K, FiniteDuration) =
      (tuple._1, tuple._2.toScala)

    @inline final def asScalaDeadline: (K, Deadline) =
      (tuple._1, tuple._2.toScala.fromNow)
  }

  implicit class PairDurationImplicits[K](pair: Pair[K, java.time.Duration]) {
    @inline final def asScala: (K, FiniteDuration) =
      (pair.left, pair.right.toScala)

    @inline final def asScalaDeadline: (K, Deadline) =
      (pair.left, pair.right.toScala.fromNow)
  }


  implicit class OptionalConverter[T](optional: Optional[T]) {
    @inline final def asScala: Option[T] =
      if (optional.isPresent)
        Some(optional.get())

      else
        None

    @inline final def asScalaMap[B](map: T => B): Option[B] =
      if (optional.isPresent)
        Some(map(optional.get()))
      else
        None
  }

  implicit class OptionConverter[T](option: Option[T]) {
    @inline final def asJava: Optional[T] =
      option match {
        case Some(value) =>
          Optional.of(value)

        case None =>
          Optional.empty()
      }

    @inline final def asJavaMap[B](map: T => B): Optional[B] =
      option match {
        case Some(value) =>
          Optional.of(map(value))

        case None =>
          Optional.empty()
      }
  }
}
