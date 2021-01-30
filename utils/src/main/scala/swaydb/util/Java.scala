/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.util

import java.util.concurrent.ExecutorService
import java.util.{Comparator, Optional}
import scala.compat.java8.DurationConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}

object Java {

  type JavaFunction[T, R] = java.util.function.Function[T, R]

  implicit class ExecutorServiceImplicit(service: ExecutorService) {
    @inline final def asScala: ExecutionContext =
      ExecutionContext.fromExecutorService(service)
  }

  implicit class ComparatorImplicit[T](comparator: Comparator[T]) {
    @inline final def asScala: Ordering[T] =
      Ordering.comparatorToOrdering(comparator)
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

  implicit class TupleDurationImplicits[K](tuple: (K, java.time.Duration)) {
    @inline final def asScala: (K, FiniteDuration) =
      (tuple._1, tuple._2.toScala)

    @inline final def asScalaDeadline: (K, Deadline) =
      (tuple._1, tuple._2.toScala.fromNow)
  }

}
