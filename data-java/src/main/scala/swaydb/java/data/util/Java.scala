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

package swaydb.java.data.util

import java.util.Comparator
import java.util.concurrent.ExecutorService

import swaydb.java.data.slice.ByteSlice

import scala.compat.java8.DurationConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}

object Java {

  type JavaFunction[T, R] = java.util.function.Function[T, R]

  implicit class ExecutorServiceImplicit(service: ExecutorService) {
    @inline def asScala: ExecutionContext =
      ExecutionContext.fromExecutorService(service)
  }

  implicit class ComparatorImplicit[T](comparator: Comparator[T]) {
    @inline def asScala: Ordering[T] =
      Ordering.comparatorToOrdering(comparator)
  }

  implicit class ComparatorByteSliceImplicit(comparator: Comparator[ByteSlice]) {
    @inline def asScala: Ordering[ByteSlice] =
      Ordering.comparatorToOrdering(comparator)
  }

  implicit class TupleImplicits[K, V](tuple: (K, V)) {
    @inline def asKeyVal: KeyVal[K, V] =
      KeyVal(tuple._1, tuple._2)

    @inline def asPair: Pair[K, V] =
      Pair(tuple._1, tuple._2)
  }

  implicit class TupleDurationImplicits[K](tuple: (K, java.time.Duration)) {
    @inline def asScala: (K, FiniteDuration) =
      (tuple._1, tuple._2.toScala)

    @inline def asScalaDeadline: (K, Deadline) =
      (tuple._1, tuple._2.toScala.fromNow)
  }
}
