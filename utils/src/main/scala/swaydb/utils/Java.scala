/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.utils

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
