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

package swaydb

import scala.concurrent.duration.{Deadline, FiniteDuration}

sealed trait Prepare[+K, +V, +F]

object Prepare {

  object Put {
    def apply[K, V](key: K, value: V) =
      new Put(key, value, None)

    def apply[K, V](key: K, value: V, expireAfter: FiniteDuration) =
      new Put(key, value, Some(expireAfter.fromNow))

    def apply[K, V](key: K, value: V, expireAt: Deadline) =
      new Put(key, value, Some(expireAt))
  }

  object Remove {
    def apply[K](key: K): Remove[K] =
      new Remove(key, None, None)

    def apply[K](from: K, to: K): Remove[K] =
      new Remove(from, Some(to), None)
  }

  object Expire {
    def apply[K](key: K, after: FiniteDuration): Remove[K] =
      new Remove(key, None, Some(after.fromNow))

    def apply[K](from: K, to: K, after: FiniteDuration): Remove[K] =
      new Remove(from, Some(to), Some(after.fromNow))

    def apply[K](key: K, at: Deadline): Remove[K] =
      new Remove(key, None, Some(at))

    def apply[K](from: K, to: K, at: Deadline): Remove[K] =
      new Remove(from, Some(to), Some(at))
  }

  object Update {
    def apply[K, V](key: K, value: V) =
      new Update(key, None, value)

    def apply[K, V](from: K, to: K, value: V) =
      new Update(from, Some(to), value)
  }

  object ApplyFunction {
    def apply[K, F](key: K, function: F) =
      new ApplyFunction(key, None, function)

    def apply[K, F](from: K, to: K, function: F) =
      new ApplyFunction(from, Some(to), function)
  }

  object Add {
    def apply[T](elem: T) =
      new Add(elem, None)

    def apply[T](elem: T, expireAfter: FiniteDuration) =
      new Add(elem, Some(expireAfter.fromNow))

    def apply[T](elem: T, expireAt: Deadline) =
      new Add(elem, Some(expireAt))
  }

  case class Put[K, V](key: K, value: V, deadline: Option[Deadline]) extends Prepare[K, V, Nothing]
  case class Remove[K](from: K, to: Option[K], deadline: Option[Deadline]) extends Prepare[K, Nothing, Nothing]
  case class Update[K, V](from: K, to: Option[K], value: V) extends Prepare[K, V, Nothing]
  case class ApplyFunction[K, F](from: K, to: Option[K], function: F) extends Prepare[K, Nothing, F]
  case class Add[T](elem: T, deadline: Option[Deadline]) extends Prepare[T, Nothing, Nothing]
}
