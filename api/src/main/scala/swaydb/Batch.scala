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

package swaydb

import swaydb.types._

import scala.concurrent.duration.{Deadline, FiniteDuration}

sealed trait Batch[K, +V]

object Batch {

  /**
    * Batch Put key & value for a [[SwayDBMap]]
    */
  object Put {
    def apply[K, V](key: K, value: V) =
      new Put(key, value, None)

    def apply[K, V](key: K, value: V, expireAfter: FiniteDuration) =
      new Put(key, value, Some(expireAfter.fromNow))

    def apply[K, V](key: K, value: V, expireAt: Deadline) =
      new Put(key, value, Some(expireAt))
  }

  private[swaydb] case class Put[K, V](key: K, value: V, deadline: Option[Deadline]) extends Batch[K, V]

  /**
    * Batch remove for [[SwayDBMap]] & [[SwayDBSet]]
    */
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

  private[swaydb] case class Remove[K](from: K, to: Option[K], deadline: Option[Deadline]) extends Batch[K, Nothing]

  /**
    * Batch Update key & value for a [[SwayDBMap]]
    */
  object Update {
    def apply[K, V](key: K, value: V) =
      new Update(key, None, value)

    def apply[K, V](from: K, to: K, value: V) =
      new Update(from, Some(to), value)
  }

  private[swaydb] case class Update[K, V](from: K, to: Option[K], value: V) extends Batch[K, V]

  /**
    * Batch Put key & value for a [[SwayDBSet]]
    */
  object Add {
    def apply[T](elem: T) =
      new Add(elem, None)

    def apply[T](elem: T, expireAfter: FiniteDuration) =
      new Add(elem, Some(expireAfter.fromNow))

    def apply[T](elem: T, expireAt: Deadline) =
      new Add(elem, Some(expireAt))
  }
  case class Add[T](elem: T, deadline: Option[Deadline]) extends Batch[T, Nothing]

}