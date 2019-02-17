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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.function

import scala.concurrent.duration.Deadline

sealed trait SwayFunction[-K, -V]
object SwayFunction {

  sealed trait Output[-V]
  object Output {
    case object Remove extends Output[Nothing]

    case class Expire(deadline: Deadline) extends Output[Nothing]

    object Update {
      def apply[V](value: V): Update[V] =
        new Update(value, None)

      def apply[V](value: V, deadline: Deadline): Update[V] =
        new Update(value, Some(deadline))
    }

    case class Update[V](value: V, deadline: Option[Deadline]) extends Output[V]
  }

  def key[K, V](f: K => Output[V]): SwayFunction[K, V] = SwayFunction.Key(f)
  def keyDeadline[K, V](f: (K, Option[Deadline]) => Output[V]): SwayFunction[K, V] = SwayFunction.KeyDeadline(f)
  def keyValue[K, V](f: (K, V) => Output[V]): SwayFunction[K, V] = SwayFunction.KeyValue(f)

  def keyValueDeadline[K, V](f: (K, V, Option[Deadline]) => Output[V]): SwayFunction[K, V] = SwayFunction.KeyValueDeadline(f)
  def value[V](f: V => Output[V]): SwayFunction[Nothing, V] = SwayFunction.Value(f)
  def valueDeadline[V](f: (V, Option[Deadline]) => Output[V]): SwayFunction[Nothing, V] = SwayFunction.ValueDeadline(f)

  private[swaydb] case class Key[K, V](key: K => Output[V]) extends SwayFunction[K, V]
  private[swaydb] case class KeyDeadline[K, V](key: (K, Option[Deadline]) => Output[V]) extends SwayFunction[K, V]
  private[swaydb] case class KeyValue[K, V](key: (K, V) => Output[V]) extends SwayFunction[K, V]

  private[swaydb] case class KeyValueDeadline[K, V](key: (K, V, Option[Deadline]) => Output[V]) extends SwayFunction[K, V]
  private[swaydb] case class Value[V](key: V => Output[V]) extends SwayFunction[Nothing, V]
  private[swaydb] case class ValueDeadline[V](key: (V, Option[Deadline]) => Output[V]) extends SwayFunction[Nothing, V]

}
