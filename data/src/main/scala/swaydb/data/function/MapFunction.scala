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

sealed trait MapFunction[K, V]
object MapFunction {

  sealed trait Output[V]
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

  def key[K, V](f: K => Output[V]): MapFunction[K, V] = MapFunction.Key(f)
  def keyDeadline[K, V](f: (K, Option[Deadline]) => Output[V]): MapFunction[K, V] = MapFunction.KeyDeadline(f)
  def keyValue[K, V](f: (K, V) => Output[V]): MapFunction[K, V] = MapFunction.KeyValue(f)

  def keyValueDeadline[K, V](f: (K, V, Option[Deadline]) => Output[V]): MapFunction[K, V] = MapFunction.KeyValueDeadline(f)
  def value[V](f: V => Output[V]): MapFunction[Nothing, V] = MapFunction.Value(f)
  def valueDeadline[V](f: (V, Option[Deadline]) => Output[V]): MapFunction[Nothing, V] = MapFunction.ValueDeadline(f)

  private[swaydb] case class Key[K, V](f: K => Output[V]) extends MapFunction[K, V]
  private[swaydb] case class KeyDeadline[K, V](f: (K, Option[Deadline]) => Output[V]) extends MapFunction[K, V]
  private[swaydb] case class KeyValue[K, V](f: (K, V) => Output[V]) extends MapFunction[K, V]

  private[swaydb] case class KeyValueDeadline[K, V](f: (K, V, Option[Deadline]) => Output[V]) extends MapFunction[K, V]
  private[swaydb] case class Value[V](f: V => Output[V]) extends MapFunction[Nothing, V]
  private[swaydb] case class ValueDeadline[V](f: (V, Option[Deadline]) => Output[V]) extends MapFunction[Nothing, V]

}
