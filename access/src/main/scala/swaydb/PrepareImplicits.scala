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

package swaydb

import swaydb.data.slice.Slice
import swaydb.serializers._

private[swaydb] object PrepareImplicits {

  implicit def prepareToUntyped[K, V](prepare: Prepare[K, V])(implicit keySerializer: Serializer[K],
                                                              valueSerializer: Serializer[V]): Prepare[Slice[Byte], Option[Slice[Byte]]] =
    prepare match {
      case Prepare.Put(key, value, deadline) =>
        Prepare.Put[Slice[Byte], Option[Slice[Byte]]](key, Some(value), deadline)

      case Prepare.Remove(from, to, deadline) =>
        Prepare.Remove[Slice[Byte]](from, to, deadline)

      case Prepare.Function(from, to, function) =>
        Prepare.Function[Slice[Byte]](from, to, function)

      case Prepare.Update(from, to, value) =>
        Prepare.Update[Slice[Byte], Option[Slice[Byte]]](from, to, Some(value))

      case Prepare.Add(key, deadline) =>
        Prepare.Add[Slice[Byte]](key, deadline)
    }

  implicit def preparesToUntyped[K, V](prepare: Iterable[Prepare[K, V]])(implicit keySerializer: Serializer[K],
                                                                         valueSerializer: Serializer[V]): Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]] =
    prepare.map(batch => prepareToUntyped(batch)(keySerializer, valueSerializer))

  implicit def preparesToUnTypes[T](prepare: Iterable[Prepare[T, Nothing]])(implicit serializer: Serializer[T]): Iterable[Prepare[Slice[Byte], Option[Slice[Byte]]]] =
    prepare.map(batch => prepareToUntyped(batch)(serializer, Default.UnitSerializer))
}
