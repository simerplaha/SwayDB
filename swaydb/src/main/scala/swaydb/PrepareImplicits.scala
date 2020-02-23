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

package swaydb

import swaydb.data.slice.{Slice, SliceOption}
import swaydb.serializers._

private[swaydb] object PrepareImplicits {

  implicit def prepareToUntyped[K, V, F, R <: Apply[V]](prepare: Prepare[K, V, F])(implicit keySerializer: Serializer[K],
                                                                                   valueSerializer: Serializer[V],
                                                                                   ev: F <:< swaydb.PureFunction[K, V, R]): Prepare[Slice[Byte], SliceOption[Byte], Slice[Byte]] =
    prepare match {
      case Prepare.Put(key, value, deadline) =>
        Prepare.Put[Slice[Byte], SliceOption[Byte]](key, value, deadline)

      case Prepare.Remove(from, to, deadline) =>
        Prepare.Remove[Slice[Byte]](from, to, deadline)

      case Prepare.ApplyFunction(from, to, function) =>
        Prepare.ApplyFunction[Slice[Byte], Slice[Byte]](from, to, Slice.writeString(function.id))

      case Prepare.Update(from, to, value) =>
        Prepare.Update[Slice[Byte], SliceOption[Byte]](from, to, value)

      case Prepare.Add(key, deadline) =>
        Prepare.Add[Slice[Byte]](key, deadline)
    }

  @inline implicit def preparesToUntyped[K, V, F, R <: Apply[V]](prepare: Iterable[Prepare[K, V, F]])(implicit keySerializer: Serializer[K],
                                                                                                      valueSerializer: Serializer[V],
                                                                                                      ev: F <:< swaydb.PureFunction[K, V, R]): Iterable[Prepare[Slice[Byte], SliceOption[Byte], Slice[Byte]]] =
    prepare.map(batch => prepareToUntyped(batch)(keySerializer, valueSerializer, ev))

  @inline implicit def preparesToUnTypes[T](prepare: Iterable[Prepare[T, Nothing, Nothing]])(implicit serializer: Serializer[T]): Iterable[Prepare[Slice[Byte], SliceOption[Byte], Slice[Byte]]] =
    prepare.map(batch => prepareToUntyped(batch))
}
