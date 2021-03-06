/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb

import swaydb.data.slice.{Slice, SliceOption}
import swaydb.serializers._

import scala.collection.compat.IterableOnce

private[swaydb] object PrepareImplicits {

  implicit def prepareToUntyped[K, V, F](prepare: Prepare[K, V, F])(implicit keySerializer: Serializer[K],
                                                                    valueSerializer: Serializer[V]): Prepare[Slice[Byte], SliceOption[Byte], Slice[Byte]] =
    prepare match {
      case Prepare.Put(key, value, deadline) =>
        Prepare.Put[Slice[Byte], SliceOption[Byte]](key, value, deadline)

      case Prepare.Remove(from, to, deadline) =>
        Prepare.Remove[Slice[Byte]](from, to, deadline)

      case Prepare.ApplyFunction(from, to, function) =>
        val functionId = function.asInstanceOf[PureFunction[_, _, _]].id
        Prepare.ApplyFunction[Slice[Byte], Slice[Byte]](from, to, Slice.writeString[Byte](functionId))

      case Prepare.Update(from, to, value) =>
        Prepare.Update[Slice[Byte], SliceOption[Byte]](from, to, value)

      case Prepare.Add(key, deadline) =>
        Prepare.Add[Slice[Byte]](key, deadline)
    }

  @inline implicit def preparesToUntyped[K, V, F](prepare: IterableOnce[Prepare[K, V, F]])(implicit keySerializer: Serializer[K],
                                                                                           valueSerializer: Serializer[V]): IterableOnce[Prepare[Slice[Byte], SliceOption[Byte], Slice[Byte]]] =
    prepare.map(batch => prepareToUntyped(batch)(keySerializer, valueSerializer))
}
