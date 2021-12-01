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

package swaydb

import swaydb.serializers._
import swaydb.slice.{Slice, SliceOption}

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
        Prepare.ApplyFunction[Slice[Byte], Slice[Byte]](from, to, Slice.writeString(functionId))

      case Prepare.Update(from, to, value) =>
        Prepare.Update[Slice[Byte], SliceOption[Byte]](from, to, value)

      case Prepare.Add(key, deadline) =>
        Prepare.Add[Slice[Byte]](key, deadline)
    }

  @inline implicit def preparesToUntyped[K, V, F](prepare: IterableOnce[Prepare[K, V, F]])(implicit keySerializer: Serializer[K],
                                                                                           valueSerializer: Serializer[V]): IterableOnce[Prepare[Slice[Byte], SliceOption[Byte], Slice[Byte]]] =
    prepare.map(batch => prepareToUntyped(batch)(keySerializer, valueSerializer))
}
