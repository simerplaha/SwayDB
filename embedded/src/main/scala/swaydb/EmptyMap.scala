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

import swaydb.data.slice.Slice
import swaydb.data.map.MapKey
import swaydb.serializers.Serializer

import scala.util.{Success, Try}

object EmptyMap {

  def apply[K, V](db: SwayDB)(implicit keySerializer: Serializer[K],
                              valueSerializer: Serializer[V],
                              ordering: Ordering[Slice[Byte]]): EmptyMap[K, V] = {
    implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)

    val map = new Map[MapKey[K], V](db)
    new EmptyMap[K, V](map)
  }
}

/**
  * An immutable empty Map used as a the head map to create child [[RootMap]]s.
  */
class EmptyMap[K, V](map: Map[MapKey[K], V])(implicit keySerializer: Serializer[K],
                                             valueSerializer: Serializer[V],
                                             ordering: Ordering[Slice[Byte]]) {

  def rootMap(key: K, value: V): Try[RootMap[K, V]] =
    map.contains(MapKey.Start(key)) flatMap {
      exists =>
        if (exists) {
          implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)
          Success(new RootMap[K, V](map, key))
        } else {
          map.batch(
            Batch.Put(MapKey.Start(key), value),
            Batch.Put(MapKey.End(key), value)
          ) map {
            _ =>
              implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)
              new RootMap[K, V](map, key)
          }
        }
    }
}