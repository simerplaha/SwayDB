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
import swaydb.iterator.{DBIterator, From, SubMapIterator}
import swaydb.serializers.Serializer

import scala.util.Try

object RootMap {

  def apply[K, V](db: SwayDB,
                  mapKey: K)(implicit keySerializer: Serializer[K],
                             valueSerializer: Serializer[V],
                             ordering: Ordering[Slice[Byte]]): RootMap[K, V] = {
    implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)
    val map = new Map[MapKey[K], V](db)

    new RootMap[K, V](map, mapKey)
  }
}

/**
  * A RootMap can only create other [[SubMap]] and is not iterable. A RootMap
  * should contain a SubMap which is iterable.
  *
  */
class RootMap[K, V](map: Map[MapKey[K], V],
                    mapKey: K)(implicit keySerializer: Serializer[K],
                               mapKeySerializer: Serializer[MapKey[K]],
                               valueSerializer: Serializer[V],
                               ordering: Ordering[Slice[Byte]]) extends SubMapIterator[K, V](mapKey, includeSubMapsBoolean = true, dbIterator = DBIterator[MapKey[K], V](map.db, Some(From(MapKey.Start(mapKey), false, false, false, true)))) {

  def subMap(key: K, value: V): Try[SubMap[K, V]] =
    SubMap.subMap[K, V](map, mapKey, key, value)
}