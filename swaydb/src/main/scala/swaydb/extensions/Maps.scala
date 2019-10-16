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

package swaydb.extensions

import swaydb.Error.API.ExceptionHandler
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.extensions.stream.{MapKeysStream, MapStream}
import swaydb.serializers.Serializer
import swaydb.{Done, From, IO}

//@formatter:off
class Maps[K, V](map: swaydb.Map[Key[K], Option[V], Nothing, IO.ApiIO],
                 mapKey: Seq[K])(implicit keySerializer: Serializer[K],
                                 mapKeySerializer: Serializer[Key[K]],
                                 keyOrder: KeyOrder[Slice[Byte]],
                                 valueSerializerOption: Serializer[Option[V]],
                                 valueSerializer: Serializer[V]) extends MapStream[K, V](mapKey = mapKey,
                                                                                         mapsOnly = true,
                                                                                         map = map.copy[Key[K], Option[V], Nothing, IO.ApiIO](from = Some(From(Key.SubMapsStart(mapKey),
                                                                                                                                              orAfter = false,
                                                                                                                                              orBefore = false,
                                                                                                                                              before = false,
                                                                                                                                              after = true)))) {
//@formatter:on

  def getOrPut(key: K, value: V): IO.ApiIO[Map[K, V]] =
    get(key) flatMap {
      got =>
        got.map(IO.Right(_)) getOrElse put(key, value)
    }

  def put(key: K, value: V): IO.ApiIO[Map[K, V]] = {
    val subMapKey = mapKey :+ key
    Map.putMap[K, V](
      map = map,
      mapKey = subMapKey,
      value = Some(value)
    ) flatMap {
      batches =>
        map.commit(batches) map {
          _ =>
            Map[K, V](
              map = map,
              mapKey = subMapKey
            )
        }
    }
  }

  def updateValue(key: K, value: V): IO.ApiIO[Map[K, V]] = {
    val subMapKey = mapKey :+ key
    map.commit {
      Map.updateMapValue[K, V](
        mapKey = subMapKey,
        value = value
      )
    } map {
      _ =>
        Map[K, V](
          map = map,
          mapKey = subMapKey
        )
    }
  }

  def remove(key: K): IO.ApiIO[Done] =
    Map.removeMap(map, mapKey :+ key) flatMap map.commit

  def get(key: K): IO.ApiIO[Option[Map[K, V]]] = {
    contains(key) map {
      exists =>
        if (exists)
          Some(
            Map[K, V](
              map = map,
              mapKey = mapKey :+ key
            )
          )
        else
          None
    }
  }

  /**
   * Removes all key-values from the target Map.
   */
  def clear(key: K): IO.ApiIO[Done] =
    get(key) flatMap {
      case Some(map) =>
        map.clear()
      case None =>
        IO.doneIO
    }

  def contains(key: K): IO.ApiIO[Boolean] =
    map.contains(Key.MapStart(mapKey :+ key))

  override def size: IO.ApiIO[Int] =
    keys.size

  /**
   * Returns None if this map does not exist or returns the value.
   */
  def getValue(key: K): IO.ApiIO[Option[V]] =
    map.get(Key.MapStart(mapKey :+ key)).map(_.flatten)

  def keys: MapKeysStream[K] =
    MapKeysStream[K](
      mapKey = mapKey,
      mapsOnly = true,
      set =
        new swaydb.Set[Key[K], Nothing, IO.ApiIO](
          core = map.core,
          from = Some(From(Key.SubMapsStart(mapKey), orAfter = false, orBefore = false, before = false, after = true))
        )
    )
}