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

package swaydb.extension

import swaydb.Batch
import swaydb.data.accelerate.Level0Meter
import swaydb.data.slice.Slice
import swaydb.extension.iterator.{MapIterator, MapKeysIterator}
import swaydb.iterator._
import swaydb.serializers.Serializer
import scala.util.{Success, Try}
import swaydb.data.order.KeyOrder

class Maps[K, V](map: swaydb.Map[Key[K], Option[V]],
                 mapKey: Seq[K])(implicit keySerializer: Serializer[K],
                                 mapKeySerializer: Serializer[Key[K]],
                                 keyOrder: KeyOrder[Slice[Byte]],
                                 valueSerializerOption: Serializer[Option[V]],
                                 valueSerializer: Serializer[V]) extends MapIterator[K, V](mapKey, mapsOnly = true, dbIterator = DBIterator[Key[K], Option[V]](map.db, Some(From(Key.SubMapsStart(mapKey), orAfter = false, orBefore = false, before = false, after = true)))) {

  def getOrPut(key: K, value: V): Try[Map[K, V]] =
    get(key) flatMap {
      got =>
        got.map(Success(_)) getOrElse put(key, value)
    }

  def put(key: K, value: V): Try[Map[K, V]] = {
    val subMapKey = mapKey :+ key
    Map.putMap[K, V](
      map = map,
      mapKey = subMapKey,
      value = Some(value)
    ) flatMap {
      batches =>
        map.batch(batches) map {
          _ =>
            Map[K, V](
              map = map,
              mapKey = subMapKey
            )
        }
    }
  }

  def updateValue(key: K, value: V): Try[Map[K, V]] = {
    val subMapKey = mapKey :+ key
    map.batch {
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

  def remove(key: K): Try[Level0Meter] =
    map.batch(Map.removeMap(map, mapKey :+ key))

  def get(key: K): Try[Option[Map[K, V]]] = {
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
  def clear(key: K): Try[Level0Meter] =
    get(key) flatMap {
      case Some(map) =>
        map.clear()
      case None =>
        Success(map.db.level0Meter)
    }

  def contains(key: K): Try[Boolean] =
    map.contains(Key.MapStart(mapKey :+ key))

  /**
    * Returns None if this map does not exist or returns the value.
    */
  def getValue(key: K): Try[Option[V]] =
    map.get(Key.MapStart(mapKey :+ key)).map(_.flatten)

  def keys: MapKeysIterator[K] =
    MapKeysIterator[K](
      mapKey = mapKey,
      mapsOnly = true,
      keysIterator =
        DBKeysIterator[Key[K]](
          db = map.db,
          from = Some(From(Key.SubMapsStart(mapKey), orAfter = false, orBefore = false, before = false, after = true))
        )
    )
}
