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

import swaydb.data.accelerate.Level0Meter
import swaydb.data.map.MapKey
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer
import swaydb.{Map, SwayDB}

import scala.util.Try

private[swaydb] object Root {

  def apply[K, V](db: SwayDB)(implicit keySerializer: Serializer[K],
                              valueSerializer: Serializer[V],
                              ordering: Ordering[Slice[Byte]]): Root[K, V] = {
    implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)

    val map = new Map[MapKey[K], V](db)
    new Root[K, V](map)
  }
}

class Root[K, V](map: Map[MapKey[K], V])(implicit keySerializer: Serializer[K],
                                         mapKeySerializer: Serializer[MapKey[K]],
                                         valueSerializer: Serializer[V],
                                         ordering: Ordering[Slice[Byte]]) {

  def createMap(key: K, value: V): Try[SubMap[K, V]] = {
    val mapKey = Seq(key)
    map.batch {
      SubMap.putMap(
        map = map,
        mapKey = mapKey,
        value = value
      )
    } map {
      _ =>
        SubMap[K, V](
          db = map.db,
          mapKey = mapKey
        )
    }
  }

  def removeMap(key: K): Try[Level0Meter] =
    SubMap.removeMap(map, Seq(key))

  /**
    * Returns target value for the input key.
    */
  def getMap(key: K): Try[Option[SubMap[K, V]]] = {
    val mapKey = Seq(key)
    mapExists(key) map {
      exists =>
        if (exists)
          Some(SubMap[K, V](map.db, mapKey))
        else
          None
    }
  }

  def mapExists(key: K): Try[Boolean] =
    map.contains(MapKey.Start(Seq(key)))

  def getMapValue(key: K): Try[Option[V]] =
    map.get(MapKey.Start(Seq(key)))

  /**
    *
    * Returns the estimate keyValue count in the database.
    */
  def dbKeyValueCount: Try[Int] =
    map.db.keyValueCount

  private[swaydb] def innerMap() =
    map
}