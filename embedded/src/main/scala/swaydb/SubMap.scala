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

import swaydb.core.util.TryUtil
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.data.map.MapKey
import swaydb.iterator._
import swaydb.serializers.{Serializer, _}

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Failure, Success, Try}

object SubMap {
  def apply[K, V](db: SwayDB,
                  mapKey: K)(implicit keySerializer: Serializer[K],
                             valueSerializer: Serializer[V],
                             ordering: Ordering[Slice[Byte]]): SubMap[K, V] = {
    implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)
    val map = Map[MapKey[K], V](db)
    new SubMap[K, V](map, mapKey)
  }

  def putSubMap[K, V](parentMap: Map[MapKey[K], V],
                      parentMapKey: K,
                      mapKey: K,
                      value: V)(implicit keySerializer: Serializer[K],
                                valueSerializer: Serializer[V],
                                ordering: Ordering[Slice[Byte]]): Try[SubMap[K, V]] =
    parentMap.batch(
      Batch.Put(MapKey.SubMap(parentMapKey, mapKey), value), //add subMap entry to parent Map's key
      Batch.Put(MapKey.Start(mapKey), value),
      Batch.Put(MapKey.EntriesStart(mapKey), value),
      Batch.Put(MapKey.EntriesEnd(mapKey), value),
      Batch.Put(MapKey.SubMapsStart(mapKey), value),
      Batch.Put(MapKey.SubMapsEnd(mapKey), value),
      Batch.Put(MapKey.End(mapKey), value)
    ) map {
      _ =>
        implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)
        SubMap[K, V](parentMap.db, mapKey)
    }
}

/**
  * Key-value or Map database API.
  *
  * For documentation check - http://swaydb.io/api/
  */
class SubMap[K, V](map: Map[MapKey[K], V],
                   mapKey: K)(implicit keySerializer: Serializer[K],
                              mapKeySerializer: Serializer[MapKey[K]],
                              ordering: Ordering[Slice[Byte]],
                              valueSerializer: Serializer[V]) extends SubMapIterator[K, V](mapKey, dbIterator = DBIterator[MapKey[K], V](map.db, Some(From(MapKey.Start(mapKey), false, false, false, true)))) {

  def putSubMap(key: K, value: V): Try[SubMap[K, V]] =
    SubMap.putSubMap[K, V](map, mapKey, key, value)

  def getSubMap(key: K): Try[Option[SubMap[K, V]]] =
    map.contains(MapKey.Start(key)) map {
      exists =>
        if (exists)
          Some(SubMap[K, V](map.db, key))
        else
          None
    }

  def put(key: K, value: V): Try[Level0Meter] =
    map.put(key = MapKey.Entry(mapKey, key), value = value)

  def put(key: K, value: V, expireAfter: FiniteDuration): Try[Level0Meter] =
    map.put(MapKey.Entry(mapKey, key), value, expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): Try[Level0Meter] =
    map.put(MapKey.Entry(mapKey, key), value, expireAt)

  def remove(key: K): Try[Level0Meter] =
    map.remove(MapKey.Entry(mapKey, key))

  def remove(from: K, to: K): Try[Level0Meter] =
    map.remove(MapKey.Entry(mapKey, from), MapKey.Entry(mapKey, to))

  def expire(key: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(MapKey.Entry(mapKey, key), after.fromNow)

  def expire(key: K, at: Deadline): Try[Level0Meter] =
    map.expire(MapKey.Entry(mapKey, key), at)

  def expire(from: K, to: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(MapKey.Entry(mapKey, from), MapKey.Entry(mapKey, to), after.fromNow)

  def expire(from: K, to: K, at: Deadline): Try[Level0Meter] =
    map.expire(MapKey.Entry(mapKey, from), MapKey.Entry(mapKey, to), at)

  def update(key: K, value: V): Try[Level0Meter] =
    map.update(MapKey.Entry(mapKey, key), value)

  def update(from: K, to: K, value: V): Try[Level0Meter] =
    map.update(MapKey.Entry(mapKey, from), MapKey.Entry(mapKey, to), value)

  def batch(batch: Batch[K, V]*): Try[Level0Meter] =
    this.batch(batch)

  def batch(batch: Iterable[Batch[K, V]]): Try[Level0Meter] =
    map.batch(
      batch map {
        case data @ Data.Put(_, _, _) =>
          data.copy(MapKey.Entry(mapKey, data.key))
        case data @ Data.Remove(_, to, _) =>
          data.copy(from = MapKey.Entry(mapKey, data.from), to = to.map(MapKey.Entry(mapKey, _)))
        case data @ Data.Update(_, to, _) =>
          data.copy(from = MapKey.Entry(mapKey, data.from), to = to.map(MapKey.Entry(mapKey, _)))
        case data @ Data.Add(_, _) =>
          data.copy(elem = MapKey.Entry(mapKey, data.elem))
      }
    )

  def batchPut(keyValues: (K, V)*): Try[Level0Meter] =
    batchPut(keyValues)

  def batchPut(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchPut {
      keyValues map {
        case (key, value) =>
          (MapKey.Entry(mapKey, key), value)
      }
    }

  def batchUpdate(keyValues: (K, V)*): Try[Level0Meter] =
    batchUpdate(keyValues)

  def batchUpdate(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchUpdate {
      keyValues map {
        case (key, value) =>
          (MapKey.Entry(mapKey, key), value)
      }
    }

  def batchRemove(keys: K*): Try[Level0Meter] =
    batchRemove(keys)

  def batchRemove(keys: Iterable[K]): Try[Level0Meter] =
    map.batchRemove(keys.map(key => MapKey.Entry(mapKey, key)))

  def batchExpire(keys: (K, Deadline)*): Try[Level0Meter] =
    batchExpire(keys)

  def batchExpire(keys: Iterable[(K, Deadline)]): Try[Level0Meter] =
    map.batchExpire(keys.map(keyDeadline => (MapKey.Entry(mapKey, keyDeadline._1), keyDeadline._2)))

  /**
    * Returns target value for the input key.
    */
  def get(key: K): Try[Option[V]] =
    map.get(MapKey.Entry(mapKey, key))

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): Try[Option[K]] =
    map.getKey(MapKey.Entry(mapKey, key)) flatMap {
      case Some(key) =>
        key match {
          case MapKey.Entry(_, dataKey) =>
            Success(Some(dataKey))
          case got =>
            Failure(new Exception(s"Unable to fetch key. Got: $got expected MapKey.Entry"))
        }
      case None =>
        TryUtil.successNone
    }

  def getKeyValue(key: K): Try[Option[(K, V)]] =
    map.getKeyValue(MapKey.Entry(mapKey, key)) flatMap {
      case Some((key, value)) =>
        key match {
          case MapKey.Entry(_, dataKey) =>
            Success(Some(dataKey, value))
          case got =>
            Failure(new Exception(s"Unable to fetch keyValue. Got: $got expected MapKey.Entry"))
        }
      case None =>
        TryUtil.successNone
    }

  /**
    * Returns target value for the input key.
    */
  def getMap(key: K): Try[Option[SubMap[K, V]]] =
    map.contains(MapKey.SubMap(mapKey, key)) map {
      exists =>
        if (exists)
          Some(SubMap[K, V](map.db, mapKey))
        else
          None
    }

  def keys: SubMapKeysIterator[K] =
    SubMapKeysIterator[K](mapKey, keysIterator = DBKeysIterator[MapKey[K]](map.db, Some(From(MapKey.Start(mapKey), orAfter = false, orBefore = false, before = false, after = true))))

  def contains(key: K): Try[Boolean] =
    map contains MapKey.Entry(mapKey, key)

  def mightContain(key: K): Try[Boolean] =
    map mightContain MapKey.Entry(mapKey, key)

  def level0Meter: Level0Meter =
    map.level0Meter

  def level1Meter: LevelMeter =
    map.level1Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    map.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    map.sizeOfSegments

  def keySize(key: K): Int =
    map keySize MapKey.Entry(mapKey, key)

  def valueSize(value: V): Int =
    map valueSize value

  def expiration(key: K): Try[Option[Deadline]] =
    map expiration MapKey.Entry(mapKey, key)

  def timeLeft(key: K): Try[Option[FiniteDuration]] =
    expiration(key).map(_.map(_.timeLeft))

}