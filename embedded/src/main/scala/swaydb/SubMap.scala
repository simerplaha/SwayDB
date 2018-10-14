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

import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.data.map.MapKey
import swaydb.iterator.{DBIterator, From, KeysIterator, SubMapIterator}
import swaydb.serializers.{Serializer, _}

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Success, Try}

object SubMap {
  def apply[K, V](db: SwayDB,
                  mapKey: K)(implicit keySerializer: Serializer[K],
                             valueSerializer: Serializer[V],
                             ordering: Ordering[Slice[Byte]]): SubMap[K, V] = {
    implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)
    val map = Map[MapKey[K], V](db)
    new SubMap[K, V](map, mapKey)
  }

  def subMap[K, V](map: Map[MapKey[K], V],
                   parentMapKey: K,
                   mapKey: K,
                   value: V)(implicit keySerializer: Serializer[K],
                             valueSerializer: Serializer[V],
                             ordering: Ordering[Slice[Byte]]): Try[SubMap[K, V]] =
    map.contains(MapKey.Start(mapKey)) flatMap {
      exists =>
        if (exists) {
          implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)
          Success(SubMap[K, V](map.db, mapKey))
        } else {
          map.batch(
            Batch.Put(MapKey.Start(mapKey), value),
            Batch.Put(MapKey.Row(parentMapKey, mapKey), value),
            Batch.Put(MapKey.End(mapKey), value)
          ) map {
            _ =>
              implicit val mapKeySerializer = MapKey.mapKeySerializer(keySerializer)
              SubMap[K, V](map.db, mapKey)
          }
        }
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
                              valueSerializer: Serializer[V]) extends SubMapIterator[K, V](mapKey, DBIterator[MapKey[K], V](map.db, Some(From(MapKey.Start(mapKey), false, false, false, true)))) {

  def subMap(key: K, value: V): Try[SubMap[K, V]] =
    SubMap.subMap[K, V](map, mapKey, key, value)

  def put(key: K, value: V): Try[Level0Meter] =
    map.put(key = MapKey.Row(mapKey, key), value = value)

  def put(key: K, value: V, expireAfter: FiniteDuration): Try[Level0Meter] =
    map.put(MapKey.Row(mapKey, key), value, expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): Try[Level0Meter] =
    map.put(MapKey.Row(mapKey, key), value, expireAt)

  def remove(key: K): Try[Level0Meter] =
    map.remove(MapKey.Row(mapKey, key))

  def remove(from: K, to: K): Try[Level0Meter] =
    map.remove(MapKey.Row(mapKey, from), MapKey.Row(mapKey, to))

  def expire(key: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(MapKey.Row(mapKey, key), after.fromNow)

  def expire(key: K, at: Deadline): Try[Level0Meter] =
    map.expire(MapKey.Row(mapKey, key), at)

  def expire(from: K, to: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(MapKey.Row(mapKey, from), MapKey.Row(mapKey, to), after.fromNow)

  def expire(from: K, to: K, at: Deadline): Try[Level0Meter] =
    map.expire(MapKey.Row(mapKey, from), MapKey.Row(mapKey, to), at)

  def update(key: K, value: V): Try[Level0Meter] =
    map.update(MapKey.Row(mapKey, key), value)

  def update(from: K, to: K, value: V): Try[Level0Meter] =
    map.update(MapKey.Row(mapKey, from), MapKey.Row(mapKey, to), value)

  def batch(batch: Batch[K, V]*): Try[Level0Meter] =
    this.batch(batch)

  def batch(batch: Iterable[Batch[K, V]]): Try[Level0Meter] =
    map.batch(
      batch.map {
        case data @ Data.Put(_, _, _) =>
          data.copy(MapKey.Row(mapKey, data.key))
        case data @ Data.Remove(_, to, _) =>
          data.copy(from = MapKey.Row(mapKey, data.from), to = to.map(MapKey.Row(mapKey, _)))
        case data @ Data.Update(_, to, _) =>
          data.copy(from = MapKey.Row(mapKey, data.from), to = to.map(MapKey.Row(mapKey, _)))
        case data @ Data.Add(_, _) =>
          data.copy(elem = MapKey.Row(mapKey, data.elem))
      }
    )

  def batchPut(keyValues: (K, V)*): Try[Level0Meter] =
    batchPut(keyValues)

  def batchPut(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchPut {
      keyValues map {
        case (key, value) =>
          (MapKey.Row(mapKey, key), value)
      }
    }

  def batchUpdate(keyValues: (K, V)*): Try[Level0Meter] =
    batchUpdate(keyValues)

  def batchUpdate(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchUpdate {
      keyValues map {
        case (key, value) =>
          (MapKey.Row(mapKey, key), value)
      }
    }

  def batchRemove(keys: K*): Try[Level0Meter] =
    batchRemove(keys)

  def batchRemove(keys: Iterable[K]): Try[Level0Meter] =
    map.batchRemove(keys.map(key => MapKey.Row(mapKey, key)))

  def batchExpire(keys: (K, Deadline)*): Try[Level0Meter] =
    batchExpire(keys)

  def batchExpire(keys: Iterable[(K, Deadline)]): Try[Level0Meter] =
    map.batchExpire(keys.map(keyDeadline => (MapKey.Row(mapKey, keyDeadline._1), keyDeadline._2)))

  /**
    * Returns target value for the input key.
    */
  def get(key: K): Try[Option[V]] =
    map.get(MapKey.Row(mapKey, key)).map(_.map(value => valueSerializer.read(value)))

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): Try[Option[K]] =
    map.getKey(MapKey.Row(mapKey, key)).map(_.map(key => keySerializer.read(key)))

  def getKeyValue(key: K): Try[Option[(K, V)]] =
    map.getKeyValue(MapKey.Row(mapKey, key)).map(_.map {
      case (key, value) =>
        (keySerializer.read(key), valueSerializer.read(value))
    })

  def keys: KeysIterator[K] = ???

  //    KeysIterator[K](db, None)(keySerializer)

  def contains(key: K): Try[Boolean] =
    map contains MapKey.Row(mapKey, key)

  def mightContain(key: K): Try[Boolean] =
    map mightContain MapKey.Row(mapKey, key)

  def level0Meter: Level0Meter =
    map.level0Meter

  def level1Meter: LevelMeter =
    map.level1Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    map.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    map.sizeOfSegments

  def keySize(key: K): Int =
    map keySize MapKey.Row(mapKey, key)

  def valueSize(value: V): Int =
    map valueSize value

  def expiration(key: K): Try[Option[Deadline]] =
    map expiration MapKey.Row(mapKey, key)

  def timeLeft(key: K): Try[Option[FiniteDuration]] =
    expiration(key).map(_.map(_.timeLeft))

}