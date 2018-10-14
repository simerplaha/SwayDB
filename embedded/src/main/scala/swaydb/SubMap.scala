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
import swaydb.data.submap.Table
import swaydb.iterator.{DBIterator, From, KeysIterator, SubMapIterator}
import swaydb.serializers.{Serializer, _}

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.{Success, Try}

object SubMap {
  def apply[K, V](db: SwayDB,
                  tableKey: K)(implicit keySerializer: Serializer[K],
                               valueSerializer: Serializer[V],
                               ordering: Ordering[Slice[Byte]]): SubMap[K, V] = {
    implicit val tableKeySerializer = Table.tableKeySerializer(keySerializer)
    new SubMap[K, V](db, tableKey)
  }

  def subMap[K, V](parentTableId: K,
                   tableId: K,
                   value: V)(implicit db: SwayDB,
                             map: Map[Table[K], V],
                             keySerializer: Serializer[K],
                             valueSerializer: Serializer[V],
                             ordering: Ordering[Slice[Byte]]): Try[SubMap[K, V]] =
    map.contains(Table.Start(tableId)) flatMap {
      exists =>
        if (exists) {
          implicit val tableKeySerializer = Table.tableKeySerializer(keySerializer)
          Success(new SubMap[K, V](db, tableId))
        } else {
          map.batch(
            Batch.Put(Table.Start(tableId), value),
            Batch.Put(Table.Row(parentTableId, tableId), value),
            Batch.Put(Table.End(tableId), value)
          ) map {
            _ =>
              implicit val tableKeySerializer = Table.tableKeySerializer(keySerializer)
              new SubMap[K, V](db, tableId)
          }
        }
    }
}

/**
  * Key-value or Map database API.
  *
  * For documentation check - http://swaydb.io/api/
  */
class SubMap[K, V](db: SwayDB,
                   tableKey: K)(implicit keySerializer: Serializer[K],
                                tableKeySerializer: Serializer[Table[K]],
                                ordering: Ordering[Slice[Byte]],
                                valueSerializer: Serializer[V]) extends SubMapIterator[K, V](db, tableKey, DBIterator[Table[K], V](db, Some(From(Table.Start(tableKey), false, false, false, true)))) {

  private val map = new Map[Table[K], V](db)

  def subMap(key: K, value: V): Try[SubMap[K, V]] =
    SubMap.subMap[K, V](tableKey, key, value)(db, map, keySerializer, valueSerializer, ordering)

  def put(key: K, value: V): Try[Level0Meter] =
    map.put(key = Table.Row(tableKey, key), value = value)

  def put(key: K, value: V, expireAfter: FiniteDuration): Try[Level0Meter] =
    map.put(Table.Row(tableKey, key), value, expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): Try[Level0Meter] =
    map.put(Table.Row(tableKey, key), value, expireAt)

  def remove(key: K): Try[Level0Meter] =
    map.remove(Table.Row(tableKey, key))

  def remove(from: K, to: K): Try[Level0Meter] =
    map.remove(Table.Row(tableKey, from), Table.Row(tableKey, to))

  def expire(key: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(Table.Row(tableKey, key), after.fromNow)

  def expire(key: K, at: Deadline): Try[Level0Meter] =
    map.expire(Table.Row(tableKey, key), at)

  def expire(from: K, to: K, after: FiniteDuration): Try[Level0Meter] =
    map.expire(Table.Row(tableKey, from), Table.Row(tableKey, to), after.fromNow)

  def expire(from: K, to: K, at: Deadline): Try[Level0Meter] =
    map.expire(Table.Row(tableKey, from), Table.Row(tableKey, to), at)

  def update(key: K, value: V): Try[Level0Meter] =
    map.update(Table.Row(tableKey, key), value)

  def update(from: K, to: K, value: V): Try[Level0Meter] =
    map.update(Table.Row(tableKey, from), Table.Row(tableKey, to), value)

  def batch(batch: Batch[K, V]*): Try[Level0Meter] =
    this.batch(batch)

  def batch(batch: Iterable[Batch[K, V]]): Try[Level0Meter] =
    map.batch(
      batch.map {
        case data @ Data.Put(_, _, _) =>
          data.copy(Table.Row(tableKey, data.key))
        case data @ Data.Remove(_, to, _) =>
          data.copy(from = Table.Row(tableKey, data.from), to = to.map(Table.Row(tableKey, _)))
        case data @ Data.Update(_, to, _) =>
          data.copy(from = Table.Row(tableKey, data.from), to = to.map(Table.Row(tableKey, _)))
        case data @ Data.Add(_, _) =>
          data.copy(elem = Table.Row(tableKey, data.elem))
      }
    )

  def batchPut(keyValues: (K, V)*): Try[Level0Meter] =
    batchPut(keyValues)

  def batchPut(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchPut {
      keyValues map {
        case (key, value) =>
          (Table.Row(tableKey, key), value)
      }
    }

  def batchUpdate(keyValues: (K, V)*): Try[Level0Meter] =
    batchUpdate(keyValues)

  def batchUpdate(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    map.batchUpdate {
      keyValues map {
        case (key, value) =>
          (Table.Row(tableKey, key), value)
      }
    }

  def batchRemove(keys: K*): Try[Level0Meter] =
    batchRemove(keys)

  def batchRemove(keys: Iterable[K]): Try[Level0Meter] =
    map.batchRemove(keys.map(key => Table.Row(tableKey, key)))

  def batchExpire(keys: (K, Deadline)*): Try[Level0Meter] =
    batchExpire(keys)

  def batchExpire(keys: Iterable[(K, Deadline)]): Try[Level0Meter] =
    map.batchExpire(keys.map(keyDeadline => (Table.Row(tableKey, keyDeadline._1), keyDeadline._2)))

  /**
    * Returns target value for the input key.
    */
  def get(key: K): Try[Option[V]] =
    map.get(Table.Row(tableKey, key)).map(_.map(value => valueSerializer.read(value)))

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): Try[Option[K]] =
    map.getKey(Table.Row(tableKey, key)).map(_.map(key => keySerializer.read(key)))

  def getKeyValue(key: K): Try[Option[(K, V)]] =
    map.getKeyValue(Table.Row(tableKey, key)).map(_.map {
      case (key, value) =>
        (keySerializer.read(key), valueSerializer.read(value))
    })

  def keys: KeysIterator[K] =
    KeysIterator[K](db, None)(keySerializer)

  def contains(key: K): Try[Boolean] =
    db contains key

  def mightContain(key: K): Try[Boolean] =
    db mightContain key

  def level0Meter: Level0Meter =
    db.level0Meter

  def level1Meter: LevelMeter =
    db.level1Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    db.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    db.sizeOfSegments

  def keySize(key: K): Int =
    (key: Slice[Byte]).size

  def valueSize(value: V): Int =
    (value: Slice[Byte]).size

  def expiration(key: K): Try[Option[Deadline]] =
    db deadline key

  def timeLeft(key: K): Try[Option[FiniteDuration]] =
    expiration(key).map(_.map(_.timeLeft))

}