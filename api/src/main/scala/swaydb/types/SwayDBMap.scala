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

package swaydb.types

import swaydb.Batch
import swaydb.api.SwayDBAPI
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.request
import swaydb.data.slice.Slice
import swaydb.iterator.{DBIterator, KeysIterator}
import swaydb.macros.CheckId
import swaydb.serializers.{Serializer, _}
import swaydb.types.BatchImplicits._

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.util.Try

object SwayDBMap {

  def apply[K, V](raw: SwayDBAPI)(implicit keySerializer: Serializer[K],
                                  valueSerializer: Serializer[V]): SwayDBMap[K, V] = {
    new SwayDBMap[K, V](raw)
  }
}

/**
  * Key-value or Map database API.
  *
  * For documentation check - http://swaydb.io/api/
  */
class SwayDBMap[K, V](api: SwayDBAPI)(implicit keySerializer: Serializer[K],
                                      valueSerializer: Serializer[V]) extends DBIterator[K, V](api, None) {

  def cacheFunction(functionId: String, function: V => V)(implicit checkId: CheckId): Try[String] =
    api.cacheFunction(
      functionId = functionId,
      function =
        (oldValue: Slice[Byte]) => {
          val oldValueTyped = valueSerializer.read(oldValue)
          val newValue = function(oldValueTyped)
          valueSerializer.write(newValue)
        }
    )

  def put(key: K, value: V): Try[Level0Meter] =
    api.put(key, Some(value))

  def put(key: K, value: V, expireAfter: FiniteDuration): Try[Level0Meter] =
    api.put(key, Some(value), expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): Try[Level0Meter] =
    api.put(key, Some(value), expireAt)

  def remove(key: K): Try[Level0Meter] =
    api.remove(key)

  def remove(from: K, to: K): Try[Level0Meter] =
    api.remove(from, to)

  def expire(key: K, after: FiniteDuration): Try[Level0Meter] =
    api.expire(key, after.fromNow)

  def expire(key: K, at: Deadline): Try[Level0Meter] =
    api.expire(key, at)

  def expire(from: K, to: K, after: FiniteDuration): Try[Level0Meter] =
    api.expire(from, to, after.fromNow)

  def expire(from: K, to: K, at: Deadline): Try[Level0Meter] =
    api.expire(from, to, at)

  def update(key: K, value: V): Try[Level0Meter] =
    api.update(key, Some(value))

  def update(key: K, functionId: String): Try[Level0Meter] =
    api.update(key, functionId)

  def update(from: K, to: K, value: V): Try[Level0Meter] =
    api.update(from, to, Some(value))

  def update(from: K, to: K, functionId: String): Try[Level0Meter] =
    api.update(from, to, functionId)

  def batch(batch: Batch[K, V]*): Try[Level0Meter] =
    api.batch(batch)

  def batch(batch: Iterable[Batch[K, V]]): Try[Level0Meter] =
    api.batch(batch)

  def batchPut(keyValues: (K, V)*): Try[Level0Meter] =
    batchPut(keyValues)

  def batchPut(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    api.batch {
      keyValues map {
        case (key, value) =>
          request.Batch.Put(key, Some(value), None)
      }
    }

  def batchUpdate(keyValues: (K, V)*): Try[Level0Meter] =
    batchUpdate(keyValues)

  def batchUpdate(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    api.batch {
      keyValues map {
        case (key, value) =>
          request.Batch.Update(key, Some(value))
      }
    }

  def batchRemove(keys: K*): Try[Level0Meter] =
    batchRemove(keys)

  def batchRemove(keys: Iterable[K]): Try[Level0Meter] =
    api.batch(keys.map(key => request.Batch.Remove(key, None)))

  def batchExpire(keys: (K, Deadline)*): Try[Level0Meter] =
    batchExpire(keys)

  def batchExpire(keys: Iterable[(K, Deadline)]): Try[Level0Meter] =
    api.batch(keys.map(keyDeadline => request.Batch.Remove(keyDeadline._1, Some(keyDeadline._2))))

  /**
    * Returns target value for the input key.
    */
  def get(key: K): Try[Option[V]] =
    api.get(key).map(_.map(_.read[V]))

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): Try[Option[K]] =
    api.getKey(key).map(_.map(_.read[K]))

  def getKeyValue(key: K): Try[Option[(K, V)]] =
    api.getKeyValue(key).map(_.map {
      case (key, value) =>
        (key.read[K], value.read[V])
    })

  def contains(key: K): Try[Boolean] =
    api contains key

  def mightContain(key: K): Try[Boolean] =
    api mightContain key

  def keys: KeysIterator[K] =
    KeysIterator[K](api, None)(keySerializer)

  def level0Meter: Level0Meter =
    api.level0Meter

  def level1Meter: LevelMeter =
    api.level1Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    api.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    api.sizeOfSegments

  def keySize(key: K): Int =
    (key: Slice[Byte]).size

  def valueSize(value: V): Int =
    (value: Slice[Byte]).size

  def expiration(key: K): Try[Option[Deadline]] =
    api deadline key

  def timeLeft(key: K): Try[Option[FiniteDuration]] =
    expiration(key).map(_.map(_.timeLeft))
}