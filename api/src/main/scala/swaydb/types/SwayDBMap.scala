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
import swaydb.iterator.{DBIterator, KeysIterator}
import swaydb.serializers.{Serializer, _}
import swaydb.types.BatchImplicits._

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

  def put(key: K, value: V): Try[Level0Meter] =
    api.put(key, Some(value))

  def batch(batch: Batch[K, V]*): Try[Level0Meter] =
    api.put(batch)

  def batch(batch: Iterable[Batch[K, V]]): Try[Level0Meter] =
    api.put(batch)

  def batchPut(keyValues: (K, V)*): Try[Level0Meter] =
    batchPut(keyValues)

  def batchPut(keyValues: Iterable[(K, V)]): Try[Level0Meter] =
    api.put {
      keyValues map {
        case (key, value) =>
          request.Batch.Put(key, Some(value))
      }
    }

  def batchRemove(keys: K*): Try[Level0Meter] =
    batchRemove(keys)

  def batchRemove(keys: Iterable[K]): Try[Level0Meter] =
    api.put(keys.map(request.Batch.Remove(_)))

  def remove(key: K): Try[Level0Meter] =
    api.remove(key)

  def remove(from: K, until: K): Try[Level0Meter] =
    api.remove(from, until)

  def update(from: K, until: K, value: V): Try[Level0Meter] =
    api.update(from, until, Some(value))

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

}
