/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb

import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter

import java.nio.file.Path
import scala.collection.compat.IterableOnce
import scala.collection.mutable
import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Base trait for a basic SwayDB SetMap type.
 */
trait SetMapT[K, V, BAG[_]] extends Source[K, (K, V), BAG] {

  def path: Path

  def put(key: K, value: V): BAG[OK]

  def put(key: K, value: V, expireAt: Option[Deadline]): BAG[OK] =
    expireAt match {
      case Some(expireAt) =>
        put(key, value, expireAt)

      case None =>
        put(key, value)
    }

  def put(key: K, value: V, expireAfter: FiniteDuration): BAG[OK]

  def put(key: K, value: V, expireAt: Deadline): BAG[OK]

  def put(keyValues: (K, V)*): BAG[OK]

  def put(keyValues: Stream[(K, V), BAG]): BAG[OK]

  def put(keyValues: IterableOnce[(K, V)]): BAG[OK]

  def remove(key: K): BAG[OK]

  def remove(keys: K*): BAG[OK]

  def remove(keys: Stream[K, BAG]): BAG[OK]

  def remove(keys: IterableOnce[K]): BAG[OK]

  def expire(key: K, after: FiniteDuration): BAG[OK]

  def expire(key: K, at: Deadline): BAG[OK]

  def clearKeyValues(): BAG[OK]

  def get(key: K): BAG[Option[V]]

  def getKey(key: K): BAG[Option[K]]

  def getKeyValue(key: K): BAG[Option[(K, V)]]

  def contains(key: K): BAG[Boolean]

  def mightContain(key: K): BAG[Boolean]

  private[swaydb] def keySet: mutable.Set[K]

  def levelZeroMeter: LevelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter]

  def sizeOfSegments: Long

  def blockCacheSize(): Option[Long]

  def cachedKeyValuesSize(): Option[Long]

  def openedFiles(): Option[Long]

  def pendingDeletes(): Option[Long]

  def expiration(key: K): BAG[Option[Deadline]]

  def timeLeft(key: K): BAG[Option[FiniteDuration]]

  def head: BAG[Option[(K, V)]]

  def keys: Stream[K, BAG]

  def values: Stream[V, BAG]

  def sizeOfBloomFilterEntries: BAG[Int]

  def isEmpty: BAG[Boolean]

  def nonEmpty: BAG[Boolean]

  def last: BAG[Option[(K, V)]]

  def toBag[X[_]](implicit bag: Bag[X]): SetMapT[K, V, X]

  def asScala: scala.collection.mutable.Map[K, V]

  def close(): BAG[Unit]

  def delete(): BAG[Unit]
}
