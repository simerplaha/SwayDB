/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb

import java.nio.file.Path

import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter

import scala.collection.mutable
import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Base trait for a basic SwayDB SetMap type.
 */
trait SetMapT[K, V, F, BAG[_]] {

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

  def put(keyValues: Stream[(K, V)]): BAG[OK]

  def put(keyValues: Iterable[(K, V)]): BAG[OK]

  def put(keyValues: Iterator[(K, V)]): BAG[OK]

  def remove(key: K): BAG[OK]

  def remove(keys: K*): BAG[OK]

  def remove(keys: Stream[K]): BAG[OK]

  def remove(keys: Iterable[K]): BAG[OK]

  def remove(keys: Iterator[K]): BAG[OK]

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

  def expiration(key: K): BAG[Option[Deadline]]

  def timeLeft(key: K): BAG[Option[FiniteDuration]]

  def from(key: K): SetMapT[K, V, F, BAG]

  def before(key: K): SetMapT[K, V, F, BAG]

  def fromOrBefore(key: K): SetMapT[K, V, F, BAG]

  def after(key: K): SetMapT[K, V, F, BAG]

  def fromOrAfter(key: K): SetMapT[K, V, F, BAG]

  def headOption: BAG[Option[(K, V)]]

  def headOrNull: BAG[(K, V)]

  def stream: Stream[(K, V)]

  def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[(K, V)]]

  def sizeOfBloomFilterEntries: BAG[Int]

  def isEmpty: BAG[Boolean]

  def nonEmpty: BAG[Boolean]

  def lastOption: BAG[Option[(K, V)]]

  def reverse: SetMapT[K, V, F, BAG]

  def toBag[X[_]](implicit bag: Bag[X]): SetMapT[K, V, F, X]

  def asScala: scala.collection.mutable.Map[K, V]

  def close(): BAG[Unit]

  def delete(): BAG[Unit]

  override def toString(): String =
    classOf[SetMapT[_, _, _, BAG]].getSimpleName
}