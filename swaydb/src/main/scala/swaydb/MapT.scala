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

import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Base trait for a basic SwayDB Map type.
 */
trait MapT[K, V, F, BAG[_]] extends SetMapT[K, V, F, BAG] { self =>

  def path: Path

  def put(key: K, value: V): BAG[OK]

  def put(key: K, value: V, expireAfter: FiniteDuration): BAG[OK]

  def put(key: K, value: V, expireAt: Deadline): BAG[OK]

  def put(keyValues: (K, V)*): BAG[OK]

  def put(keyValues: Stream[(K, V)]): BAG[OK]

  def put(keyValues: Iterable[(K, V)]): BAG[OK]

  def put(keyValues: Iterator[(K, V)]): BAG[OK]

  def remove(key: K): BAG[OK]

  def remove(from: K, to: K): BAG[OK]

  def remove(keys: K*): BAG[OK]

  def remove(keys: Stream[K]): BAG[OK]

  def remove(keys: Iterable[K]): BAG[OK]

  def remove(keys: Iterator[K]): BAG[OK]

  def expire(key: K, after: FiniteDuration): BAG[OK]

  def expire(key: K, at: Deadline): BAG[OK]

  def expire(from: K, to: K, after: FiniteDuration): BAG[OK]

  def expire(from: K, to: K, at: Deadline): BAG[OK]

  def expire(keys: (K, Deadline)*): BAG[OK]

  def expire(keys: Stream[(K, Deadline)]): BAG[OK]

  def expire(keys: Iterable[(K, Deadline)]): BAG[OK]

  def expire(keys: Iterator[(K, Deadline)]): BAG[OK]

  def update(key: K, value: V): BAG[OK]

  def update(from: K, to: K, value: V): BAG[OK]

  def update(keyValues: (K, V)*): BAG[OK]

  def update(keyValues: Stream[(K, V)]): BAG[OK]

  def update(keyValues: Iterable[(K, V)]): BAG[OK]

  def update(keyValues: Iterator[(K, V)]): BAG[OK]

  def clearKeyValues(): BAG[OK]

  def applyFunction[PF <: F](key: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK]

  def applyFunction[PF <: F](from: K, to: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK]

  def commit[PF <: F](prepare: Prepare[K, V, PF]*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK]

  def commit[PF <: F](prepare: Stream[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK]

  def commit[PF <: F](prepare: Iterable[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[OK]

  /**
   * Returns target value for the input key.
   */
  def get(key: K): BAG[Option[V]]

  /**
   * Returns target full key for the input partial key.
   *
   * This function is mostly used for Set databases where partial ordering on the Key is provided.
   */
  def getKey(key: K): BAG[Option[K]]

  def getKeyValue(key: K): BAG[Option[(K, V)]]

  def getKeyDeadline(key: K): BAG[Option[(K, Option[Deadline])]]

  def getKeyDeadline[BAG[_]](key: K, bag: Bag[BAG]): BAG[Option[(K, Option[Deadline])]]

  def contains(key: K): BAG[Boolean]

  def mightContain(key: K): BAG[Boolean]

  def mightContainFunction[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): BAG[Boolean]

  def levelZeroMeter: LevelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter]

  def sizeOfSegments: Long

  def keySize(key: K): Int

  def valueSize(value: V): Int

  def expiration(key: K): BAG[Option[Deadline]]

  def timeLeft(key: K): BAG[Option[FiniteDuration]]

  def from(key: K): MapT[K, V, F, BAG]

  def before(key: K): MapT[K, V, F, BAG]

  def fromOrBefore(key: K): MapT[K, V, F, BAG]

  def after(key: K): MapT[K, V, F, BAG]

  def fromOrAfter(key: K): MapT[K, V, F, BAG]

  def headOption: BAG[Option[(K, V)]]

  def headOrNull: BAG[(K, V)]

  def stream: Stream[(K, V)]

  def iterator[BAG[_]](implicit bag: Bag.Sync[BAG]): Iterator[BAG[(K, V)]]

  def sizeOfBloomFilterEntries: BAG[Int]

  def isEmpty: BAG[Boolean]

  def nonEmpty: BAG[Boolean]

  def lastOption: BAG[Option[(K, V)]]

  def reverse: MapT[K, V, F, BAG]

  /**
   * Returns an Async API of type O where the [[Bag]] is known.
   */
  def toBag[X[_]](implicit bag: Bag[X]): MapT[K, V, F, X]

  def asScala: scala.collection.mutable.Map[K, V]

  def close(): BAG[Unit]

  def delete(): BAG[Unit]
}