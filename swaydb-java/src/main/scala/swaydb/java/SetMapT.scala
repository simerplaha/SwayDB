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

package swaydb.java

import java.nio.file.Path
import java.time.Duration
import java.util.Optional

import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.{Expiration, KeyVal, OK}

/**
 * Base trait for a basic SwayDB SetMap type.
 */
trait SetMapT[K, V] {

  def path: Path

  def put(key: K, value: V): OK

  def put(key: K, value: V, expireAfter: Duration): OK

  def put(keyValues: Stream[KeyVal[K, V]]): OK

  def put(keyValues: java.lang.Iterable[KeyVal[K, V]]): OK

  def put(keyValues: java.util.Iterator[KeyVal[K, V]]): OK

  def remove(key: K): OK

  def remove(keys: Stream[K]): OK

  def remove(keys: java.lang.Iterable[K]): OK

  def remove(keys: java.util.Iterator[K]): OK

  def expire(key: K, after: Duration): OK

  def clearKeyValues(): OK

  def get(key: K): Optional[V]

  def getKey(key: K): Optional[K]

  def getKeyValue(key: K): Optional[KeyVal[K, V]]

  def contains(key: K): java.lang.Boolean

  def mightContain(key: K): java.lang.Boolean

  def levelZeroMeter: LevelZeroMeter

  def levelMeter(levelNumber: Int): Optional[LevelMeter]

  def sizeOfSegments: Long

  def expiration(key: K): Optional[Expiration]

  def timeLeft(key: K): Optional[Duration]

  def head: Optional[KeyVal[K, V]]

  def stream: Source[K, KeyVal[K, V]]

  def keys: Stream[K]

  def values: Stream[V]

  def iterator: java.util.Iterator[KeyVal[K, V]]

  def reverse: Source[K, KeyVal[K, V]] =
    stream.reverse

  def sizeOfBloomFilterEntries: Int

  def isEmpty: java.lang.Boolean

  def nonEmpty: java.lang.Boolean

  def last: Optional[KeyVal[K, V]]

  def asJava: java.util.Map[K, V]

  def close(): Unit

  def delete(): Unit

  override def toString(): String =
    this.getClass.getSimpleName
}
