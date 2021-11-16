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

package swaydb.java

import swaydb.Expiration
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter

import java.nio.file.Path
import java.time.Duration
import java.util.Optional

/**
 * Base trait for a basic SwayDB SetMap type.
 */
trait SetMapT[K, V] extends swaydb.java.Source[K, KeyVal[K, V]] {

  def path: Path

  def put(key: K, value: V): Unit

  def put(key: K, value: V, expireAfter: Duration): Unit

  def put(keyValues: Stream[KeyVal[K, V]]): Unit

  def put(keyValues: java.lang.Iterable[KeyVal[K, V]]): Unit

  def put(keyValues: java.util.Iterator[KeyVal[K, V]]): Unit

  def remove(key: K): Unit

  def remove(keys: Stream[K]): Unit

  def remove(keys: java.lang.Iterable[K]): Unit

  def remove(keys: java.util.Iterator[K]): Unit

  def expire(key: K, after: Duration): Unit

  def clearKeyValues(): Unit

  def get(key: K): Optional[V]

  def getKey(key: K): Optional[K]

  def getKeyValue(key: K): Optional[KeyVal[K, V]]

  def contains(key: K): Boolean

  def mightContain(key: K): Boolean

  def levelZeroMeter: LevelZeroMeter

  def levelMeter(levelNumber: Int): Optional[LevelMeter]

  def sizeOfSegments: Long

  def blockCacheSize(): Option[Long]

  def cachedKeyValuesSize(): Option[Long]

  def openedFiles(): Option[Long]

  def pendingDeletes(): Option[Long]

  def expiration(key: K): Optional[Expiration]

  def timeLeft(key: K): Optional[Duration]

  def head: Optional[KeyVal[K, V]]

  def keys: Stream[K]

  def values: Stream[V]

  def sizeOfBloomFilterEntries: Int

  def isEmpty: Boolean

  def nonEmpty: Boolean

  def last: Optional[KeyVal[K, V]]

  def asJava: java.util.Map[K, V]

  def close(): Unit

  def delete(): Unit
}
