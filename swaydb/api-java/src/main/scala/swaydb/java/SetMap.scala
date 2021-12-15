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

import swaydb.config.compaction.LevelMeter
import swaydb.java.KeyVal._
import swaydb.java.util.Java._
import swaydb.utils.Java._
import swaydb.{Expiration, Glass}

import java.nio.file.Path
import java.time.Duration
import java.util.Optional
import scala.compat.java8.DurationConverters._
import scala.jdk.CollectionConverters._

/**
 * Documentation - https://swaydb.io/
 */
case class SetMap[K, V](asScala: swaydb.SetMap[K, V, Glass]) extends SetMapT[K, V] {

  def path: Path =
    asScala.path

  def put(key: K, value: V): Unit =
    asScala.put(key, value)

  def put(key: K, value: V, expireAfter: java.time.Duration): Unit =
    asScala.put(key, value, expireAfter.toScala)

  def put(keyValues: java.lang.Iterable[KeyVal[K, V]]): Unit =
    asScala.put(keyValues.asScala.map(_.toTuple))

  def put(keyValues: Stream[KeyVal[K, V]]): Unit =
    asScala.put(keyValues.asScalaStream.map(_.toTuple))

  def put(keyValues: java.util.Iterator[KeyVal[K, V]]): Unit =
    asScala.put(keyValues.asScala.map(_.toTuple))

  def remove(key: K): Unit =
    asScala.remove(key)

  def remove(keys: java.lang.Iterable[K]): Unit =
    asScala.remove(keys.asScala)

  def remove(keys: Stream[K]): Unit =
    asScala.remove(keys.asScalaStream)

  def remove(keys: java.util.Iterator[K]): Unit =
    asScala.remove(keys.asScala)

  def expire(key: K, after: java.time.Duration): Unit =
    asScala.expire(key, after.toScala)

  def expiration(key: K): Optional[Expiration] =
    asScala.expiration(key).asJavaMap(_.asJava)

  def clearKeyValues(): Unit =
    asScala.clearKeyValues()

  def get(key: K): Optional[V] =
    asScala.get(key).asJava

  def getKey(key: K): Optional[K] =
    asScala.getKey(key).asJava

  def getKeyValue(key: K): Optional[KeyVal[K, V]] =
    asScala.getKeyValue(key).asJavaMap(KeyVal(_))

  def contains(key: K): Boolean =
    asScala.contains(key)

  def mightContain(key: K): Boolean =
    asScala.mightContain(key)

  def levelZeroMeter: LevelZeroMeter =
    asScala.levelZeroMeter

  def levelMeter(levelNumber: Int): Optional[LevelMeter] =
    asScala.levelMeter(levelNumber).asJava

  def sizeOfSegments: Long =
    asScala.sizeOfSegments

  def blockCacheSize(): Option[Long] =
    asScala.blockCacheSize()

  def cachedKeyValuesSize(): Option[Long] =
    asScala.cachedKeyValuesSize()

  def openedFiles(): Option[Long] =
    asScala.openedFiles()

  def pendingDeletes(): Option[Long] =
    asScala.pendingDeletes()

  def timeLeft(key: K): Optional[Duration] =
    asScala.timeLeft(key).asJavaMap(_.toJava)

  override def head: Optional[KeyVal[K, V]] =
    asScala.head.asJavaMap(KeyVal(_))

  override def keys: Stream[K] =
    Stream.fromScala[K](asScala.keys)

  def values: Stream[V] =
    Stream.fromScala[V](asScala.values)

  def sizeOfBloomFilterEntries: Int =
    asScala.sizeOfBloomFilterEntries

  def isEmpty: Boolean =
    asScala.isEmpty

  def nonEmpty: Boolean =
    asScala.nonEmpty

  override def last: Optional[KeyVal[K, V]] =
    asScala.last.asJavaMap(KeyVal(_))

  def asJava: java.util.Map[K, V] =
    asScala.asScala.asJava

  override def asScalaStream: swaydb.Source[K, KeyVal[K, V], Glass] =
    asScala.transformValue(_.asKeyVal)

  def close(): Unit =
    asScala.close()

  def delete(): Unit =
    asScala.delete()

  private def copy(): Unit = ()

  override def equals(other: Any): Boolean =
    other match {
      case other: SetMap[_, _] =>
        other.asScala.equals(this.asScala)

      case _ =>
        false
    }

  override def hashCode(): Int =
    asScala.hashCode()

  override def toString(): String =
    asScala.toString()
}
