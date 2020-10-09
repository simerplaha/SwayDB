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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.java

import java.nio.file.Path
import java.time.Duration
import java.util
import java.util.Optional

import swaydb.Bag.Glass
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.util.Java._
import swaydb.java.data.util.Java._
import swaydb.{Bag, Expiration, KeyVal}

import scala.compat.java8.DurationConverters._
import scala.jdk.CollectionConverters._

/**
 * Documentation - http://swaydb.io/
 */
case class SetMap[K, V](asScala: swaydb.SetMap[K, V, Bag.Glass]) extends SetMapT[K, V] {

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

  def asJava: util.Map[K, V] =
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
