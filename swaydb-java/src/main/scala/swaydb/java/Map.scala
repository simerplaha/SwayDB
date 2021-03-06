/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.KeyVal._
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.java.data.util.Java._
import swaydb.utils.Java._
import swaydb.{Expiration, Glass, KeyVal, Pair, Prepare, PureFunction}

import java.nio.file.Path
import java.time.Duration
import java.util.Optional
import java.{lang, util}
import scala.compat.java8.DurationConverters._
import scala.concurrent.duration
import scala.jdk.CollectionConverters._

/**
 * Documentation - http://swaydb.io/
 */
case class Map[K, V, F](asScala: swaydb.Map[K, V, F, Glass])(implicit evd: F <:< PureFunction.Map[K, V]) extends swaydb.java.MapT[K, V, F] {

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

  def remove(from: K, to: K): Unit =
    asScala.remove(from, to)

  def remove(keys: java.lang.Iterable[K]): Unit =
    asScala.remove(keys.asScala)

  def remove(keys: Stream[K]): Unit =
    asScala.remove(keys.asScalaStream)

  def remove(keys: java.util.Iterator[K]): Unit =
    asScala.remove(keys.asScala)

  def expire(key: K, after: java.time.Duration): Unit =
    asScala.expire(key, after.toScala)

  def expire(from: K, to: K, after: java.time.Duration): Unit =
    asScala.expire(from, to, after.toScala)

  def expire(keys: java.lang.Iterable[Pair[K, java.time.Duration]]): Unit =
    asScala.expire(
      keys.asScala map {
        keyValue =>
          (keyValue.left, keyValue.right.toScala.fromNow)
      }
    )

  def expire(keys: Stream[Pair[K, java.time.Duration]]): Unit =
    asScala.expire(keys.asScalaStream.map(_.toScala))

  def expire(keys: java.util.Iterator[Pair[K, java.time.Duration]]): Unit =
    asScala.expire(keys.asScala.map(_.asScalaDeadline))

  def expiration(key: K): Optional[Expiration] =
    asScala.expiration(key).asJavaMap(_.asJava)

  def update(key: K, value: V): Unit =
    asScala.update(key, value)

  def update(from: K, to: K, value: V): Unit =
    asScala.update(from, to, value)

  def update(keyValues: java.lang.Iterable[KeyVal[K, V]]): Unit =
    asScala.update(keyValues.asScala.map(_.toTuple))

  def update(keyValues: Stream[KeyVal[K, V]]): Unit =
    asScala.update(keyValues.asScalaStream.map(_.toTuple))

  def update(keyValues: java.util.Iterator[KeyVal[K, V]]): Unit =
    asScala.update(keyValues.asScala.map(_.toTuple))

  def clearKeyValues(): Unit =
    asScala.clearKeyValues()

  def applyFunction(key: K, function: F): Unit =
    asScala.applyFunction(key, function)

  def applyFunction(from: K, to: K, function: F): Unit =
    asScala.applyFunction(from, to, function)

  def commit(prepare: java.lang.Iterable[Prepare[K, V, F]]): Unit =
    asScala.commit(prepare.asScala)

  def commit(prepare: Stream[Prepare[K, V, F]]): Unit =
    asScala.commit(prepare.asScalaStream)

  def get(key: K): Optional[V] =
    asScala.get(key).asJava

  def getKey(key: K): Optional[K] =
    asScala.getKey(key).asJava

  def getKeyValue(key: K): Optional[KeyVal[K, V]] =
    asScala.getKeyValue(key).asJavaMap(KeyVal(_))

  def getKeyDeadline(key: K): Optional[Pair[K, Optional[Expiration]]] =
    (asScala.getKeyDeadline(key): Option[(K, Option[duration.Deadline])]) match {
      case Some((key, deadline)) =>
        Optional.of(Pair(key, deadline.asJava))

      case None =>
        Optional.empty()
    }

  def getKeyValueDeadline(key: K): Optional[Pair[KeyVal[K, V], Optional[Expiration]]] =
    (asScala.getKeyValueDeadline(key): Option[((K, V), Option[duration.Deadline])]) match {
      case Some(((key, value), deadline)) =>
        Optional.of(Pair(KeyVal(key, value), deadline.asJava))

      case None =>
        Optional.empty()
    }

  def contains(key: K): Boolean =
    asScala.contains(key)

  def mightContain(key: K): Boolean =
    asScala.mightContain(key)

  def mightContainFunction(function: F): Boolean =
    asScala.mightContainFunction(function)

  def toSet: Set[K, Void] =
    Set(asScala.keys.asInstanceOf[swaydb.Set[K, Void, Glass]])(null)

  def keys: Stream[K] =
    Stream.fromScala[K](asScala.keys)

  def values: Stream[V] =
    Stream.fromScala[V](asScala.values)

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

  def keySize(key: K): Int =
    asScala.keySize(key)

  def valueSize(value: V): Int =
    asScala.valueSize(value)

  def timeLeft(key: K): Optional[Duration] =
    asScala.timeLeft(key).asJavaMap(_.toJava)

  override def head: Optional[KeyVal[K, V]] =
    asScala.head.asJavaMap(KeyVal(_))

  override def asScalaStream: swaydb.Source[K, KeyVal[K, V], Glass] =
    asScala.transformValue(_.asKeyVal)

  def sizeOfBloomFilterEntries: Int =
    asScala.sizeOfBloomFilterEntries

  def isEmpty: Boolean =
    asScala.isEmpty

  def nonEmpty: Boolean =
    asScala.nonEmpty

  override def last: Optional[KeyVal[K, V]] =
    asScala.last.asJavaMap(KeyVal(_))

  def clearAppliedFunctions(): lang.Iterable[String] =
    asScala.clearAppliedFunctions().asJava

  def clearAppliedAndRegisteredFunctions(): lang.Iterable[String] =
    asScala.clearAppliedAndRegisteredFunctions().asJava

  def isFunctionApplied(function: F): Boolean =
    asScala.isFunctionApplied(function)

  def asJava: util.Map[K, V] =
    asScala.asScala.asJava

  def close(): Unit =
    asScala.close()

  def delete(): Unit =
    asScala.delete()

  override def equals(other: Any): Boolean =
    other match {
      case other: Map[_, _, _] =>
        other.asScala.equals(this.asScala)

      case _ =>
        false
    }

  override def hashCode(): Int =
    asScala.hashCode()

  override def toString(): String =
    asScala.toString()
}
