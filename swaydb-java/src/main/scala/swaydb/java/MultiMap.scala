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
import java.{lang, util}

import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.util.Java._
import swaydb.java.data.util.Java._
import swaydb.java.multimap.Schema
import swaydb.multimap.MultiPrepare
import swaydb.{Bag, Expiration, KeyVal, OK, Pair, Prepare, PureFunction}

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration
import scala.jdk.CollectionConverters._

/**
 * Documentation - http://swaydb.io/
 */
case class MultiMap[M, K, V, F](asScala: swaydb.MultiMap[M, K, V, F, Bag.Less])(implicit evd: F <:< PureFunction.Map[K, V]) extends MapT[K, V, F] {

  def mapKey = asScala.mapKey

  def defaultExpiration: Optional[Expiration] = Expiration(asScala.defaultExpiration)

  def path: Path =
    asScala.path

  def schema: Schema[M, K, V, F] =
    Schema(asScala.schema)

  def put(key: K, value: V): swaydb.OK =
    asScala.put(key, value)

  def put(key: K, value: V, expireAfter: java.time.Duration): swaydb.OK =
    asScala.put(key, value, expireAfter.toScala)

  def put(keyValues: java.lang.Iterable[KeyVal[K, V]]): swaydb.OK =
    asScala.put(keyValues.asScala.map(_.toTuple))

  def put(keyValues: Stream[KeyVal[K, V]]): swaydb.OK =
    asScala.put(keyValues.asScala.map(_.toTuple))

  def put(keyValues: java.util.Iterator[KeyVal[K, V]]): swaydb.OK =
    asScala.put(keyValues.asScala.map(_.toTuple))

  def remove(key: K): swaydb.OK =
    asScala.remove(key)

  def remove(from: K, to: K): swaydb.OK =
    asScala.remove(from = from, to = to)

  def remove(keys: java.lang.Iterable[K]): swaydb.OK =
    asScala.remove(keys = keys.asScala)

  def remove(keys: Stream[K]): swaydb.OK =
    asScala.remove(keys.asScala)

  def remove(keys: java.util.Iterator[K]): swaydb.OK =
    asScala.remove(keys.asScala)

  def expire(key: K, after: java.time.Duration): swaydb.OK =
    asScala.expire(key, after.toScala)

  def expire(from: K, to: K, after: java.time.Duration): swaydb.OK =
    asScala.expire(from, to, after.toScala)

  def expire(keys: java.lang.Iterable[Pair[K, java.time.Duration]]): swaydb.OK =
    asScala.expire(
      keys.asScala map {
        keyValue =>
          (keyValue.left, keyValue.right.toScala.fromNow)
      }
    )

  def expire(keys: Stream[Pair[K, java.time.Duration]]): swaydb.OK =
    asScala.expire(keys.asScala.map(_.toScala))

  def expire(keys: java.util.Iterator[Pair[K, java.time.Duration]]): swaydb.OK =
    asScala.expire(keys.asScala.map(_.asScalaDeadline))

  def expiration(key: K): Optional[Expiration] =
    asScala.expiration(key).asJavaMap(_.asJava)

  def update(key: K, value: V): swaydb.OK =
    asScala.update(key, value)

  def update(from: K, to: K, value: V): swaydb.OK =
    asScala.update(from, to, value)

  def update(keyValues: java.lang.Iterable[KeyVal[K, V]]): swaydb.OK =
    asScala.update(keyValues.asScala.map(_.toTuple))

  def update(keyValues: Stream[KeyVal[K, V]]): swaydb.OK =
    asScala.update(keyValues.asScala.map(_.toTuple))

  def update(keyValues: java.util.Iterator[KeyVal[K, V]]): swaydb.OK =
    asScala.update(keyValues.asScala.map(_.toTuple))

  def clearKeyValues(): swaydb.OK =
    asScala.clearKeyValues()

  def applyFunction(key: K, function: F): swaydb.OK =
    asScala.applyFunction(key, function)

  def applyFunction(from: K, to: K, function: F): swaydb.OK =
    asScala.applyFunction(from, to, function)

  def commitMultiPrepare(transaction: java.lang.Iterable[MultiPrepare[M, K, V, F]]): OK =
    asScala.commitMultiPrepare(transaction.asScala)

  def commitMultiPrepare(transaction: java.util.Iterator[MultiPrepare[M, K, V, F]]): OK =
    asScala.commitMultiPrepare(transaction.asScala)

  def commitMultiPrepare(transaction: java.util.stream.Stream[MultiPrepare[M, K, V, F]]): OK =
    asScala.commitMultiPrepare(transaction.iterator().asScala)

  def commit(prepare: java.lang.Iterable[Prepare[K, V, F]]): swaydb.OK =
    asScala.commit(prepare.asScala)

  def commit(prepare: Stream[Prepare[K, V, F]]): swaydb.OK =
    asScala.commit(prepare.asScala)

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
    (asScala.getKeyValueDeadline(key, Bag.less): Option[((K, V), Option[duration.Deadline])]) match {
      case Some(((key, value), deadline)) =>
        Optional.of(Pair(KeyVal(key, value), deadline.asJava))

      case None =>
        Optional.empty()
    }

  def contains(key: K): java.lang.Boolean =
    asScala.contains(key)

  def mightContain(key: K): java.lang.Boolean =
    asScala.mightContain(key)

  def mightContainFunction(function: F): java.lang.Boolean =
    asScala.mightContainFunction(function)

  def levelZeroMeter: LevelZeroMeter =
    asScala.levelZeroMeter

  def levelMeter(levelNumber: Int): Optional[LevelMeter] =
    asScala.levelMeter(levelNumber).asJava

  def sizeOfSegments: Long =
    asScala.sizeOfSegments

  def keySize(key: K): Int =
    asScala.keySize(key)

  def valueSize(value: V): Int =
    asScala.valueSize(value)

  def timeLeft(key: K): Optional[Duration] =
    asScala.timeLeft(key).asJavaMap(_.toJava)

  def head: Optional[KeyVal[K, V]] =
    asScala.head.asJavaMap(KeyVal(_))

  override def keys: Stream[K] =
    new Stream[K](asScala.keys)

  def values: Stream[V] =
    new Stream[V](asScala.values)

  def stream: Source[K, KeyVal[K, V]] =
    new Source(asScala.stream.transformValue(_.asKeyVal))

  def iterator: java.util.Iterator[KeyVal[K, V]] =
    asScala
      .iterator(Bag.less)
      .map(KeyVal(_))
      .asJava

  def sizeOfBloomFilterEntries: Int =
    asScala.sizeOfBloomFilterEntries

  def isEmpty: java.lang.Boolean =
    asScala.isEmpty

  def nonEmpty: java.lang.Boolean =
    asScala.nonEmpty

  def last: Optional[KeyVal[K, V]] =
    asScala.last.asJavaMap(KeyVal(_))

  def clearAppliedFunctions(): lang.Iterable[String] =
    asScala.clearAppliedFunctions().asJava

  def clearAppliedAndRegisteredFunctions(): lang.Iterable[String] =
    asScala.clearAppliedAndRegisteredFunctions().asJava

  def isFunctionApplied(function: F): java.lang.Boolean =
    asScala.isFunctionApplied(function)

  def asJava: util.Map[K, V] =
    asScala.asScala.asJava

  def close(): Unit =
    asScala.close()

  def delete(): Unit =
    asScala.delete()

  override def toString(): String =
    asScala.toString()

}
