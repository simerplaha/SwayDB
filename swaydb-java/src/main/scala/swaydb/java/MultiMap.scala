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
import swaydb.data.slice.Slice
import swaydb.data.util.Java._
import swaydb.java.data.util.Java._
import swaydb.java.multimap.Schema
import swaydb.multimap.Transaction
import swaydb.{Apply, Bag, KeyVal, Pair}

import scala.compat.java8.DurationConverters._
import scala.concurrent.duration
import scala.jdk.CollectionConverters._

/**
 * Documentation - http://swaydb.io/
 */
case class MultiMap[M, K, V, F](private val _asScala: swaydb.MultiMap[M, K, V, _, Bag.Less]) {

  val asScala: swaydb.MultiMap[M, K, V, swaydb.PureFunction[K, V, Apply.Map[V]], Bag.Less] =
    _asScala.asInstanceOf[swaydb.MultiMap[M, K, V, swaydb.PureFunction[K, V, Apply.Map[V]], Bag.Less]]

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

  def expiration(key: K): Optional[Deadline] =
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
    asScala.applyFunction(key, PureFunction.asScala(function.asInstanceOf[swaydb.java.PureFunction[K, V, Return.Map[V]]]))

  def applyFunction(from: K, to: K, function: F): swaydb.OK =
    asScala.applyFunction(from, to, PureFunction.asScala(function.asInstanceOf[swaydb.java.PureFunction[K, V, Return.Map[V]]]))

  def toTransaction[P <: Prepare.Map[K, V, F]](prepare: java.lang.Iterable[P]): java.lang.Iterable[Transaction[M, K, V, F]] =
    asScala
      .toTransaction(Prepare.toScalaFromIterable(prepare.asScala))
      .asJava
      .asInstanceOf[lang.Iterable[Transaction[M, K, V, F]]]

  def commit[P <: Prepare.Map[K, V, F]](prepare: java.lang.Iterable[P]): swaydb.OK =
    commit[P](prepare.iterator())

  def commit[P <: Prepare.Map[K, V, F]](prepare: Stream[P]): swaydb.OK =
    asScala.commit(Prepare.toScalaFromStream[K, V, F](prepare.asInstanceOf[Stream[Prepare.Map[K, V, F]]]))

  def commit[P <: Prepare.Map[K, V, F]](prepare: java.util.Iterator[P]): swaydb.OK =
    asScala commit Prepare.toScalaFromIterator(prepare.asScala)

  def get(key: K): Optional[V] =
    asScala.get(key).asJava

  def getKey(key: K): Optional[K] =
    asScala.getKey(key).asJava

  def getKeyValue(key: K): Optional[KeyVal[K, V]] =
    asScala.getKeyValue(key).asJavaMap(KeyVal(_))

  def getKeyDeadline(key: K): Optional[Pair[K, Optional[Deadline]]] =
    asScala.getKeyDeadline(key).map {
      case (key, deadline) =>
        Pair(key, deadline.asJava)
    }.asJava

  def getKeyValueDeadline(key: K): Optional[Pair[Pair[K, V], Optional[Deadline]]] =
    (asScala.getKeyValueDeadline(key, Bag.less): Option[((K, V), Option[duration.Deadline])]) match {
      case Some(((key, value), deadline)) =>
        Optional.of(Pair(Pair(key, value), deadline.asJava))

      case None =>
        Optional.empty()
    }

  def contains(key: K): java.lang.Boolean =
    asScala.contains(key)

  def mightContain(key: K): java.lang.Boolean =
    asScala.mightContain(key)

  def mightContainFunction(function: F): java.lang.Boolean = {
    val functionId = function.asInstanceOf[swaydb.java.PureFunction[K, V, Return.Map[V]]].id
    val functionBytes = Slice.writeString(functionId)
    asScala.innerMap.core.mightContainFunction(functionBytes)
  }

  def level0Meter: LevelZeroMeter =
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

  def headOptional: Optional[KeyVal[K, V]] =
    asScala.headOption.asJavaMap(KeyVal(_))

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

  def lastOptional: Optional[KeyVal[K, V]] =
    asScala.lastOption.asJavaMap(KeyVal(_))

  def clearAppliedFunctions(): lang.Iterable[String] =
    asScala.clearAppliedFunctions().asJava

  def clearAppliedAndRegisteredFunctions(): lang.Iterable[String] =
    asScala.clearAppliedAndRegisteredFunctions().asJava

  def isFunctionApplied(function: F): Boolean =
    asScala.isFunctionApplied(PureFunction.asScala(function.asInstanceOf[swaydb.java.PureFunction[K, V, Return.Map[V]]]))

  def asJava: util.Map[K, V] =
    asScala.asScala.asJava

  def close(): Unit =
    asScala.close()

  def delete(): Unit =
    asScala.delete()

  private def copy(): Unit = ()

  override def toString(): String =
    asScala.toString()
}
