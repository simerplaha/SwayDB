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
 */

package swaydb.java

import java.time.Duration
import java.util.Optional

import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.java.data.util.Java._
import swaydb.{Apply, Bag}

import scala.collection.mutable.ListBuffer
import scala.compat.java8.DurationConverters._
import scala.jdk.CollectionConverters._
import scala.collection.compat._

/**
 * IOMap database API.
 *
 * For documentation check - http://swaydb.io/tag/
 */
case class Map[K, V, F <: swaydb.java.PureFunction[K, V, Return.Map[V]]](private val _asScala: swaydb.Map[K, V, _, Bag.Less]) {

  implicit val bag = Bag.less

  val asScala: swaydb.Map[K, V, swaydb.PureFunction[K, V, Apply.Map[V]], Bag.Less] =
    _asScala.asInstanceOf[swaydb.Map[K, V, swaydb.PureFunction[K, V, Apply.Map[V]], Bag.Less]]

  def put(key: K, value: V): swaydb.OK =
    asScala.put(key, value)

  def put(key: K, value: V, expireAfter: java.time.Duration): swaydb.OK =
    asScala.put(key, value, expireAfter.toScala)

  def put(keyValues: java.util.List[KeyVal[K, V]]): swaydb.OK =
    asScala.put(keyValues.asScala.map(_.toTuple))

  def put(keyValues: Stream[KeyVal[K, V]]): swaydb.OK =
    asScala.put(keyValues.asScala.map(_.toTuple))

  def put(keyValues: java.util.Iterator[KeyVal[K, V]]): swaydb.OK =
    asScala.put(keyValues.asScala.map(_.toTuple).to(Iterable))

  def remove(key: K): swaydb.OK =
    asScala.remove(key)

  def remove(from: K, to: K): swaydb.OK =
    asScala.remove(from, to)

  def remove(keys: java.util.List[K]): swaydb.OK =
    asScala.remove(keys.asScala)

  def remove(keys: Stream[K]): swaydb.OK =
    asScala.remove(keys.asScala)

  def remove(keys: java.util.Iterator[K]): swaydb.OK =
    asScala.remove(keys.asScala.to(Iterable))

  def expire(key: K, after: java.time.Duration): swaydb.OK =
    asScala.expire(key, after.toScala)

  def expire(from: K, to: K, after: java.time.Duration): swaydb.OK =
    asScala.expire(from, to, after.toScala)

  def expire(keys: java.util.List[Pair[K, java.time.Duration]]): swaydb.OK =
    asScala.expire(
      keys.asScala map {
        keyValue =>
          (keyValue.left, keyValue.right.toScala.fromNow)
      }
    )

  def expire(keys: Stream[Pair[K, java.time.Duration]]): swaydb.OK =
    asScala.expire(keys.asScala.map(_.toScala))

  def expire(keys: java.util.Iterator[Pair[K, java.time.Duration]]): swaydb.OK =
    asScala.expire(keys.asScala.map(_.asScalaDeadline).to(Iterable))

  def expiration(key: K): Optional[Deadline] =
    asScala.expiration(key).asJavaMap(_.asJava)

  def update(key: K, value: V): swaydb.OK =
    asScala.update(key, value)

  def update(from: K, to: K, value: V): swaydb.OK =
    asScala.update(from, to, value)

  def update(keyValues: java.util.List[KeyVal[K, V]]): swaydb.OK =
    asScala.update(keyValues.asScala.map(_.toTuple))

  def update(keyValues: Stream[KeyVal[K, V]]): swaydb.OK =
    asScala.update(keyValues.asScala.map(_.toTuple))

  def update(keyValues: java.util.Iterator[KeyVal[K, V]]): swaydb.OK =
    asScala.update(keyValues.asScala.map(_.toTuple).to(Iterable))

  def clear(): swaydb.OK =
    asScala.clear()

  def registerFunction(function: F): swaydb.OK =
    asScala.registerFunction(PureFunction.asScala(function))

  def applyFunction(key: K, function: F): swaydb.OK =
    asScala.applyFunction(key, PureFunction.asScala(function))

  def applyFunction(from: K, to: K, function: F): swaydb.OK =
    asScala.applyFunction(from, to, PureFunction.asScala(function))

  def commit[P <: Prepare.Map[K, V, F]](prepare: java.util.List[P]): swaydb.OK =
    commit[P](prepare.iterator())

  def commit[P <: Prepare.Map[K, V, F]](prepare: Stream[P]): swaydb.OK =
    asScala.commit {
      prepare
        .asScala
        .foldLeft(ListBuffer.empty[swaydb.Prepare[K, V, swaydb.PureFunction[K, V, Apply.Map[V]]]])(_ += Prepare.toScala(_))
    }

  def commit[P <: Prepare.Map[K, V, F]](prepare: java.util.Iterator[P]): swaydb.OK = {
    val prepareStatements =
      prepare
        .asScala
        .foldLeft(ListBuffer.empty[swaydb.Prepare[K, V, swaydb.PureFunction[K, V, Apply.Map[V]]]])(_ += Prepare.toScala(_))

    asScala commit prepareStatements
  }

  def get(key: K): Optional[V] =
    asScala.get(key).asJava

  def getKey(key: K): Optional[K] =
    asScala.getKey(key).asJava

  def getKeyValue(key: K): Optional[KeyVal[K, V]] =
    asScala.getKeyValue(key).asJavaMap(KeyVal(_))

  def contains(key: K): java.lang.Boolean =
    asScala.contains(key)

  def mightContain(key: K): java.lang.Boolean =
    asScala.mightContain(key)

  def mightContainFunction(functionId: K): java.lang.Boolean =
    asScala.mightContainFunction(functionId)

  def keys: Set[K, PureFunction.VoidS[K]] =
    Set(asScala.keys)

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

  def from(key: K): Map[K, V, F] =
    copy(asScala.from(key))

  def before(key: K): Map[K, V, F] =
    copy(asScala.before(key))

  def fromOrBefore(key: K): Map[K, V, F] =
    copy(asScala.fromOrBefore(key))

  def after(key: K): Map[K, V, F] =
    copy(asScala.after(key))

  def fromOrAfter(key: K): Map[K, V, F] =
    copy(asScala.fromOrAfter(key))

  def headOptional: Optional[KeyVal[K, V]] =
    asScala.headOption.asJavaMap(KeyVal(_))

  def stream: Stream[KeyVal[K, V]] =
    new Stream(asScala.stream.map(_.asKeyVal))

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

  def reverse: Map[K, V, F] =
    copy(asScala.reverse)

  def close(): Unit =
    asScala.close()

  def delete(): Unit =
    asScala.delete()

  override def toString(): String =
    classOf[Map[_, _, _]].getClass.getSimpleName
}
