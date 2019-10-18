/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

import java.util.Optional
import java.util.function.{BiFunction, Consumer, Predicate}

import swaydb.IO.ThrowableIO
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.java.ScalaMapToJavaMapOutputConverter._
import swaydb.java.data.util.Java._
import swaydb.{Apply, Prepare}

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import scala.compat.java8.DurationConverters._

/**
 * IOMap database API.
 *
 * For documentation check - http://swaydb.io/tag/
 */
case class MapIO[K, V, F <: swaydb.java.PureFunction[K, V, Return.Map[V]]](_asScala: swaydb.Map[K, V, _, swaydb.IO.ThrowableIO]) {

  implicit val exceptionHandler = swaydb.IO.ExceptionHandler.Throwable

  private val asScala: swaydb.Map[K, V, swaydb.PureFunction[K, V, Apply.Map[V]], ThrowableIO] =
    _asScala.asInstanceOf[swaydb.Map[K, V, swaydb.PureFunction[K, V, Apply.Map[V]], swaydb.IO.ThrowableIO]]

  def put(key: K, value: V): IO[scala.Throwable, swaydb.Done] =
    asScala.put(key, value)

  def put(key: K, value: V, expireAfter: java.time.Duration): IO[scala.Throwable, swaydb.Done] =
    asScala.put(key, value, expireAfter.toScala)

  def put(keyValues: java.util.List[KeyVal[K, V]]): IO[scala.Throwable, swaydb.Done] =
    asScala.put(keyValues.asScala.map(_.toTuple))

  def put(keyValues: StreamIO[KeyVal[K, V]]): IO[scala.Throwable, swaydb.Done] =
    asScala.put(keyValues.asScala.map(_.toTuple))

  def put(keyValues: java.util.Iterator[KeyVal[K, V]]): IO[scala.Throwable, swaydb.Done] =
    asScala.put(keyValues.asScala.map(_.toTuple).toIterable)

  def remove(key: K): IO[scala.Throwable, swaydb.Done] =
    asScala.remove(key)

  def remove(from: K, to: K): IO[scala.Throwable, swaydb.Done] =
    asScala.remove(from, to)

  def remove(keys: java.util.List[K]): IO[scala.Throwable, swaydb.Done] =
    asScala.remove(keys.asScala)

  def remove(keys: StreamIO[K]): IO[scala.Throwable, swaydb.Done] =
    asScala.remove(keys.asScala)

  def remove(keys: java.util.Iterator[K]): IO[scala.Throwable, swaydb.Done] =
    asScala.remove(keys.asScala.toIterable)

  def expire(key: K, after: java.time.Duration): IO[scala.Throwable, swaydb.Done] =
    asScala.expire(key, after.toScala)

  def expire(from: K, to: K, after: java.time.Duration): IO[scala.Throwable, swaydb.Done] =
    asScala.expire(from, to, after.toScala)

  def expire(keys: java.util.List[Pair[K, java.time.Duration]]): IO[scala.Throwable, swaydb.Done] =
    asScala.expire(
      keys.asScala map {
        keyValue =>
          (keyValue.left, keyValue.right.toScala.fromNow)
      }
    )

  def expire(keys: StreamIO[Pair[K, java.time.Duration]]): IO[scala.Throwable, swaydb.Done] =
    asScala.expire(keys.asScala.map(_.toScala))

  def expire(keys: java.util.Iterator[Pair[K, java.time.Duration]]): IO[scala.Throwable, swaydb.Done] =
    asScala.expire(keys.asScala.map(_.asScalaDeadline).toIterable)

  def expiration(key: K): IO[Throwable, Optional[Deadline]] =
    asScala.expiration(key).transform(_.asJavaMap(_.asJava))

  def update(key: K, value: V): IO[scala.Throwable, swaydb.Done] =
    asScala.update(key, value)

  def update(from: K, to: K, value: V): IO[scala.Throwable, swaydb.Done] =
    asScala.update(from, to, value)

  def update(keyValues: java.util.List[KeyVal[K, V]]): IO[scala.Throwable, swaydb.Done] =
    asScala.update(keyValues.asScala.map(_.toTuple))

  def update(keyValues: StreamIO[KeyVal[K, V]]): IO[scala.Throwable, swaydb.Done] =
    asScala.update(keyValues.asScala.map(_.toTuple))

  def update(keyValues: java.util.Iterator[KeyVal[K, V]]): IO[scala.Throwable, swaydb.Done] =
    asScala.update(keyValues.asScala.map(_.toTuple).toIterable)

  def clear(): IO[scala.Throwable, swaydb.Done] =
    asScala.clear()

  def registerFunction(function: F): IO[scala.Throwable, swaydb.Done] =
    asScala.registerFunction(PureFunction.asScala(function))

  def applyFunction(key: K, function: F): IO[scala.Throwable, swaydb.Done] =
    asScala.applyFunction(key, PureFunction.asScala(function))

  def applyFunction(from: K, to: K, function: F): IO[scala.Throwable, swaydb.Done] =
    asScala.applyFunction(from, to, PureFunction.asScala(function))

  def commit[P <: Prepare.Map[K, V, F]](prepare: java.util.List[P]): IO[scala.Throwable, swaydb.Done] =
    commit[P](prepare.iterator())

  def commit[P <: Prepare.Map[K, V, F]](prepare: StreamIO[P]): IO[scala.Throwable, swaydb.Done] =
    prepare
      .asScala
      .foldLeft(ListBuffer.empty[Prepare[K, V, swaydb.PureFunction[K, V, Apply.Map[V]]]])(_ += Prepare.toScala(_))
      .flatMap {
        statements =>
          asScala commit statements
      }

  def commit[P <: Prepare.Map[K, V, F]](prepare: java.util.Iterator[P]): IO[scala.Throwable, swaydb.Done] = {
    val prepareStatements =
      prepare
        .asScala
        .foldLeft(ListBuffer.empty[Prepare[K, V, swaydb.PureFunction[K, V, Apply.Map[V]]]])(_ += Prepare.toScala(_))

    asScala commit prepareStatements
  }

  def get(key: K): IO[scala.Throwable, Optional[V]] =
    asScala.get(key).transform(_.asJava)

  def getKey(key: K): IO[scala.Throwable, Optional[K]] =
    asScala.getKey(key).transform(_.asJava)

  def getKeyValue(key: K): IO[scala.Throwable, Optional[KeyVal[K, V]]] =
    asScala.getKeyValue(key).transform(_.asJavaMap(KeyVal(_)))

  def contains(key: K): IO[scala.Throwable, java.lang.Boolean] =
    asScala.contains(key)

  def mightContain(key: K): IO[scala.Throwable, java.lang.Boolean] =
    asScala.mightContain(key)

  def mightContainFunction(functionId: K): IO[scala.Throwable, java.lang.Boolean] =
    asScala.mightContainFunction(functionId)

  def keys: SetIO[K, PureFunction.VoidS[K]] =
    SetIO(asScala.keys)

  def level0Meter: LevelZeroMeter =
    asScala.levelZeroMeter

  def levelMeter(levelNumber: Integer): Optional[LevelMeter] =
    asScala.levelMeter(levelNumber).asJava

  def sizeOfSegments: Long =
    asScala.sizeOfSegments

  def keySize(key: K): Integer =
    asScala.keySize(key)

  def valueSize(value: V): Integer =
    asScala.valueSize(value)

  def timeLeft(key: K): IO[scala.Throwable, Optional[java.time.Duration]] =
    new IO(asScala.timeLeft(key).transform(_.asJavaMap(_.toJava)))

  def from(key: K): MapIO[K, V, F] =
    copy(asScala.from(key))

  def before(key: K): MapIO[K, V, F] =
    copy(asScala.before(key))

  def fromOrBefore(key: K): MapIO[K, V, F] =
    copy(asScala.fromOrBefore(key))

  def after(key: K): MapIO[K, V, F] =
    copy(asScala.after(key))

  def fromOrAfter(key: K): MapIO[K, V, F] =
    copy(asScala.fromOrAfter(key))

  def headOptional: IO[scala.Throwable, Optional[KeyVal[K, V]]] =
    asScala.headOption.transform(_.asJavaMap(KeyVal(_)))

  def drop(count: Integer): StreamIO[KeyVal[K, V]] =
    new StreamIO(asScala.drop(count).map(_.asKeyVal))

  def dropWhile(function: Predicate[KeyVal[K, V]]): StreamIO[KeyVal[K, V]] =
    Stream.fromScala(
      asScala
        .map(_.asKeyVal)
        .dropWhile(function.test)
    )

  def take(count: Integer): StreamIO[KeyVal[K, V]] =
    Stream.fromScala(asScala.take(count).map(_.asKeyVal))

  def takeWhile(function: Predicate[KeyVal[K, V]]): StreamIO[KeyVal[K, V]] =
    Stream.fromScala(
      asScala
        .map(_.asKeyVal)
        .takeWhile(function.test)
    )

  def map[B](function: JavaFunction[KeyVal[K, V], B]): StreamIO[B] =
    Stream.fromScala(
      asScala map {
        case (key: K, value: V) =>
          function.apply(KeyVal(key, value))
      }
    )

  def flatMap[B](function: JavaFunction[KeyVal[K, V], StreamIO[B]]): StreamIO[B] =
    Stream.fromScala(
      asScala.flatMap {
        case (key: K, value: V) =>
          function.apply(KeyVal(key, value)).asScala
      }
    )

  def forEach(function: Consumer[KeyVal[K, V]]): StreamIO[Unit] =
    Stream.fromScala(
      asScala foreach {
        case (key: K, value: V) =>
          function.accept(KeyVal(key, value))
      }
    )

  def filter(function: Predicate[KeyVal[K, V]]): StreamIO[KeyVal[K, V]] =
    Stream.fromScala(
      asScala
        .map(_.asKeyVal)
        .filter(function.test)
    )

  def filterNot(function: Predicate[KeyVal[K, V]]): StreamIO[KeyVal[K, V]] =
    Stream.fromScala(
      asScala
        .map(_.asKeyVal)
        .filterNot(function.test)
    )

  def foldLeft[B](initial: B)(function: BiFunction[B, KeyVal[K, V], B]): IO[scala.Throwable, B] =
    stream.foldLeft(initial, function)

  def size: IO[scala.Throwable, Integer] =
    asScala.size

  def stream: StreamIO[KeyVal[K, V]] =
    new StreamIO(asScala.stream.map(_.asKeyVal))

  def sizeOfBloomFilterEntries: IO[scala.Throwable, Integer] =
    asScala.sizeOfBloomFilterEntries

  def isEmpty: IO[scala.Throwable, java.lang.Boolean] =
    asScala.isEmpty

  def nonEmpty: IO[scala.Throwable, java.lang.Boolean] =
    asScala.nonEmpty

  def lastOptional: IO[scala.Throwable, Optional[KeyVal[K, V]]] =
    asScala.lastOption.transform(_.asJavaMap(KeyVal(_)))

  def reverse: MapIO[K, V, F] =
    copy(asScala.reverse)

  def close(): IO[scala.Throwable, Unit] =
    asScala.close()

  def delete(): IO[scala.Throwable, Unit] =
    asScala.delete()

  override def toString(): String =
    classOf[MapIO[_, _, _]].getClass.getSimpleName
}
