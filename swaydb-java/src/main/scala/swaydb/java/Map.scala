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

import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.java.data.Stream
import swaydb.java.data.util.Javaz._
import swaydb.java.data.util.KeyVal
import swaydb.{IO, Prepare, Set}

import scala.compat.java8.DurationConverters._
import scala.compat.java8.OptionConverters._

/**
 * IOMap database API.
 *
 * For documentation check - http://swaydb.io/tag/
 */
case class Map[K, V, F](asScala: swaydb.Map[K, V, F, IO.ThrowableIO]) {

  def put(key: K, value: V): IO[Throwable, IO.Done] =
    asScala.put(key, value)

  def put(key: K, value: V, expireAfter: java.time.Duration): IO[Throwable, IO.Done] =
    asScala.put(key, value, expireAfter.toScala)

  def put(keyValues: KeyVal[K, V]*): IO[Throwable, IO.Done] =
    asScala.put(keyValues.map(_.toTuple))

  def put(keyValues: Stream[KeyVal[K, V]]): IO[Throwable, IO.Done] =
    asScala.put(keyValues.asScala.map(_.toTuple))

  def put(keyValues: Iterable[KeyVal[K, V]]): IO[Throwable, IO.Done] =
    asScala.put(keyValues.map(_.toTuple))

  def remove(key: K): IO[Throwable, IO.Done] =
    asScala.remove(key)

  def remove(from: K, to: K): IO[Throwable, IO.Done] =
    asScala.remove(from, to)

  def remove(keys: K*): IO[Throwable, IO.Done] =
    asScala.remove(keys)

  def remove(keys: Stream[K]): IO[Throwable, IO.Done] =
    asScala.remove(keys.asScala)

  def remove(keys: Iterable[K]): IO[Throwable, IO.Done] =
    asScala.remove(keys)

  def expire(key: K, after: java.time.Duration): IO[Throwable, IO.Done] =
    asScala.expire(key, after.toScala)

  def expire(from: K, to: K, after: java.time.Duration): IO[Throwable, IO.Done] =
    asScala.expire(from, to, after.toScala)

  def expire(keys: KeyVal[K, java.time.Duration]*): IO[Throwable, IO.Done] =
    asScala.expire(
      keys map {
        keyValue =>
          (keyValue.key, keyValue.value.toScala.fromNow)
      }
    )

  def expire(keys: Stream[KeyVal[K, java.time.Duration]]): IO[Throwable, IO.Done] =
    asScala.expire(keys.asScala.map(_.toScala))

  def expire(keys: Iterable[(K, java.time.Duration)]): IO[Throwable, IO.Done] =
    asScala.expire(keys.map(_.asScalaDeadline))

  def update(key: K, value: V): IO[Throwable, IO.Done] =
    asScala.update(key, value)

  def update(from: K, to: K, value: V): IO[Throwable, IO.Done] =
    asScala.update(from, to, value)

  def update(keyValues: KeyVal[K, V]*): IO[Throwable, IO.Done] =
    asScala.update(keyValues.map(_.toTuple))

  def update(keyValues: Stream[KeyVal[K, V]]): IO[Throwable, IO.Done] =
    asScala.update(keyValues.asScala.map(_.toTuple))

  def update(keyValues: Iterable[KeyVal[K, V]]): IO[Throwable, IO.Done] =
    asScala.update(keyValues.map(_.toTuple))

  def clear(): IO[Throwable, IO.Done] =
    asScala.clear()

  def registerFunction(function: F with swaydb.Function[K, V]): Unit =
    asScala.registerFunction(function)

  def applyFunction(key: K, function: F with swaydb.Function[K, V]): IO[Throwable, IO.Done] =
    asScala.applyFunction(key, function)

  def applyFunction(from: K, to: K, function: F with swaydb.Function[K, V]): IO[Throwable, IO.Done] =
    asScala.applyFunction(from, to, function)

  def commit(prepare: Prepare[K, V]*): IO[Throwable, IO.Done] =
    asScala.commit(prepare)

  def commit(prepare: Stream[Prepare[K, V]]): IO[Throwable, IO.Done] =
    asScala.commit(prepare.asScala)

  def commit(prepare: Iterable[Prepare[K, V]]): IO[Throwable, IO.Done] =
    asScala.commit(prepare)

  def get(key: K): IO.ThrowableIO[Optional[V]] =
    asScala.get(key).map(_.asJava)

  def getKey(key: K): IO.ThrowableIO[Optional[K]] =
    asScala.getKey(key).map(_.asJava)

  def getKeyValue(key: K): IO.ThrowableIO[Optional[KeyVal[K, V]]] =
    asScala.getKeyValue(key).map(_.map(KeyVal(_)).asJava)

  def contains(key: K): IO.ThrowableIO[Boolean] =
    asScala.contains(key)

  def mightContain(key: K): IO.ThrowableIO[Boolean] =
    asScala.mightContain(key)

  def mightContainFunction(functionId: K): IO.ThrowableIO[Boolean] =
    asScala.mightContainFunction(functionId)

  def keys: Set[K, F, IO.ThrowableIO] =
    asScala.keys

  def level0Meter: LevelZeroMeter =
    asScala.level0Meter

  def levelMeter(levelNumber: Int): Optional[LevelMeter] =
    asScala.levelMeter(levelNumber).asJava

  def sizeOfSegments: Long =
    asScala.sizeOfSegments

  def keySize(key: K): Int =
    asScala.keySize(key)

  def valueSize(value: V): Int =
    asScala.valueSize(value)

  def timeLeft(key: K): IO.ThrowableIO[Optional[java.time.Duration]] =
    asScala.timeLeft(key).map {
      case Some(duration) =>
        Optional.of(duration.toJava)

      case None =>
        Optional.empty()
    }

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

  def headOptional: IO.ThrowableIO[Optional[KeyVal[K, V]]] =
    asScala.headOption.map(_.map(KeyVal(_)).asJava)

  def drop(count: Int): Stream[KeyVal[K, V]] =
    new Stream(asScala.drop(count).map(_.asJava))

  def dropWhile(function: Predicate[KeyVal[K, V]]): Stream[KeyVal[K, V]] =
    Stream(
      asScala
        .map(_.asJava)
        .dropWhile(function.test)
    )

  def take(count: Int): Stream[KeyVal[K, V]] =
    Stream(asScala.take(count).map(_.asJava))

  def takeWhile(function: Predicate[KeyVal[K, V]]): Stream[KeyVal[K, V]] =
    Stream(
      asScala
        .map(_.asJava)
        .takeWhile(function.test)
    )

  def map[B](function: JavaFunction[KeyVal[K, V], B]): Stream[B] =
    Stream(
      asScala map {
        case (key: K, value: V) =>
          function.apply(KeyVal(key, value))
      }
    )

  def flatMap[B](function: JavaFunction[KeyVal[K, V], Stream[B]]): Stream[B] =
    Stream(
      asScala.flatMap {
        case (key: K, value: V) =>
          function.apply(KeyVal(key, value)).asScala
      }
    )

  def foreach(function: Consumer[KeyVal[K, V]]): Stream[Unit] =
    Stream(
      asScala foreach {
        case (key: K, value: V) =>
          function.accept(KeyVal(key, value))
      }
    )

  def filter(function: Predicate[KeyVal[K, V]]): Stream[KeyVal[K, V]] =
    Stream(
      asScala
        .map(_.asJava)
        .filter(function.test)
    )

  def filterNot(function: Predicate[KeyVal[K, V]]): Stream[KeyVal[K, V]] =
    Stream(
      asScala
        .map(_.asJava)
        .filterNot(function.test)
    )

  def foldLeft[B](initial: B)(function: BiFunction[B, KeyVal[K, V], B]): data.IO[Throwable, B] =
    stream.foldLeft(initial, function)

  def size: IO.ThrowableIO[Int] =
    asScala.size

  def stream: Stream[KeyVal[K, V]] =
    new Stream(asScala.stream.map(_.asJava))

  def sizeOfBloomFilterEntries: IO.ThrowableIO[Int] =
    asScala.sizeOfBloomFilterEntries

  def isEmpty: IO.ThrowableIO[Boolean] =
    asScala.isEmpty

  def nonEmpty: IO.ThrowableIO[Boolean] =
    asScala.nonEmpty

  def lastOptional: IO.ThrowableIO[Optional[KeyVal[K, V]]] =
    asScala.lastOption.map(_.map(KeyVal(_)).asJava)

  def reverse: Map[K, V, F] =
    copy(asScala.reverse)

  def close(): IO.ThrowableIO[Unit] =
    asScala.close()

  def delete(): IO.ThrowableIO[Unit] =
    asScala.delete()

  override def toString(): String =
    classOf[Map[_, _, _]].getClass.getSimpleName
}
