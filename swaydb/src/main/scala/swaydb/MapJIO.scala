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

package swaydb

import java.util.Optional
import java.util.function.{BiFunction, Consumer, Predicate}

import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.util.Javaz._
import swaydb.data.util.KeyVal

import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * IOMap database API.
 *
 * For documentation check - http://swaydb.io/tag/
 */
case class MapJIO[K, V, F](map: Map[K, V, F, IO.ThrowableIO]) { self =>

  def put(key: K, value: V): IO[Throwable, IO.Done] =
    map.put(key, value)

  def put(key: K, value: V, expireAfter: FiniteDuration): IO[Throwable, IO.Done] =
    map.put(key, value, expireAfter)

  def put(key: K, value: V, expireAt: Deadline): IO[Throwable, IO.Done] =
    map.put(key, value, expireAt)

  def put(keyValues: (K, V)*): IO[Throwable, IO.Done] =
    map.put(keyValues)

  def put(keyValues: StreamJIO[(K, V)]): IO[Throwable, IO.Done] =
    map.put(keyValues.stream)

  def put(keyValues: Iterable[(K, V)]): IO[Throwable, IO.Done] =
    map.put(keyValues)

  def remove(key: K): IO[Throwable, IO.Done] =
    map.remove(key)

  def remove(from: K, to: K): IO[Throwable, IO.Done] =
    map.remove(from, to)

  def remove(keys: K*): IO[Throwable, IO.Done] =
    map.remove(keys)

  def remove(keys: StreamJIO[K]): IO[Throwable, IO.Done] =
    map.remove(keys.stream)

  def remove(keys: Iterable[K]): IO[Throwable, IO.Done] =
    map.remove(keys)

  def expire(key: K, after: FiniteDuration): IO[Throwable, IO.Done] =
    map.expire(key, after)

  def expire(key: K, at: Deadline): IO[Throwable, IO.Done] =
    map.expire(key, at)

  def expire(from: K, to: K, after: FiniteDuration): IO[Throwable, IO.Done] =
    map.expire(from, to, after)

  def expire(from: K, to: K, at: Deadline): IO[Throwable, IO.Done] =
    map.expire(from, to, at)

  def expire(keys: (K, Deadline)*): IO[Throwable, IO.Done] =
    map.expire(keys)

  def expire(keys: StreamJIO[(K, Deadline)]): IO[Throwable, IO.Done] =
    map.expire(keys.stream)

  def expire(keys: Iterable[(K, Deadline)]): IO[Throwable, IO.Done] =
    map.expire(keys)

  def update(key: K, value: V): IO[Throwable, IO.Done] =
    map.update(key, value)

  def update(from: K, to: K, value: V): IO[Throwable, IO.Done] =
    map.update(from, to, value)

  def update(keyValues: (K, V)*): IO[Throwable, IO.Done] =
    map.update(keyValues)

  def update(keyValues: StreamJIO[(K, V)]): IO[Throwable, IO.Done] =
    map.update(keyValues.stream)

  def update(keyValues: Iterable[(K, V)]): IO[Throwable, IO.Done] =
    map.update(keyValues)

  def clear(): IO[Throwable, IO.Done] =
    map.clear()

  def registerFunction(function: F with swaydb.Function[K, V]): Unit =
    map.registerFunction(function)

  def applyFunction(key: K, function: F with swaydb.Function[K, V]): IO[Throwable, IO.Done] =
    map.applyFunction(key, function)

  def applyFunction(from: K, to: K, function: F with swaydb.Function[K, V]): IO[Throwable, IO.Done] =
    map.applyFunction(from, to, function)

  def commit(prepare: Prepare[K, V]*): IO[Throwable, IO.Done] =
    map.commit(prepare)

  def commit(prepare: StreamJIO[Prepare[K, V]]): IO[Throwable, IO.Done] =
    map.commit(prepare.stream)

  def commit(prepare: Iterable[Prepare[K, V]]): IO[Throwable, IO.Done] =
    map.commit(prepare)

  /**
   * Returns target value for the input key.
   */
  def get(key: K): IO.ThrowableIO[Optional[V]] =
    map.get(key).map(_.asJava)

  /**
   * Returns target full key for the input partial key.
   *
   * This function is mostly used for Set databases where partial ordering on the Key is provided.
   */
  def getKey(key: K): IO.ThrowableIO[Optional[K]] =
    map.getKey(key).map(_.asJava)

  def getKeyValue(key: K): IO.ThrowableIO[Optional[(K, V)]] =
    map.getKeyValue(key).map(_.asJava)

  def contains(key: K): IO.ThrowableIO[Boolean] =
    map.contains(key)

  def mightContain(key: K): IO.ThrowableIO[Boolean] =
    map.mightContain(key)

  def mightContainFunction(functionId: K): IO.ThrowableIO[Boolean] =
    map.mightContainFunction(functionId)

  def keys: Set[K, F, IO.ThrowableIO] =
    map.keys

  def level0Meter: LevelZeroMeter =
    map.level0Meter

  def levelMeter(levelNumber: Int): Optional[LevelMeter] =
    map.levelMeter(levelNumber).asJava

  def sizeOfSegments: Long =
    map.sizeOfSegments

  def keySize(key: K): Int =
    map.keySize(key)

  def valueSize(value: V): Int =
    map.valueSize(value)

  def expiration(key: K): IO.ThrowableIO[Optional[Deadline]] =
    map.expiration(key).map(_.asJava)

  def timeLeft(key: K): IO.ThrowableIO[Optional[FiniteDuration]] =
    map.timeLeft(key).map(_.asJava)

  def from(key: K): MapJIO[K, V, F] =
    copy(map.from(key))

  def before(key: K): MapJIO[K, V, F] =
    copy(map.before(key))

  def fromOrBefore(key: K): MapJIO[K, V, F] =
    copy(map.fromOrBefore(key))

  def after(key: K): MapJIO[K, V, F] =
    copy(map.after(key))

  def fromOrAfter(key: K): MapJIO[K, V, F] =
    copy(map.fromOrAfter(key))

  def headOptional: IO.ThrowableIO[Optional[(K, V)]] =
    map.headOption.map(_.asJava)

  def drop(count: Int): StreamJIO[(K, V)] =
    new StreamJIO(map.drop(count))

  def dropWhile(function: Predicate[KeyVal[K, V]]): StreamJIO[(K, V)] =
    StreamJIO(map.dropWhile {
      case (key: K, value: V) =>
        function.test(KeyVal(key, value))
    })

  def take(count: Int): StreamJIO[(K, V)] =
    StreamJIO(map.take(count))

  def takeWhile(function: Predicate[KeyVal[K, V]]): StreamJIO[(K, V)] =
    StreamJIO(map.takeWhile {
      case (key: K, value: V) =>
        function.test(KeyVal(key, value))
    })

  def map[B](function: JavaFunction[KeyVal[K, V], B]): StreamJIO[B] =
    StreamJIO(map.map {
      case (key: K, value: V) =>
        function.apply(KeyVal(key, value))
    })

  def flatMap[B](function: JavaFunction[KeyVal[K, V], StreamJIO[B]]): StreamJIO[B] =
    StreamJIO(map.flatMap {
      case (key: K, value: V) =>
        function.apply(KeyVal(key, value)).stream
    })

  def foreach(function: Consumer[KeyVal[K, V]]): StreamJIO[Unit] =
    StreamJIO(map.foreach {
      case (key: K, value: V) =>
        function.accept(KeyVal(key, value))
    })

  def filter(function: Predicate[KeyVal[K, V]]): StreamJIO[(K, V)] =
    StreamJIO(map.filter {
      case (key: K, value: V) =>
        function.test(KeyVal(key, value))
    })

  def filterNot(function: Predicate[KeyVal[K, V]]): StreamJIO[(K, V)] =
    StreamJIO(map.filterNot {
      case (key: K, value: V) =>
        function.test(KeyVal(key, value))
    })

  def foldLeft[B](initial: B)(function: BiFunction[B, KeyVal[K, V], B]): IO.ThrowableIO[B] =
    stream.foldLeft(initial, function)

  def size: IO.ThrowableIO[Int] =
    map.size

  def stream: StreamJIO[KeyVal[K, V]] =
    new StreamJIO(map.stream.map(_.asJava))

  def sizeOfBloomFilterEntries: IO.ThrowableIO[Int] =
    map.sizeOfBloomFilterEntries

  def isEmpty: IO.ThrowableIO[Boolean] =
    map.isEmpty

  def nonEmpty: IO.ThrowableIO[Boolean] =
    map.nonEmpty

  def lastOptional: IO.ThrowableIO[Optional[(K, V)]] =
    map.lastOption.map(_.asJava)

  def reverse: MapJIO[K, V, F] =
    copy(map.reverse)

  def close(): IO.ThrowableIO[Unit] =
    map.close()

  def delete(): IO.ThrowableIO[Unit] =
    map.delete()

  override def toString(): String =
    classOf[MapJIO[_, _, _]].getClass.getSimpleName
}