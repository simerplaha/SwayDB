/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

import swaydb.PrepareImplicits._
import swaydb.core.Core
import swaydb.core.segment.ThreadReadState
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.serializers.{Serializer, _}

import scala.concurrent.duration.{Deadline, FiniteDuration}

/**
 * Map database API.
 *
 * For documentation check - http://swaydb.io/tag/
 */
case class Map[K, V, F, T[_]](private[swaydb] val core: Core[T],
                              private val from: Option[From[K]] = None,
                              private[swaydb] val reverseIteration: Boolean = false)(implicit keySerializer: Serializer[K],
                                                                                     valueSerializer: Serializer[V],
                                                                                     tag: Tag[T]) extends Streamable[(K, V), T] { self =>

  def put(key: K, value: V): T[OK] =
    tag.point(core.put(key = key, value = Some(value)))

  def put(key: K, value: V, expireAfter: FiniteDuration): T[OK] =
    tag.point(core.put(key, Some(value), expireAfter.fromNow))

  def put(key: K, value: V, expireAt: Deadline): T[OK] =
    tag.point(core.put(key, Some(value), expireAt))

  def put(keyValues: (K, V)*): T[OK] =
    tag.point(put(keyValues))

  def put(keyValues: Stream[(K, V), T]): T[OK] =
    tag.flatMap(keyValues.materialize)(put)

  def put(keyValues: Iterable[(K, V)]): T[OK] =
    put(keyValues.iterator)

  def put(keyValues: Iterator[(K, V)]): T[OK] =
    tag.point {
      core.put {
        keyValues map {
          case (key, value) =>
            Prepare.Put(keySerializer.write(key), Some(valueSerializer.write(value)), None)
        }
      }
    }

  def remove(key: K): T[OK] =
    tag.point(core.remove(key))

  def remove(from: K, to: K): T[OK] =
    tag.point(core.remove(from, to))

  def remove(keys: K*): T[OK] =
    tag.point(remove(keys))

  def remove(keys: Stream[K, T]): T[OK] =
    tag.flatMap(keys.materialize)(remove)

  def remove(keys: Iterable[K]): T[OK] =
    remove(keys.iterator)

  def remove(keys: Iterator[K]): T[OK] =
    tag.point(core.put(keys.map(key => Prepare.Remove(keySerializer.write(key)))))

  def expire(key: K, after: FiniteDuration): T[OK] =
    tag.point(core.remove(key, after.fromNow))

  def expire(key: K, at: Deadline): T[OK] =
    tag.point(core.remove(key, at))

  def expire(from: K, to: K, after: FiniteDuration): T[OK] =
    tag.point(core.remove(from, to, after.fromNow))

  def expire(from: K, to: K, at: Deadline): T[OK] =
    tag.point(core.remove(from, to, at))

  def expire(keys: (K, Deadline)*): T[OK] =
    tag.point(expire(keys))

  def expire(keys: Stream[(K, Deadline), T]): T[OK] =
    tag.flatMap(keys.materialize)(expire)

  def expire(keys: Iterable[(K, Deadline)]): T[OK] =
    expire(keys.iterator)

  def expire(keys: Iterator[(K, Deadline)]): T[OK] =
    tag.point {
      core.put {
        keys map {
          keyDeadline =>
            Prepare.Remove(
              from = keySerializer.write(keyDeadline._1),
              to = None,
              deadline = Some(keyDeadline._2)
            )
        }
      }
    }

  def update(key: K, value: V): T[OK] =
    tag.point(core.update(key, Some(value)))

  def update(from: K, to: K, value: V): T[OK] =
    tag.point(core.update(from, to, Some(value)))

  def update(keyValues: (K, V)*): T[OK] =
    tag.point(update(keyValues))

  def update(keyValues: Stream[(K, V), T]): T[OK] =
    tag.flatMap(keyValues.materialize)(update)

  def update(keyValues: Iterable[(K, V)]): T[OK] =
    update(keyValues.iterator)

  def update(keyValues: Iterator[(K, V)]): T[OK] =
    tag.point {
      core.put {
        keyValues map {
          case (key, value) =>
            Prepare.Update(keySerializer.write(key), Some(valueSerializer.write(value)))
        }
      }
    }

  def clear(): T[OK] =
    tag.point(core.clear(core.readStates.get()))

  def registerFunction[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): T[OK] =
    (function: swaydb.PureFunction[K, V, Apply.Map[V]]) match {
      case function: swaydb.PureFunction.OnValue[V, Apply.Map[V]] =>
        core.registerFunction(function.id, SwayDB.toCoreFunction(function))

      case function: swaydb.PureFunction.OnKey[K, V, Apply.Map[V]] =>
        core.registerFunction(function.id, SwayDB.toCoreFunction(function))

      case function: swaydb.PureFunction.OnKeyValue[K, V, Apply.Map[V]] =>
        core.registerFunction(function.id, SwayDB.toCoreFunction(function))
    }

  def applyFunction[PF <: F](key: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): T[OK] =
    tag.point(core.function(key, function.id))

  def applyFunction[PF <: F](from: K, to: K, function: PF)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): T[OK] =
    tag.point(core.function(from, to, function.id))

  def commit[PF <: F](prepare: Prepare[K, V, PF]*)(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): T[OK] =
    tag.point(core.put(preparesToUntyped(prepare).iterator))

  def commit[PF <: F](prepare: Stream[Prepare[K, V, PF], T])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): T[OK] =
    tag.flatMap(prepare.materialize) {
      prepares =>
        commit(prepares)
    }

  def commit[PF <: F](prepare: Iterable[Prepare[K, V, PF]])(implicit ev: PF <:< swaydb.PureFunction[K, V, Apply.Map[V]]): T[OK] =
    tag.point(core.put(preparesToUntyped(prepare).iterator))

  /**
   * Returns target value for the input key.
   */
  def get(key: K): T[Option[V]] =
    tag.map(core.get(key, core.readStates.get()))(_.map(_.read[V]))

  /**
   * Returns target full key for the input partial key.
   *
   * This function is mostly used for Set databases where partial ordering on the Key is provided.
   */
  def getKey(key: K): T[Option[K]] =
    tag.map(core.getKey(key, core.readStates.get()))(_.map(_.read[K]))

  def getKeyValue(key: K): T[Option[(K, V)]] =
    tag.map(core.getKeyValue(key, core.readStates.get()))(_.map {
      case (key, value) =>
        (key.read[K], value.read[V])
    })

  def contains(key: K): T[Boolean] =
    tag.point(core.contains(key, core.readStates.get()))

  def mightContain(key: K): T[Boolean] =
    tag.point(core mightContainKey key)

  def mightContainFunction(functionId: K): T[Boolean] =
    tag.point(core mightContainFunction functionId)

  def keys: Set[K, F, T] =
    Set[K, F, T](
      core = core,
      from = from,
      reverseIteration = reverseIteration
    )(keySerializer, tag)

  def levelZeroMeter: LevelZeroMeter =
    core.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    core.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    core.sizeOfSegments

  def keySize(key: K): Int =
    (key: Slice[Byte]).size

  def valueSize(value: V): Int =
    (value: Slice[Byte]).size

  def expiration(key: K): T[Option[Deadline]] =
    tag.point(core.deadline(key, core.readStates.get()))

  def timeLeft(key: K): T[Option[FiniteDuration]] =
    tag.map(expiration(key))(_.map(_.timeLeft))

  def from(key: K): Map[K, V, F, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: K): Map[K, V, F, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: K): Map[K, V, F, T] =
    copy(from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: K): Map[K, V, F, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: K): Map[K, V, F, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)))

  def headOption: T[Option[(K, V)]] =
    headOption(core.readStates.get())

  protected def headOption(readState: ThreadReadState): T[Option[(K, V)]] =
    tag.map {
      from match {
        case Some(from) =>
          val fromKeyBytes: Slice[Byte] = from.key

          if (from.before)
            core.before(fromKeyBytes, readState)
          else if (from.after)
            core.after(fromKeyBytes, readState)
          else
            tag.flatMap(core.getKeyValue(fromKeyBytes, readState)) {
              case some @ Some(_) =>
                tag.success(some): T[Option[(Slice[Byte], Option[Slice[Byte]])]]

              case _ =>
                if (from.orAfter)
                  core.after(fromKeyBytes, readState)
                else if (from.orBefore)
                  core.before(fromKeyBytes, readState)
                else
                  tag.success(None): T[Option[(Slice[Byte], Option[Slice[Byte]])]]
            }

        case None =>
          if (reverseIteration) core.last(readState) else core.head(readState)
      }
    }(_.map {
      case (key, value) =>
        (key.read[K], value.read[V])
    })

  override def drop(count: Int): Stream[(K, V), T] =
    stream drop count

  override def dropWhile(f: ((K, V)) => Boolean): Stream[(K, V), T] =
    stream dropWhile f

  override def take(count: Int): Stream[(K, V), T] =
    stream take count

  override def takeWhile(f: ((K, V)) => Boolean): Stream[(K, V), T] =
    stream takeWhile f

  override def map[B](f: ((K, V)) => B): Stream[B, T] =
    stream map f

  override def flatMap[B](f: ((K, V)) => Stream[B, T]): Stream[B, T] =
    stream flatMap f

  override def foreach[U](f: ((K, V)) => U): Stream[Unit, T] =
    stream foreach f

  override def filter(f: ((K, V)) => Boolean): Stream[(K, V), T] =
    stream filter f

  override def filterNot(f: ((K, V)) => Boolean): Stream[(K, V), T] =
    stream filterNot f

  override def foldLeft[B](initial: B)(f: (B, (K, V)) => B): T[B] =
    stream.foldLeft(initial)(f)

  def size: T[Int] =
    tag.point(keys.size)

  def stream: Stream[(K, V), T] =
    new Stream[(K, V), T] {
      val readState = core.readStates.get()
      override def headOption: T[Option[(K, V)]] =
        self.headOption(readState)

      override private[swaydb] def next(previous: (K, V)): T[Option[(K, V)]] =
        tag.map {
          if (reverseIteration)
            core.before(keySerializer.write(previous._1), readState)
          else
            core.after(keySerializer.write(previous._1), readState)
        }(_.map {
          case (key, value) =>
            (key.read[K], value.read[V])
        })
    }

  def streamer: Streamer[(K, V), T] =
    stream.streamer

  def sizeOfBloomFilterEntries: T[Int] =
    tag.point(core.bloomFilterKeyValueCount)

  def isEmpty: T[Boolean] =
    tag.map(core.headKey(core.readStates.get()))(_.isEmpty)

  def nonEmpty: T[Boolean] =
    tag.map(isEmpty)(!_)

  def lastOption: T[Option[(K, V)]] =
    if (reverseIteration)
      tag.map(core.head(core.readStates.get())) {
        case Some((key, value)) =>
          Some(key.read[K], value.read[V])

        case _ =>
          None
      }
    else
      tag.map(core.last(core.readStates.get())) {
        case Some((key, value)) =>
          Some(key.read[K], value.read[V])
        case _ =>
          None
      }

  def reverse: Map[K, V, F, T] =
    copy(reverseIteration = true)

  /**
   * Returns an Async API of type O where the [[Tag]] is known.
   */
  def toTag[X[_]](implicit tag: Tag[X]): Map[K, V, F, X] =
    copy(core = core.toTag[X])

  def asScala: scala.collection.mutable.Map[K, V] =
    ScalaMap[K, V, F](toTag[IO.ApiIO](Tag.apiIO))

  def close(): T[Unit] =
    tag.point(core.close())

  def delete(): T[Unit] =
    tag.point(core.delete())

  override def toString(): String =
    classOf[Map[_, _, _, T]].getClass.getSimpleName
}