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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}
import swaydb.PrepareImplicits._
import swaydb.core.Core
import swaydb.data.IO
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.io.{Tag, TagAsync}
import swaydb.data.io.Tag._
import swaydb.data.slice.Slice
import swaydb.serializers.{Serializer, _}

/**
  * Map database API.
  *
  * For documentation check - http://swaydb.io/wrap/
  */
case class Map[K, V, T[_]](private[swaydb] val core: Core[T],
                           private val from: Option[From[K]] = None,
                           private[swaydb] val reverseIteration: Boolean = false)(implicit keySerializer: Serializer[K],
                                                                                  valueSerializer: Serializer[V],
                                                                                  tag: Tag[T]) extends Streamer[(K, V), T] { self =>

  def wrapCall[C](f: => T[C]): T[C] =
    tag.success(()) flatMap (_ => f)

  def put(key: K, value: V): T[IO.OK] =
    wrapCall(core.put(key = key, value = Some(value)))

  def put(key: K, value: V, expireAfter: FiniteDuration): T[IO.OK] =
    wrapCall(core.put(key, Some(value), expireAfter.fromNow))

  def put(key: K, value: V, expireAt: Deadline): T[IO.OK] =
    wrapCall(core.put(key, Some(value), expireAt))

  def put(keyValues: (K, V)*): T[IO.OK] =
    wrapCall(put(keyValues))

  def put(keyValues: Stream[(K, V), T]): T[IO.OK] =
    wrapCall(keyValues.materialize flatMap put)

  def put(keyValues: Iterable[(K, V)]): T[IO.OK] =
    wrapCall {
      core.put {
        keyValues map {
          case (key, value) =>
            Prepare.Put(keySerializer.write(key), Some(valueSerializer.write(value)), None)
        }
      }
    }

  def remove(key: K): T[IO.OK] =
    wrapCall(core.remove(key))

  def remove(from: K, to: K): T[IO.OK] =
    wrapCall(core.remove(from, to))

  def remove(keys: K*): T[IO.OK] =
    wrapCall(remove(keys))

  def remove(keys: Stream[K, T]): T[IO.OK] =
    wrapCall(keys.materialize flatMap remove)

  def remove(keys: Iterable[K]): T[IO.OK] =
    wrapCall(core.put(keys.map(key => Prepare.Remove(keySerializer.write(key)))))

  def expire(key: K, after: FiniteDuration): T[IO.OK] =
    wrapCall(core.remove(key, after.fromNow))

  def expire(key: K, at: Deadline): T[IO.OK] =
    wrapCall(core.remove(key, at))

  def expire(from: K, to: K, after: FiniteDuration): T[IO.OK] =
    wrapCall(core.remove(from, to, after.fromNow))

  def expire(from: K, to: K, at: Deadline): T[IO.OK] =
    wrapCall(core.remove(from, to, at))

  def expire(keys: (K, Deadline)*): T[IO.OK] =
    wrapCall(expire(keys))

  def expire(keys: Stream[(K, Deadline), T]): T[IO.OK] =
    wrapCall(keys.materialize flatMap expire)

  def expire(keys: Iterable[(K, Deadline)]): T[IO.OK] =
    wrapCall {
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

  def update(key: K, value: V): T[IO.OK] =
    wrapCall(core.update(key, Some(value)))

  def update(from: K, to: K, value: V): T[IO.OK] =
    wrapCall(core.update(from, to, Some(value)))

  def update(keyValues: (K, V)*): T[IO.OK] =
    wrapCall(update(keyValues))

  def update(keyValues: Stream[(K, V), T]): T[IO.OK] =
    wrapCall(keyValues.materialize flatMap update)

  def update(keyValues: Iterable[(K, V)]): T[IO.OK] =
    wrapCall {
      core.put {
        keyValues map {
          case (key, value) =>
            Prepare.Update(keySerializer.write(key), Some(valueSerializer.write(value)))
        }
      }
    }

  def clear(): T[IO.OK] =
    wrapCall(core.clear())

  def registerFunction(functionID: K, function: V => Apply.Map[V]): K = {
    core.registerFunction(functionID, SwayDB.toCoreFunction(function))
    functionID
  }

  def registerFunction(functionID: K, function: (K, Option[Deadline]) => Apply.Map[V]): K = {
    core.registerFunction(functionID, SwayDB.toCoreFunction(function))
    functionID
  }

  def registerFunction(functionID: K, function: (K, V, Option[Deadline]) => Apply.Map[V]): K = {
    core.registerFunction(functionID, SwayDB.toCoreFunction(function))
    functionID
  }

  def applyFunction(key: K, functionID: K): T[IO.OK] =
    wrapCall(core.function(key, functionID))

  def applyFunction(from: K, to: K, functionID: K): T[IO.OK] =
    wrapCall(core.function(from, to, functionID))

  def commit(prepare: Prepare[K, V]*): T[IO.OK] =
    wrapCall(core.put(prepare))

  def commit(prepare: Stream[Prepare[K, V], T]): T[IO.OK] =
    wrapCall(prepare.materialize flatMap commit)

  def commit(prepare: Iterable[Prepare[K, V]]): T[IO.OK] =
    wrapCall(core.put(prepare))

  /**
    * Returns target value for the input key.
    */
  def get(key: K): T[Option[V]] =
    wrapCall(core.get(key).map(_.map(_.read[V])))

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): T[Option[K]] =
    wrapCall(core.getKey(key).map(_.map(_.read[K])))

  def getKeyValue(key: K): T[Option[(K, V)]] =
    wrapCall {
      core.getKeyValue(key).map(_.map {
        case (key, value) =>
          (key.read[K], value.read[V])
      })
    }

  def contains(key: K): T[Boolean] =
    wrapCall(core contains key)

  def mightContain(key: K): T[Boolean] =
    wrapCall(core mightContain key)

  def keys: Set[K, T] =
    Set[K, T](
      core = core,
      from = from,
      reverseIteration = reverseIteration
    )(keySerializer, tag)

  def level0Meter: LevelZeroMeter =
    core.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    core.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    core.sizeOfSegments

  def keySize(key: K): Int =
    (key: Slice[Byte]).size

  def valueSize(value: V): Int =
    (value: Slice[Byte]).size

  def expiration(key: K): T[Option[Deadline]] =
    wrapCall(core deadline key)

  def timeLeft(key: K): T[Option[FiniteDuration]] =
    wrapCall(expiration(key).map(_.map(_.timeLeft)))

  def from(key: K): Map[K, V, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: K): Map[K, V, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: K): Map[K, V, T] =
    copy(from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: K): Map[K, V, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: K): Map[K, V, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)))

  def headOption: T[Option[(K, V)]] =
    wrapCall {
      from match {
        case Some(from) =>
          val fromKeyBytes: Slice[Byte] = from.key

          if (from.before)
            core.before(fromKeyBytes)
          else if (from.after)
            core.after(fromKeyBytes)
          else
            core.getKeyValue(fromKeyBytes)
              .flatMap {
                case some @ Some(_) =>
                  tag.success(some): T[Option[(Slice[Byte], Option[Slice[Byte]])]]

                case _ =>
                  if (from.orAfter)
                    core.after(fromKeyBytes)
                  else if (from.orBefore)
                    core.before(fromKeyBytes)
                  else
                    tag.success(None): T[Option[(Slice[Byte], Option[Slice[Byte]])]]
              }

        case None =>
          if (reverseIteration) core.last else core.head
      }
    } map (_.map {
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
    wrapCall(keys.size)

  def stream: Stream[(K, V), T] =
    new Stream[(K, V), T] {
      override def headOption: T[Option[(K, V)]] =
        self.headOption

      override private[swaydb] def next(previous: (K, V)): T[Option[(K, V)]] =
        wrapCall {
          val next =
            if (reverseIteration)
              core.before(keySerializer.write(previous._1))
            else
              core.after(keySerializer.write(previous._1))

          next map (_.map {
            case (key, value) =>
              (key.read[K], value.read[V])
          })
        }
    }

  def sizeOfBloomFilterEntries: T[Int] =
    wrapCall(core.bloomFilterKeyValueCount)

  def isEmpty: T[Boolean] =
    wrapCall(core.headKey.map(_.isEmpty))

  def nonEmpty: T[Boolean] =
    isEmpty.map(!_)

  def lastOption: T[Option[(K, V)]] =
    if (reverseIteration)
      wrapCall {
        core.head map {
          case Some((key, value)) =>
            Some(key.read[K], value.read[V])

          case _ =>
            None
        }
      }
    else
      wrapCall {
        core.last map {
          case Some((key, value)) =>
            Some(key.read[K], value.read[V])
          case _ =>
            None
        }
      }

  def reverse: Map[K, V, T] =
    copy(reverseIteration = true)

  /**
    * Returns an Async API of type O where the [[Tag]] is known.
    */
  def tagAsync[O[_]](implicit ec: ExecutionContext,
                     tag: TagAsync[O]): Map[K, V, O] =
    copy(core = core.tagAsync[O])

  /**
    * Returns an blocking API of type O where the [[Tag]] is known.
    */
  def tagBlocking[O[_]](implicit tag: Tag[O]): Map[K, V, O] =
    copy(core = core.tagBlocking[O])

  def asScala: scala.collection.mutable.Map[K, V] =
    ScalaMap[K, V](tagBlocking[IO](Tag.io))

  def close(): T[Unit] =
    wrapCall(core.close())

  override def toString(): String =
    classOf[Map[_, _, T]].getClass.getSimpleName

}