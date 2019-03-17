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

import scala.concurrent.duration.{Deadline, FiniteDuration}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try
import swaydb.PrepareImplicits._
import swaydb.Wrap._
import swaydb.core.Core
import swaydb.data.IO
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.serializers.{Serializer, _}

/**
  * Map database API.
  *
  * For documentation check - http://swaydb.io/wrap/
  */
case class Map[K, V, W[_]](private[swaydb] val core: Core[W],
                           private[swaydb] val count: Option[Int] = None,
                           private[swaydb] val skip: Int = 0,
                           private val from: Option[From[K]] = None,
                           private[swaydb] val reverseIteration: Boolean = false,
                           private val takeWhileCondition: Option[(K, V) => Boolean] = None)(implicit keySerializer: Serializer[K],
                                                                                             valueSerializer: Serializer[V],
                                                                                             wrap: Wrap[W]) extends Stream[(K, V), W] {

  def wrapCall[T](f: => W[T]): W[T] =
    wrap(()).flatMap(_ => f)

  def put(key: K, value: V): W[Level0Meter] =
    wrapCall(core.put(key = key, value = Some(value)))

  def put(key: K, value: V, expireAfter: FiniteDuration): W[Level0Meter] =
    wrapCall(core.put(key, Some(value), expireAfter.fromNow))

  def put(key: K, value: V, expireAt: Deadline): W[Level0Meter] =
    wrapCall(core.put(key, Some(value), expireAt))

  def put(keyValues: (K, V)*): W[Level0Meter] =
    wrapCall(put(keyValues))

  def put(keyValues: Iterable[(K, V)]): W[Level0Meter] =
    wrapCall {
      core.put {
        keyValues map {
          case (key, value) =>
            Prepare.Put(keySerializer.write(key), Some(valueSerializer.write(value)), None)
        }
      }
    }

  def remove(key: K): W[Level0Meter] =
    wrapCall(core.remove(key))

  def remove(from: K, to: K): W[Level0Meter] =
    wrapCall(core.remove(from, to))

  def remove(keys: K*): W[Level0Meter] =
    wrapCall(remove(keys))

  def remove(keys: Iterable[K]): W[Level0Meter] =
    wrapCall(core.put(keys.map(key => Prepare.Remove(keySerializer.write(key)))))

  def expire(key: K, after: FiniteDuration): W[Level0Meter] =
    wrapCall(core.remove(key, after.fromNow))

  def expire(key: K, at: Deadline): W[Level0Meter] =
    wrapCall(core.remove(key, at))

  def expire(from: K, to: K, after: FiniteDuration): W[Level0Meter] =
    wrapCall(core.remove(from, to, after.fromNow))

  def expire(from: K, to: K, at: Deadline): W[Level0Meter] =
    wrapCall(core.remove(from, to, at))

  def expire(keys: (K, Deadline)*): W[Level0Meter] =
    wrapCall(expire(keys))

  def expire(keys: Iterable[(K, Deadline)]): W[Level0Meter] =
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

  def update(key: K, value: V): W[Level0Meter] =
    wrapCall(core.update(key, Some(value)))

  def update(from: K, to: K, value: V): W[Level0Meter] =
    wrapCall(core.update(from, to, Some(value)))

  def update(keyValues: (K, V)*): W[Level0Meter] =
    wrapCall(update(keyValues))

  def update(keyValues: Iterable[(K, V)]): W[Level0Meter] =
    wrapCall {
      core.put {
        keyValues map {
          case (key, value) =>
            Prepare.Update(keySerializer.write(key), Some(valueSerializer.write(value)))
        }
      }
    }

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

  def applyFunction(key: K, functionID: K): W[Level0Meter] =
    wrapCall(core.function(key, functionID))

  def applyFunction(from: K, to: K, functionID: K): W[Level0Meter] =
    wrapCall(core.function(from, to, functionID))

  def commit(prepare: Prepare[K, V]*): W[Level0Meter] =
    wrapCall(core.put(prepare))

  def commit(prepare: Iterable[Prepare[K, V]]): W[Level0Meter] =
    wrapCall(core.put(prepare))

  /**
    * Returns target value for the input key.
    */
  def get(key: K): W[Option[V]] =
    wrapCall(core.get(key).map(_.map(_.read[V])))

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): W[Option[K]] =
    wrapCall(core.getKey(key).map(_.map(_.read[K])))

  def getKeyValue(key: K): W[Option[(K, V)]] =
    wrapCall {
      core.getKeyValue(key).map(_.map {
        case (key, value) =>
          (key.read[K], value.read[V])
      })
    }

  def contains(key: K): W[Boolean] =
    wrapCall(core contains key)

  def mightContain(key: K): W[Boolean] =
    wrapCall(core mightContain key)

  def keys: Set[K, W] =
    Set[K, W](core, None)(keySerializer, wrap)

  def level0Meter: Level0Meter =
    core.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    core.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    core.sizeOfSegments

  def keySize(key: K): Int =
    (key: Slice[Byte]).size

  def valueSize(value: V): Int =
    (value: Slice[Byte]).size

  def expiration(key: K): W[Option[Deadline]] =
    wrapCall(core deadline key)

  def timeLeft(key: K): W[Option[FiniteDuration]] =
    wrapCall(expiration(key).map(_.map(_.timeLeft)))

  def from(key: K): Map[K, V, W] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: K) =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: K) =
    copy(from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: K) =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: K) =
    copy(from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)))

  def takeWhile(condition: (K, V) => Boolean) =
    copy(takeWhileCondition = Some(condition))

  def takeWhileKey(condition: K => Boolean) =
    copy(
      takeWhileCondition =
        Some(
          (key: K, _: V) =>
            condition(key)
        )
    )

  def takeWhileValue(condition: V => Boolean) =
    copy(
      takeWhileCondition =
        Some(
          (_: K, value: V) =>
            condition(value)
        )
    )

  def checkTakeWhile(key: Slice[Byte], value: Option[Slice[Byte]]): Option[(K, V)] = {
    val keyT = key.read[K]
    val valueT = value.read[V]
    if (takeWhileCondition.forall(_ (keyT, valueT)))
      Some(keyT, valueT)
    else
      None
  }

  override def headOption: W[Option[(K, V)]] =
    wrapCall {
      from match {
        case Some(from) =>
          val fromKeyBytes: Slice[Byte] = from.key

          val first =
            if (from.before)
              core.before(fromKeyBytes)
            else if (from.after)
              core.after(fromKeyBytes)
            else
              core.getKeyValue(fromKeyBytes)
                .flatMap {
                  case some @ Some(_) =>
                    wrap.success(some): W[Option[(Slice[Byte], Option[Slice[Byte]])]]

                  case _ =>
                    if (from.orAfter)
                      core.after(fromKeyBytes)
                    else if (from.orBefore)
                      core.before(fromKeyBytes)
                    else
                      wrap.success(None): W[Option[(Slice[Byte], Option[Slice[Byte]])]]
                }

          first.map(_.flatMap {
            case (key, value) =>
              checkTakeWhile(key, value)
          })

        case None =>
          val first = if (reverseIteration) core.last else core.head
          first.map(_.flatMap {
            case (key, value) =>
              checkTakeWhile(key, value)
          })
      }
    }

  override def next(previous: (K, V)): W[Option[(K, V)]] =
    wrapCall {
      val next =
        if (reverseIteration)
          core.before(keySerializer.write(previous._1))
        else
          core.after(keySerializer.write(previous._1))

      next.map(_.flatMap {
        case (key, value) =>
          checkTakeWhile(key, value)
      })
    }

  def take(count: Int): Map[K, V, W] =
    copy(count = Some(count))

  def drop(count: Int) =
    copy(skip = count)

  def restart: Stream[(K, V), W] =
    copy()

  def size: W[Int] =
    wrapCall(core.bloomFilterKeyValueCount)

  def isEmpty: W[Boolean] =
    wrapCall(core.headKey.map(_.isEmpty))

  def nonEmpty: W[Boolean] =
    isEmpty.map(!_)

  def lastOption: W[Option[(K, V)]] =
    if (takeWhileCondition.isDefined)
      wrapCall(lastOptionStream)
    else if (reverseIteration)
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

  def reverse =
    copy(reverseIteration = true)

  def closeDatabase(): W[Unit] =
    wrapCall(core.close())

  def async(implicit futureWrap: Wrap[Future],
            ec: ExecutionContext): Map[K, V, Future] =
    copy(core = core.async())

  def syncTry(implicit tryWrap: Wrap[Try],
              ec: ExecutionContext): Map[K, V, Try] =
    copy(core = core.syncTry())

  def syncIO(implicit ioWrap: Wrap[IO],
             ec: ExecutionContext): Map[K, V, IO] =
    copy(core = core.syncIO())

  override def toString(): String =
    classOf[Map[_, _, W]].getClass.getSimpleName

}