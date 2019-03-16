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

import scala.collection.generic.CanBuildFrom
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
                           private val from: Option[From[K]] = None,
                           private[swaydb] val reverse: Boolean = false,
                           private val till: (K, V) => Boolean = (_: K, _: V) => true)(implicit keySerializer: Serializer[K],
                                                                                       valueSerializer: Serializer[V],
                                                                                       wrap: Wrap[W]) extends Stream[(K, V), W] {

  def put(key: K, value: V): W[Level0Meter] =
    core.put(key = key, value = Some(value))

  def put(key: K, value: V, expireAfter: FiniteDuration): W[Level0Meter] =
    core.put(key, Some(value), expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): W[Level0Meter] =
    core.put(key, Some(value), expireAt)

  def put(keyValues: (K, V)*): W[Level0Meter] =
    put(keyValues)

  def put(keyValues: Iterable[(K, V)]): W[Level0Meter] =
    core.put {
      keyValues map {
        case (key, value) =>
          Prepare.Put(keySerializer.write(key), Some(valueSerializer.write(value)), None)
      }
    }

  def remove(key: K): W[Level0Meter] =
    core.remove(key)

  def remove(from: K, to: K): W[Level0Meter] =
    core.remove(from, to)

  def remove(keys: K*): W[Level0Meter] =
    remove(keys)

  def remove(keys: Iterable[K]): W[Level0Meter] =
    core.put(keys.map(key => Prepare.Remove(keySerializer.write(key))))

  def expire(key: K, after: FiniteDuration): W[Level0Meter] =
    core.remove(key, after.fromNow)

  def expire(key: K, at: Deadline): W[Level0Meter] =
    core.remove(key, at)

  def expire(from: K, to: K, after: FiniteDuration): W[Level0Meter] =
    core.remove(from, to, after.fromNow)

  def expire(from: K, to: K, at: Deadline): W[Level0Meter] =
    core.remove(from, to, at)

  def expire(keys: (K, Deadline)*): W[Level0Meter] =
    expire(keys)

  def expire(keys: Iterable[(K, Deadline)]): W[Level0Meter] =
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

  def update(key: K, value: V): W[Level0Meter] =
    core.update(key, Some(value))

  def update(from: K, to: K, value: V): W[Level0Meter] =
    core.update(from, to, Some(value))

  def update(keyValues: (K, V)*): W[Level0Meter] =
    update(keyValues)

  def update(keyValues: Iterable[(K, V)]): W[Level0Meter] =
    core.put {
      keyValues map {
        case (key, value) =>
          Prepare.Update(keySerializer.write(key), Some(valueSerializer.write(value)))
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
    core.function(key, functionID)

  def applyFunction(from: K, to: K, functionID: K): W[Level0Meter] =
    core.function(from, to, functionID)

  def commit(prepare: Prepare[K, V]*): W[Level0Meter] =
    core.put(prepare)

  def commit(prepare: Iterable[Prepare[K, V]]): W[Level0Meter] =
    core.put(prepare)

  /**
    * Returns target value for the input key.
    */
  def get(key: K): W[Option[V]] =
    core.get(key).map(_.map(_.read[V]))

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): W[Option[K]] =
    core.getKey(key).map(_.map(_.read[K]))

  def getKeyValue(key: K): W[Option[(K, V)]] =
    core.getKeyValue(key).map(_.map {
      case (key, value) =>
        (key.read[K], value.read[V])
    })

  def contains(key: K): W[Boolean] =
    core contains key

  def mightContain(key: K): W[Boolean] =
    core mightContain key

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
    core deadline key

  def timeLeft(key: K): W[Option[FiniteDuration]] =
    expiration(key).map(_.map(_.timeLeft))

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

  def till(condition: (K, V) => Boolean) =
    copy(till = condition)

  def tillKey(condition: K => Boolean) =
    copy(
      till =
        (key: K, _: V) =>
          condition(key)
    )

  def tillValue(condition: V => Boolean) =
    copy(
      till =
        (_: K, value: V) =>
          condition(value)
    )

  override def first(): W[Option[(K, V)]] =
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
            Some(key.read[K], value.read[V])
        })

      case None =>
        val first = if (reverse) core.last else core.head
        first.map(_.flatMap {
          case (key, value) =>
            Some(key.read[K], value.read[V])
        })
    }

  override def next(previous: (K, V)): W[Option[(K, V)]] = {
    val next =
      if (reverse)
        core.before(keySerializer.write(previous._1))
      else
        core.after(keySerializer.write(previous._1))

    next.map(_.flatMap {
      case (key, value) =>
        val keyT = key.read[K]
        val valueT = value.read[V]
        if (till(keyT, valueT)) {
          Some(keyT, valueT)
        } else
          None
    })
  }

  override def size: Int =
    core.bloomFilterKeyValueCount.get

  override def isEmpty: Boolean =
    isEmptyW.get

  def isEmptyW: W[Boolean] =
    core.headKey.map(_.isEmpty)

  override def nonEmpty: Boolean =
    !isEmpty

  def nonEmptyW: W[Boolean] =
    isEmptyW.map(!_)

  override def head: (K, V) =
    headOption.get

  override def last: (K, V) =
    lastOption.get

  override def headOption: Option[(K, V)] =
    headOptionW.get

  def headOptionW: W[Option[(K, V)]] =
    if (from.isDefined)
      wrap(this.take(1).headOption)
    else
      core.head map {
        case Some((key, value)) =>
          Some(key.read[K], value.read[V])
        case _ =>
          None
      }

  override def lastOption: Option[(K, V)] =
    lastOptionW.get

  def lastOptionW: W[Option[(K, V)]] =
    core.last map {
      case Some((key, value)) =>
        Some(key.read[K], value.read[V])
      case _ =>
        None
    }

  def foreachRight[U](f: (K, V) => U): Unit =
    copy(reverse = true) foreach {
      case (k, v) =>
        f(k, v)
    }

  def mapRight[B, T](f: (K, V) => B)(implicit bf: CanBuildFrom[Iterable[(K, V)], B, T]): Stream[B, W] =
    copy(reverse = true).map({
      case (k, v) =>
        f(k, v)
    })(collection.breakOut)

  override def foldRight[B](z: B)(op: ((K, V), B) => B): B =
    copy(reverse = true).foldLeft(z) {
      case (b, (k, v)) =>
        op((k, v), b)
    }

  def takeRight(n: Int): Traversable[(K, V)] =
    copy(reverse = true).take(n)

  def dropRight(n: Int): Traversable[(K, V)] =
    copy(reverse = true).drop(n)

  override def reduceRight[B >: (K, V)](op: ((K, V), B) => B): B =
    copy(reverse = true).reduceLeft[B] {
      case (b, (k, v)) =>
        op((k, v), b)
    }

  override def reduceRightOption[B >: (K, V)](op: ((K, V), B) => B): Option[B] =
    copy(reverse = true).reduceLeftOption[B] {
      case (b, (k, v)) =>
        op((k, v), b)
    }

  override def scanRight[B, That](z: B)(op: ((K, V), B) => B)(implicit bf: CanBuildFrom[Stream[(K, V), W], B, That]): That =
    copy(reverse = true).scanLeft(z) {
      case (z, (k, v)) =>
        op((k, v), z)
    }

  def closeDatabase(): W[Unit] =
    core.close()

  def async(implicit futureWrap: Wrap[Future],
            ec: ExecutionContext) =
    copy(core = core.async())

  def syncTry(implicit tryWrap: Wrap[Try],
              ec: ExecutionContext) =
    copy(core = core.syncTry())

  def syncIO(implicit ioWrap: Wrap[IO],
             ec: ExecutionContext) =
    copy(core = core.syncIO())

  override def toString(): String =
    classOf[Map[_, _, W]].getClass.getSimpleName
}