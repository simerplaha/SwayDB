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
import swaydb.PrepareImplicits._
import swaydb.Wrap._
import swaydb.core.Core
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

  override def hasNext: W[Boolean] = ???
  override def next(): W[(K, V)] = ???

//  override def iterator = new Iterator[W[(K, V)]] {
//
//    private var hasMore: Boolean = true
//    private var started: Boolean = false
//    private var stashedKeyValueBytes: W[(Slice[Byte], Option[Slice[Byte]])] = _
//    private var stashedKeyValueTyped: W[(K, V)] = _
//
//    private def start: W[Option[(Slice[Byte], Option[Slice[Byte]])]] =
//      from match {
//        case Some(from) =>
//          val fromKeyBytes: Slice[Byte] = from.key
//          if (from.before)
//            core.before(fromKeyBytes)
//          else if (from.after)
//            core.after(fromKeyBytes)
//          else
//            core.getKeyValue(fromKeyBytes) flatMap {
//              case Some((key, valueOption)) =>
//                wrap.success(Some(key, valueOption))
//              case _ =>
//                if (from.orAfter)
//                  core.after(fromKeyBytes)
//                else if (from.orBefore)
//                  core.before(fromKeyBytes)
//                else
//                  wrap.none
//            }
//
//        case None =>
//          if (reverse)
//            core.last
//          else
//            core.head
//      }
//
//    override def hasNext: Boolean =
//      hasMore
//
//    override def next(): W[(K, V)] =
//      if (started) {
//        started = true
//        if (stashedKeyValueBytes == null) {
//          hasMore = false
//          wrap.terminate
//        } else {
//          stashedKeyValueBytes flatMap {
//            previous =>
//              val next =
//                if (reverse)
//                  core.before(previous._1)
//                else
//                  core.after(previous._1)
//
//              next flatMap {
//                case Some(keyValue @ (key, value)) =>
//                  val keyT = key.read[K]
//                  val valueT = value.read[V]
//                  if (till(keyT, valueT)) {
//                    stashedKeyValueBytes = wrap.success(keyValue)
//                    stashedKeyValueTyped = wrap.success(keyT, valueT)
//                    hasMore = true
//                    stashedKeyValueTyped
//                  } else {
//                    hasMore = false
//                    wrap.terminate
//                  }
//
//                case _ =>
//                  hasMore = false
//                  wrap.terminate
//              }
//          }
//        }
//      } else {
//        start flatMap {
//          start =>
//            started = true
//            start match {
//              case Some(keyValue @ (key, value)) =>
//                val keyT = key.read[K]
//                val valueT = value.read[V]
//                if (till(keyT, valueT)) {
//                  stashedKeyValueBytes = wrap.success(keyValue)
//                  stashedKeyValueTyped = wrap.success(keyT, valueT)
//                  stashedKeyValueTyped
//                } else {
//                  hasMore = false
//                  wrap.terminate
//                }
//
//              case _ =>
//                hasMore = false
//                wrap.terminate
//            }
//        }
//      }
//
//    override def toString(): String =
//      classOf[Map[_, _, W]].getClass.getSimpleName
//  }

  override def size: Int =
    sizeIO.get

  def sizeIO: W[Int] =
    core.bloomFilterKeyValueCount

  override def isEmpty: Boolean =
    core.headKey.get.isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

//  override def headOption: Option[W[(K, V)]] =
//    throw new UnsupportedOperationException("headOption is not supported")
//
//  override def lastOption: Option[W[(K, V)]] =
//    throw new UnsupportedOperationException("lastOption is not supported")
//
//  def foreachRight[U](f: W[(K, V)] => U): Unit =
//    copy(reverse = true) foreach f
//
//  def mapRight[B, T](f: W[(K, V)] => B): Traversable[B] =
//    copy(reverse = true) map f
//
//  override def foldRight[B](z: B)(op: (W[(K, V)], B) => B): B =
//    copy(reverse = true).foldLeft(z) {
//      case (b, container) =>
//        op(container, b)
//    }
//
//  def takeRight(n: Int): Traversable[W[(K, V)]] =
//    copy(reverse = true).take(n)
//
//  def dropRight(n: Int): Traversable[W[(K, V)]] =
//    copy(reverse = true).drop(n)
//
//  override def reduceRight[B >: W[(K, V)]](op: (W[(K, V)], B) => B): B =
//    copy(reverse = true).reduceLeft[B] {
//      case (b, container) =>
//        op(container, b)
//    }
//
//  override def reduceRightOption[B >: W[(K, V)]](op: (W[(K, V)], B) => B): Option[B] =
//    copy(reverse = true).reduceLeftOption[B] {
//      case (b, container) =>
//        op(container, b)
//    }

  def closeDatabase(): W[Unit] =
    core.close()

  def futureAPI(implicit futureWrap: Wrap[Future],
                ec: ExecutionContext) =
    copy(core = core.async())

  override def toString(): String =
    classOf[Map[_, _, W]].getClass.getSimpleName

}