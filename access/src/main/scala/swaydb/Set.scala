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
import swaydb.PrepareImplicits._
import swaydb.core.Core
import swaydb.data.IO
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.serializers.{Serializer, _}
import Wrap._

object Set {
  def apply[T](api: Core[IO])(implicit serializer: Serializer[T]): Set[T, IO] =
    new Set(api, None)
}

/**
  * Set database API.
  *
  * For documentation check - http://swaydb.io/api/
  */
case class Set[T, F[_]](private val core: Core[F],
                        private val from: Option[From[T]],
                        private[swaydb] val reverse: Boolean = false,
                        private val till: T => Boolean = (_: T) => true)(implicit serializer: Serializer[T],
                                                                         wrap: Wrap[F]) extends Iterable[F[T]] {

  //  def unsafeGet(elem: T): F[Option[T]] =
  //    core.getKey(elem).map(_.map(_.read[T]))
  //
  //  def contains(elem: T): F[Boolean] =
  //    core contains elem
  //
  //  def mightContain(elem: T): F[Boolean] =
  //    core mightContain elem
  //
  //  def add(elem: T): F[Level0Meter] =
  //    core.put(key = elem)
  //
  //  def add(elem: T, expireAt: Deadline): F[Level0Meter] =
  //    core.put(elem, None, expireAt)
  //
  //  def add(elem: T, expireAfter: FiniteDuration): F[Level0Meter] =
  //    core.put(elem, None, expireAfter.fromNow)
  //
  //  def add(elems: T*): F[Level0Meter] =
  //    add(elems)
  //
  //  def add(elems: Iterable[T]): F[Level0Meter] =
  //    core.put(elems.map(elem => Prepare.Put(key = serializer.write(elem), value = None, deadline = None)))
  //
  //  def remove(elem: T): F[Level0Meter] =
  //    core.remove(elem)
  //
  //  def remove(from: T, to: T): F[Level0Meter] =
  //    core.remove(from, to)
  //
  //  def remove(elems: T*): F[Level0Meter] =
  //    remove(elems)
  //
  //  def remove(elems: Iterable[T]): F[Level0Meter] =
  //    core.put(elems.map(elem => Prepare.Remove(serializer.write(elem))))
  //
  //  def expire(elem: T, after: FiniteDuration): F[Level0Meter] =
  //    core.remove(elem, after.fromNow)
  //
  //  def expire(elem: T, at: Deadline): F[Level0Meter] =
  //    core.remove(elem, at)
  //
  //  def expire(from: T, to: T, after: FiniteDuration): F[Level0Meter] =
  //    core.remove(from, to, after.fromNow)
  //
  //  def expire(from: T, to: T, at: Deadline): F[Level0Meter] =
  //    core.remove(from, to, at)
  //
  //  def expire(elems: (T, Deadline)*): F[Level0Meter] =
  //    expire(elems)
  //
  //  def expire(elems: Iterable[(T, Deadline)]): F[Level0Meter] =
  //    core.put {
  //      elems map {
  //        elemWithExpire =>
  //          Prepare.Remove(
  //            from = serializer.write(elemWithExpire._1),
  //            to = None,
  //            deadline = Some(elemWithExpire._2)
  //          )
  //      }
  //    }
  //
  //  def registerFunction(functionID: T, function: (T, Option[Deadline]) => Apply.Set[T]): T = {
  //    core.registerFunction(functionID, SwayDB.toCoreFunction(function))
  //    functionID
  //  }
  //
  //  def applyFunction(from: T, to: T, functionID: T): F[Level0Meter] =
  //    core.function(from, to, functionID)
  //
  //  def applyFunction(elem: T, function: T): F[Level0Meter] =
  //    core.function(elem, function)
  //
  //  def commit(prepare: Prepare[T, Nothing]*): F[Level0Meter] =
  //    core.put(prepare)
  //
  //  def commit(prepare: Iterable[Prepare[T, Nothing]]): F[Level0Meter] =
  //    core.put(prepare)
  //
  //  def level0Meter: Level0Meter =
  //    core.level0Meter
  //
  //  def levelMeter(levelNumber: Int): Option[LevelMeter] =
  //    core.levelMeter(levelNumber)
  //
  //  def sizeOfSegments: Long =
  //    core.sizeOfSegments
  //
  //  def elemSize(elem: T): Int =
  //    (elem: Slice[Byte]).size
  //
  //  def expiration(elem: T): F[Option[Deadline]] =
  //    core deadline elem
  //
  //  def timeLeft(elem: T): F[Option[FiniteDuration]] =
  //    expiration(elem).map(_.map(_.timeLeft))
  //
  //  def from(key: T): Set[T, F] =
  //    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)))
  //
  //  def before(key: T) =
  //    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)))
  //
  //  def fromOrBefore(key: T) =
  //    copy(from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)))
  //
  //  def after(key: T) =
  //    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)))
  //
  //  def fromOrAfter(key: T) =
  //    copy(from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)))
  //
  //  def till(condition: T => Boolean) =
  //    copy(till = condition)
  //
  //  override def iterator = new Iterator[T] {
  //
  //    private var started: Boolean = false
  //    private var nextKeyBytes: Slice[Byte] = _
  //    private var nextKeyTyped: T = _
  //
  //    private def start: F[Option[Slice[Byte]]] =
  //      from match {
  //        case Some(from) =>
  //          val fromKeyBytes: Slice[Byte] = from.key
  //          if (from.before)
  //            core.beforeKey(fromKeyBytes)
  //          else if (from.after)
  //            core.afterKey(fromKeyBytes)
  //          else
  //            core.getKey(fromKeyBytes)
  //              .flatMap {
  //                case Some(key) =>
  //                  wrap.success(Some(key))
  //
  //                case _ =>
  //                  if (from.orAfter)
  //                    core.afterKey(fromKeyBytes)
  //                  else if (from.orBefore)
  //                    core.beforeKey(fromKeyBytes)
  //                  else
  //                    wrap.none
  //              }
  //
  //        case None =>
  //          if (reverse)
  //            core.lastKey
  //          else
  //            core.headKey
  //      }
  //
  //    override def hasNext: Boolean =
  //      if (started) {
  //        if (nextKeyBytes == null)
  //          false
  //        else {
  //          val next =
  //            if (reverse)
  //              core.beforeKey(nextKeyBytes)
  //            else
  //              core.afterKey(nextKeyBytes)
  //
  //          next match {
  //            case IO.Success(key) =>
  //              key match {
  //                case Some(key) =>
  //                  val keyT = key.read[T]
  //                  if (till(keyT)) {
  //                    nextKeyBytes = key
  //                    nextKeyTyped = keyT
  //                    true
  //                  } else
  //                    false
  //
  //                case _ =>
  //                  false
  //              }
  //            case IO.Failure(error) =>
  //              System.err.println("Failed to iterate", error)
  //              throw error.exception
  //          }
  //        }
  //      } else
  //        start match {
  //          case IO.Success(value) =>
  //            started = true
  //            value match {
  //              case Some(key) =>
  //                val keyT = key.read[T]
  //                if (till(keyT)) {
  //                  nextKeyBytes = key
  //                  nextKeyTyped = keyT
  //                  true
  //                } else
  //                  false
  //
  //              case _ =>
  //                false
  //            }
  //          case IO.Failure(error) =>
  //            System.err.println("Failed to start Key iterator", error)
  //            throw error.exception
  //        }
  //
  //    override def next(): T =
  //      nextKeyTyped
  //
  //    override def toString(): String =
  //      classOf[Set[_, F]].getClass.getSimpleName
  //  }
  //
  //  override def head: F[T] =
  //    headOption
  //
  //  override def last: T =
  //    lastOption.unsafeGet
  //
  //  override def size: Int =
  //    core.bloomFilterKeyValueCount.unsafeGet
  //
  //  override def isEmpty: Boolean =
  //    core.headKey.unsafeGet.isEmpty
  //
  //  override def nonEmpty: Boolean =
  //    !isEmpty
  //
  //  override def headOption: Option[T] =
  //    if (from.isDefined)
  //      this.take(1).headOption
  //    else
  //      core.headKey.map(_.map(_.read[T])).unsafeGet
  //
  //  override def lastOption: Option[T] =
  //    core.lastKey.map(_.map(_.read[T])).unsafeGet
  //
  //  def foreachRight[U](f: T => U): Unit =
  //    copy(reverse = true) foreach f
  //
  //  def mapRight[B, C](f: T => B)(implicit bf: CanBuildFrom[Iterable[T], B, C]): C = {
  //    copy(reverse = true) map f
  //  }
  //
  //  override def foldRight[B](z: B)(op: (T, B) => B): B =
  //    copy(reverse = true).foldLeft(z) {
  //      case (b, k) =>
  //        op(k, b)
  //    }
  //
  //  override def takeRight(n: Int): Iterable[T] =
  //    copy(reverse = true).take(n)
  //
  //  override def dropRight(n: Int): Iterable[T] =
  //    copy(reverse = true).drop(n)
  //
  //  override def reduceRight[B >: T](op: (T, B) => B): B =
  //    copy(reverse = true).reduceLeft[B] {
  //      case (b, k) =>
  //        op(k, b)
  //    }
  //
  //  override def reduceRightOption[B >: T](op: (T, B) => B): Option[B] =
  //    copy(reverse = true).reduceLeftOption[B] {
  //      case (b, k) =>
  //        op(k, b)
  //    }
  //
  //  override def scanRight[B, That](z: B)(op: (T, B) => B)(implicit bf: CanBuildFrom[Iterable[T], B, That]): That =
  //    copy(reverse = true).scanLeft(z) {
  //      case (z, k) =>
  //        op(k, z)
  //    }
  //
  //  def closeDatabase(): F[Unit] =
  //    core.close()
  //
  //  override def toString(): String =
  //  //    classOf[Set[_, _]].getClass.getSimpleName
  //    ???
  override def iterator: Iterator[F[T]] = ???
}