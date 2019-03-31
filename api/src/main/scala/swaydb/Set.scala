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
import swaydb.Wrap._
import swaydb.core.Core
import swaydb.data.IO
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.io.converter.{AsyncIOConverter, BlockingIOConverter}
import swaydb.data.slice.Slice
import swaydb.serializers.{Serializer, _}

object Set {
  def apply[T](api: Core[IO])(implicit serializer: Serializer[T]): Set[T, IO] =
    new Set(api, None)
}

/**
  * Set database API.
  *
  * For documentation check - http://swaydb.io/api/
  */
case class Set[T, W[_]](private val core: Core[W],
                        private val from: Option[From[T]],
                        private[swaydb] val count: Option[Int] = None,
                        private[swaydb] val skip: Int = 0,
                        private[swaydb] val reverseIteration: Boolean = false,
                        private val till: Option[T => Boolean] = None)(implicit serializer: Serializer[T],
                                                                       wrap: Wrap[W]) extends Stream[T, W](skip, count) {

  def wrapCall[C](f: => W[C]): W[C] =
    wrap(()).flatMap(_ => f)

  def get(elem: T): W[Option[T]] =
    wrapCall(core.getKey(elem).map(_.map(_.read[T])))

  def contains(elem: T): W[Boolean] =
    wrapCall(core contains elem)

  def mightContain(elem: T): W[Boolean] =
    wrapCall(core mightContain elem)

  def add(elem: T): W[Level0Meter] =
    wrapCall(core.put(key = elem))

  def add(elem: T, expireAt: Deadline): W[Level0Meter] =
    wrapCall(core.put(elem, None, expireAt))

  def add(elem: T, expireAfter: FiniteDuration): W[Level0Meter] =
    wrapCall(core.put(elem, None, expireAfter.fromNow))

  def add(elems: T*): W[Level0Meter] =
    add(elems)

  def add(elems: Stream[T, W]): W[Level0Meter] =
    wrapCall(elems.run flatMap add)

  def add(elems: Iterable[T]): W[Level0Meter] =
    wrapCall(core.put(elems.map(elem => Prepare.Put(key = serializer.write(elem), value = None, deadline = None))))

  def remove(elem: T): W[Level0Meter] =
    wrapCall(core.remove(elem))

  def remove(from: T, to: T): W[Level0Meter] =
    wrapCall(core.remove(from, to))

  def remove(elems: T*): W[Level0Meter] =
    remove(elems)

  def remove(elems: Stream[T, W]): W[Level0Meter] =
    wrapCall(elems.run flatMap remove)

  def remove(elems: Iterable[T]): W[Level0Meter] =
    wrapCall(core.put(elems.map(elem => Prepare.Remove(serializer.write(elem)))))

  def expire(elem: T, after: FiniteDuration): W[Level0Meter] =
    wrapCall(core.remove(elem, after.fromNow))

  def expire(elem: T, at: Deadline): W[Level0Meter] =
    wrapCall(core.remove(elem, at))

  def expire(from: T, to: T, after: FiniteDuration): W[Level0Meter] =
    wrapCall(core.remove(from, to, after.fromNow))

  def expire(from: T, to: T, at: Deadline): W[Level0Meter] =
    wrapCall(core.remove(from, to, at))

  def expire(elems: (T, Deadline)*): W[Level0Meter] =
    expire(elems)

  def expire(elems: Stream[(T, Deadline), W]): W[Level0Meter] =
    wrapCall(elems.run flatMap expire)

  def expire(elems: Iterable[(T, Deadline)]): W[Level0Meter] =
    wrapCall {
      core.put {
        elems map {
          elemWithExpire =>
            Prepare.Remove(
              from = serializer.write(elemWithExpire._1),
              to = None,
              deadline = Some(elemWithExpire._2)
            )
        }
      }
    }

  def clear(): W[Level0Meter] =
    wrapCall(core.clear())

  def registerFunction(functionID: T, function: (T, Option[Deadline]) => Apply.Set[T]): T = {
    core.registerFunction(functionID, SwayDB.toCoreFunction(function))
    functionID
  }

  def applyFunction(from: T, to: T, functionID: T): W[Level0Meter] =
    wrapCall(core.function(from, to, functionID))

  def applyFunction(elem: T, function: T): W[Level0Meter] =
    wrapCall(core.function(elem, function))

  def commit(prepare: Prepare[T, Nothing]*): W[Level0Meter] =
    wrapCall(core.put(prepare))

  def commit(prepare: Stream[Prepare[T, Nothing], W]): W[Level0Meter] =
    wrapCall(prepare.run flatMap commit)

  def commit(prepare: Iterable[Prepare[T, Nothing]]): W[Level0Meter] =
    wrapCall(core.put(prepare))

  def level0Meter: Level0Meter =
    core.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    core.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    core.sizeOfSegments

  def elemSize(elem: T): Int =
    (elem: Slice[Byte]).size

  def expiration(elem: T): W[Option[Deadline]] =
    wrapCall(core deadline elem)

  def timeLeft(elem: T): W[Option[FiniteDuration]] =
    expiration(elem).map(_.map(_.timeLeft))

  def from(key: T): Set[T, W] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: T): Set[T, W] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: T): Set[T, W] =
    copy(from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: T): Set[T, W] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: T): Set[T, W] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)))

  def takeWhile(condition: T => Boolean): Set[T, W] =
    copy(till = Some(condition))

  def take(count: Int): Set[T, W] =
    copy(count = Some(count))

  def drop(count: Int): Set[T, W] =
    copy(skip = count)

  private def checkTakeWhile(key: Slice[Byte]): Option[T] = {
    val keyT = key.read[T]
    if (till.forall(_ (keyT)))
      Some(keyT)
    else
      None
  }

  override def headOption: W[Option[T]] =
    wrapCall {
      from match {
        case Some(from) =>
          val fromKeyBytes: Slice[Byte] = from.key
          val first =
            if (from.before)
              core.beforeKey(fromKeyBytes)
            else if (from.after)
              core.afterKey(fromKeyBytes)
            else
              core.getKey(fromKeyBytes)
                .flatMap {
                  case Some(key) =>
                    wrap.success(Some(key)): W[Option[Slice[Byte]]]

                  case _ =>
                    if (from.orAfter)
                      core.afterKey(fromKeyBytes)
                    else if (from.orBefore)
                      core.beforeKey(fromKeyBytes)
                    else
                      wrap.success(None): W[Option[Slice[Byte]]]
                }

          first.map(_.flatMap(checkTakeWhile))

        case None =>
          val first = if (reverseIteration) core.lastKey else core.headKey
          first.map(_.flatMap(checkTakeWhile))
      }
    }

  override def next(previous: T): W[Option[T]] =
    wrapCall {
      val next =
        if (reverseIteration)
          core.beforeKey(serializer.write(previous))
        else
          core.afterKey(serializer.write(previous))

      next.map(_.flatMap(checkTakeWhile))
    }

  def size: W[Int] =
    wrapCall(core.bloomFilterKeyValueCount)

  def isEmpty: W[Boolean] =
    wrapCall(core.headKey.map(_.isEmpty))

  def nonEmpty: W[Boolean] =
    isEmpty.map(!_)

  def lastOption: W[Option[T]] =
    if (till.isDefined)
      wrapCall(lastOptionLinear)
    else if (reverseIteration)
      wrapCall(core.headKey.map(_.map(_.read[T])))
    else
      wrapCall(core.lastKey.map(_.map(_.read[T])))

  def reverse: Set[T, W] =
    copy(reverseIteration = true)

  def asyncAPI[O[_]](implicit ec: ExecutionContext,
                     convert: AsyncIOConverter[O],
                     wrap: Wrap[O]): Set[T, O] =
    copy(core = core.async[O])

  def blockingAPI[O[_]](implicit convert: BlockingIOConverter[O],
                        wrap: Wrap[O]): Set[T, O] =
    copy(core = core.blocking[O])

  def asScala: scala.collection.mutable.Set[T] =
    ScalaSet[T](blockingAPI[IO])

  def closeDatabase(): W[Unit] =
    wrapCall(core.close())

  override def toString(): String =
    classOf[Map[_, _, W]].getClass.getSimpleName
}