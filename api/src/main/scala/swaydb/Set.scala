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

import swaydb.IO.SIO
import swaydb.PrepareImplicits._
import swaydb.core.Core
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.io.Tag
import swaydb.data.io.Tag._
import swaydb.data.slice.Slice
import swaydb.serializers.{Serializer, _}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{Deadline, FiniteDuration}

object Set {
  def apply[T](api: Core[SIO])(implicit serializer: Serializer[T]): Set[T, SIO] =
    new Set(api, None)
}

/**
  * Set database API.
  *
  * For documentation check - http://swaydb.io/api/
  */
case class Set[A, T[_]](private val core: Core[T],
                        private val from: Option[From[A]],
                        private[swaydb] val reverseIteration: Boolean = false)(implicit serializer: Serializer[A],
                                                                               tag: Tag[T]) extends Streamed[A, T] { self =>

  def wrapCall[C](f: => T[C]): T[C] =
    tag.success(()) flatMap (_ => f)

  def get(elem: A): T[Option[A]] =
    wrapCall(core.getKey(elem).map(_.map(_.read[A])))

  def contains(elem: A): T[Boolean] =
    wrapCall(core contains elem)

  def mightContain(elem: A): T[Boolean] =
    wrapCall(core mightContainKey elem)

  def mightContainFunction(functionId: A): T[Boolean] =
    wrapCall(core mightContainFunction functionId)

  def add(elem: A): T[IO.OK] =
    wrapCall(core.put(key = elem))

  def add(elem: A, expireAt: Deadline): T[IO.OK] =
    wrapCall(core.put(elem, None, expireAt))

  def add(elem: A, expireAfter: FiniteDuration): T[IO.OK] =
    wrapCall(core.put(elem, None, expireAfter.fromNow))

  def add(elems: A*): T[IO.OK] =
    add(elems)

  def add(elems: Stream[A, T]): T[IO.OK] =
    wrapCall(elems.materialize flatMap add)

  def add(elems: Iterable[A]): T[IO.OK] =
    wrapCall(core.put(elems.map(elem => Prepare.Put(key = serializer.write(elem), value = None, deadline = None))))

  def remove(elem: A): T[IO.OK] =
    wrapCall(core.remove(elem))

  def remove(from: A, to: A): T[IO.OK] =
    wrapCall(core.remove(from, to))

  def remove(elems: A*): T[IO.OK] =
    remove(elems)

  def remove(elems: Stream[A, T]): T[IO.OK] =
    wrapCall(elems.materialize flatMap remove)

  def remove(elems: Iterable[A]): T[IO.OK] =
    wrapCall(core.put(elems.map(elem => Prepare.Remove(serializer.write(elem)))))

  def expire(elem: A, after: FiniteDuration): T[IO.OK] =
    wrapCall(core.remove(elem, after.fromNow))

  def expire(elem: A, at: Deadline): T[IO.OK] =
    wrapCall(core.remove(elem, at))

  def expire(from: A, to: A, after: FiniteDuration): T[IO.OK] =
    wrapCall(core.remove(from, to, after.fromNow))

  def expire(from: A, to: A, at: Deadline): T[IO.OK] =
    wrapCall(core.remove(from, to, at))

  def expire(elems: (A, Deadline)*): T[IO.OK] =
    expire(elems)

  def expire(elems: Stream[(A, Deadline), T]): T[IO.OK] =
    wrapCall(elems.materialize flatMap expire)

  def expire(elems: Iterable[(A, Deadline)]): T[IO.OK] =
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

  def clear(): T[IO.OK] =
    wrapCall(core.clear())

  def registerFunction(functionID: A, function: (A, Option[Deadline]) => Apply.Set[A]): A = {
    core.registerFunction(functionID, SwayDB.toCoreFunction(function))
    functionID
  }

  def applyFunction(from: A, to: A, functionID: A): T[IO.OK] =
    wrapCall(core.function(from, to, functionID))

  def applyFunction(elem: A, function: A): T[IO.OK] =
    wrapCall(core.function(elem, function))

  def commit(prepare: Prepare[A, Nothing]*): T[IO.OK] =
    wrapCall(core.put(prepare))

  def commit(prepare: Stream[Prepare[A, Nothing], T]): T[IO.OK] =
    wrapCall(prepare.materialize flatMap commit)

  def commit(prepare: Iterable[Prepare[A, Nothing]]): T[IO.OK] =
    wrapCall(core.put(prepare))

  def level0Meter: LevelZeroMeter =
    core.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    core.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    core.sizeOfSegments

  def elemSize(elem: A): Int =
    (elem: Slice[Byte]).size

  def expiration(elem: A): T[Option[Deadline]] =
    wrapCall(core deadline elem)

  def timeLeft(elem: A): T[Option[FiniteDuration]] =
    expiration(elem).map(_.map(_.timeLeft))

  def from(key: A): Set[A, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: A): Set[A, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: A): Set[A, T] =
    copy(from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: A): Set[A, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: A): Set[A, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)))

  override def headOption: T[Option[A]] =
    wrapCall {
      from match {
        case Some(from) =>
          val fromKeyBytes: Slice[Byte] = from.key
          if (from.before)
            core.beforeKey(fromKeyBytes)
          else if (from.after)
            core.afterKey(fromKeyBytes)
          else
            core.getKey(fromKeyBytes)
              .flatMap {
                case Some(key) =>
                  tag.success(Some(key)): T[Option[Slice[Byte]]]

                case _ =>
                  if (from.orAfter)
                    core.afterKey(fromKeyBytes)
                  else if (from.orBefore)
                    core.beforeKey(fromKeyBytes)
                  else
                    tag.success(None): T[Option[Slice[Byte]]]
              }

        case None =>
          if (reverseIteration) core.lastKey else core.headKey
      }
    } map (_.map(_.read[A]))

  override def drop(count: Int): Stream[A, T] =
    stream drop count

  override def dropWhile(f: A => Boolean): Stream[A, T] =
    stream dropWhile f

  override def take(count: Int): Stream[A, T] =
    stream take count

  override def takeWhile(f: A => Boolean): Stream[A, T] =
    stream takeWhile f

  override def map[B](f: A => B): Stream[B, T] =
    stream map f

  override def flatMap[B](f: A => Stream[B, T]): Stream[B, T] =
    stream flatMap f

  override def foreach[U](f: A => U): Stream[Unit, T] =
    stream foreach f

  override def filter(f: A => Boolean): Stream[A, T] =
    stream filter f

  override def filterNot(f: A => Boolean): Stream[A, T] =
    stream filterNot f

  override def foldLeft[B](initial: B)(f: (B, A) => B): T[B] =
    stream.foldLeft(initial)(f)

  def size: T[Int] =
    stream.size

  def stream: Stream[A, T] =
    new Stream[A, T] {
      override def headOption: T[Option[A]] =
        self.headOption

      override private[swaydb] def next(previous: A): T[Option[A]] =
        wrapCall {
          if (reverseIteration)
            core.beforeKey(serializer.write(previous))
          else
            core.afterKey(serializer.write(previous))
        } map (_.map(_.read[A]))
    }

  def sizeOfBloomFilterEntries: T[Int] =
    wrapCall(core.bloomFilterKeyValueCount)

  def isEmpty: T[Boolean] =
    wrapCall(core.headKey.map(_.isEmpty))

  def nonEmpty: T[Boolean] =
    isEmpty.map(!_)

  def lastOption: T[Option[A]] =
    if (reverseIteration)
      wrapCall(core.headKey.map(_.map(_.read[A])))
    else
      wrapCall(core.lastKey.map(_.map(_.read[A])))

  def reverse: Set[A, T] =
    copy(reverseIteration = true)

  def tagAsync[O[_]](implicit ec: ExecutionContext,
                     tag: Async[O]): Set[A, O] =
    copy(core = core.tagAsync[O])

  def tagBlocking[O[_]](implicit tag: Tag[O]): Set[A, O] =
    copy(core = core.tagBlocking[O])

  def asScala: scala.collection.mutable.Set[A] =
    ScalaSet[A](tagBlocking[SIO])

  def close(): T[Unit] =
    wrapCall(core.close())

  def delete(): T[Unit] =
    wrapCall(core.delete())

  override def toString(): String =
    classOf[Map[_, _, T]].getClass.getSimpleName
}