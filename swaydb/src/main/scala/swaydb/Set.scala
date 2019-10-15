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

import swaydb.PrepareImplicits._
import swaydb.Tag.Implicits._
import swaydb.core.Core
import swaydb.core.segment.ReadState
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.serializers.{Serializer, _}

import scala.concurrent.duration.{Deadline, FiniteDuration}

object Set {
  def apply[A, F, T[_]](api: Core[T])(implicit serializer: Serializer[A],
                                      tag: Tag[T]): Set[A, F, T] =
    new Set(api, None)
}

/**
 * Set database API.
 *
 * For documentation check - http://swaydb.io/api/
 */
case class Set[A, F, T[_]](private val core: Core[T],
                           private val from: Option[From[A]],
                           private[swaydb] val reverseIteration: Boolean = false)(implicit serializer: Serializer[A],
                                                                                  tag: Tag[T]) extends Streamable[A, T] { self =>

  def get(elem: A): T[Option[A]] =
    tag.point(core.getKey(elem, core.readStates.get()).map(_.map(_.read[A])))

  def contains(elem: A): T[Boolean] =
    tag.point(core.contains(elem, core.readStates.get()))

  def mightContain(elem: A): T[Boolean] =
    tag.point(core mightContainKey elem)

  def mightContainFunction(functionId: A): T[Boolean] =
    tag.point(core mightContainFunction functionId)

  def add(elem: A): T[IO.Done] =
    tag.point(core.put(key = elem))

  def add(elem: A, expireAt: Deadline): T[IO.Done] =
    tag.point(core.put(elem, None, expireAt))

  def add(elem: A, expireAfter: FiniteDuration): T[IO.Done] =
    tag.point(core.put(elem, None, expireAfter.fromNow))

  def add(elems: A*): T[IO.Done] =
    add(elems)

  def add(elems: Stream[A, T]): T[IO.Done] =
    tag.point(elems.materialize flatMap add)

  def add(elems: Iterable[A]): T[IO.Done] =
    tag.point(core.put(elems.map(elem => Prepare.Put(key = serializer.write(elem), value = None, deadline = None))))

  def remove(elem: A): T[IO.Done] =
    tag.point(core.remove(elem))

  def remove(from: A, to: A): T[IO.Done] =
    tag.point(core.remove(from, to))

  def remove(elems: A*): T[IO.Done] =
    remove(elems)

  def remove(elems: Stream[A, T]): T[IO.Done] =
    tag.point(elems.materialize flatMap remove)

  def remove(elems: Iterable[A]): T[IO.Done] =
    tag.point(core.put(elems.map(elem => Prepare.Remove(serializer.write(elem)))))

  def expire(elem: A, after: FiniteDuration): T[IO.Done] =
    tag.point(core.remove(elem, after.fromNow))

  def expire(elem: A, at: Deadline): T[IO.Done] =
    tag.point(core.remove(elem, at))

  def expire(from: A, to: A, after: FiniteDuration): T[IO.Done] =
    tag.point(core.remove(from, to, after.fromNow))

  def expire(from: A, to: A, at: Deadline): T[IO.Done] =
    tag.point(core.remove(from, to, at))

  def expire(elems: (A, Deadline)*): T[IO.Done] =
    expire(elems)

  def expire(elems: Stream[(A, Deadline), T]): T[IO.Done] =
    tag.point(elems.materialize flatMap expire)

  def expire(elems: Iterable[(A, Deadline)]): T[IO.Done] =
    tag.point {
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

  def clear(): T[IO.Done] =
    tag.point(core.clear(core.readStates.get()))

  def registerFunction[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing]): T[IO.Done] =
    core.registerFunction(function.id, SwayDB.toCoreFunction(function))

  def applyFunction[PF <: F](from: A, to: A, function: PF)(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing]): T[IO.Done] =
    tag.point(core.function(from, to, function.id))

  def applyFunction[PF <: F](elem: A, function: PF)(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing]): T[IO.Done] =
    tag.point(core.function(elem, function.id))

  def commit[PF <: F](prepare: Prepare[A, Nothing, PF]*)(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing]): T[IO.Done] =
    tag.point(core.put(prepare))

  def commit[PF <: F](prepare: Stream[Prepare[A, Nothing, PF], T])(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing]): T[IO.Done] =
    tag.point {
      prepare.materialize flatMap {
        statements =>
          commit(statements)
      }
    }

  def commit[PF <: F](prepare: Iterable[Prepare[A, Nothing, PF]])(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing]): T[IO.Done] =
    tag.point(core.put(prepare))

  def levelZeroMeter: LevelZeroMeter =
    core.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    core.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    core.sizeOfSegments

  def elemSize(elem: A): Int =
    (elem: Slice[Byte]).size

  def expiration(elem: A): T[Option[Deadline]] =
    tag.point(core.deadline(elem, core.readStates.get()))

  def timeLeft(elem: A): T[Option[FiniteDuration]] =
    expiration(elem).map(_.map(_.timeLeft))

  def from(key: A): Set[A, F, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: A): Set[A, F, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: A): Set[A, F, T] =
    copy(from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: A): Set[A, F, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: A): Set[A, F, T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)))

  override def headOption: T[Option[A]] =
    headOption(core.readStates.get())

  protected def headOption(readState: ReadState): T[Option[A]] =
    tag.point {
      from match {
        case Some(from) =>
          val fromKeyBytes: Slice[Byte] = from.key
          if (from.before)
            core.beforeKey(fromKeyBytes, readState)
          else if (from.after)
            core.afterKey(fromKeyBytes, readState)
          else
            core.getKey(fromKeyBytes, readState)
              .flatMap {
                case Some(key) =>
                  tag.success(Some(key)): T[Option[Slice[Byte]]]

                case _ =>
                  if (from.orAfter)
                    core.afterKey(fromKeyBytes, readState)
                  else if (from.orBefore)
                    core.beforeKey(fromKeyBytes, readState)
                  else
                    tag.success(None): T[Option[Slice[Byte]]]
              }

        case None =>
          if (reverseIteration) core.lastKey(readState) else core.headKey(readState)
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
        tag.point {
          if (reverseIteration)
            core.beforeKey(serializer.write(previous), core.readStates.get())
          else
            core.afterKey(serializer.write(previous), core.readStates.get())
        } map (_.map(_.read[A]))
    }

  def sizeOfBloomFilterEntries: T[Int] =
    tag.point(core.bloomFilterKeyValueCount)

  def isEmpty: T[Boolean] =
    tag.point(core.headKey(core.readStates.get()).map(_.isEmpty))

  def nonEmpty: T[Boolean] =
    isEmpty.map(!_)

  def lastOption: T[Option[A]] =
    if (reverseIteration)
      tag.point(core.headKey(core.readStates.get()).map(_.map(_.read[A])))
    else
      tag.point(core.lastKey(core.readStates.get()).map(_.map(_.read[A])))

  def reverse: Set[A, F, T] =
    copy(reverseIteration = true)

  def toTag[X[_]](implicit tag: Tag[X]): Set[A, F, X] =
    copy(core = core.toTag[X])

  def asScala: scala.collection.mutable.Set[A] =
    ScalaSet[A, F](toTag[IO.ApiIO])

  def close(): T[Unit] =
    tag.point(core.close())

  def delete(): T[Unit] =
    tag.point(core.delete())

  override def toString(): String =
    classOf[Map[_, _, _, T]].getClass.getSimpleName
}