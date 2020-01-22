/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

object Set {
  def apply[A, F, BAG[_]](api: Core[BAG])(implicit serializer: Serializer[A],
                                          bag: Bag[BAG]): Set[A, F, BAG] =
    new Set(api, None)
}

/**
 * Set database API.
 *
 * For documentation check - http://swaydb.io/api/
 */
case class Set[A, F, BAG[_]](private val core: Core[BAG],
                             private val from: Option[From[A]],
                             private[swaydb] val reverseIteration: Boolean = false)(implicit serializer: Serializer[A],
                                                                                    bag: Bag[BAG]) extends Streamable[A] { self =>

  def get(elem: A): BAG[Option[A]] =
    bag.map(core.getKey(elem, core.readStates.get()))(_.map(_.read[A]))

  def contains(elem: A): BAG[Boolean] =
    bag.point(core.contains(elem, core.readStates.get()))

  def mightContain(elem: A): BAG[Boolean] =
    bag.point(core mightContainKey elem)

  def mightContainFunction(functionId: A): BAG[Boolean] =
    bag.point(core mightContainFunction functionId)

  def add(elem: A): BAG[OK] =
    bag.point(core.put(key = elem))

  def add(elem: A, expireAt: Deadline): BAG[OK] =
    bag.point(core.put(elem, None, expireAt))

  def add(elem: A, expireAfter: FiniteDuration): BAG[OK] =
    bag.point(core.put(elem, None, expireAfter.fromNow))

  def add(elems: A*): BAG[OK] =
    add(elems)

  def add(elems: Stream[A]): BAG[OK] =
  //    bag.flatMap(elems.materialize)(add)
    ???

  def add(elems: Iterable[A]): BAG[OK] =
    add(elems.iterator)

  def add(elems: Iterator[A]): BAG[OK] =
    bag.point(core.put(elems.map(elem => Prepare.Put(key = serializer.write(elem), value = None, deadline = None))))

  def remove(elem: A): BAG[OK] =
    bag.point(core.remove(elem))

  def remove(from: A, to: A): BAG[OK] =
    bag.point(core.remove(from, to))

  def remove(elems: A*): BAG[OK] =
    remove(elems)

  def remove(elems: Stream[A]): BAG[OK] =
  //    bag.flatMap(elems.materialize)(remove)
    ???

  def remove(elems: Iterable[A]): BAG[OK] =
    remove(elems.iterator)

  def remove(elems: Iterator[A]): BAG[OK] =
    bag.point(core.put(elems.map(elem => Prepare.Remove(serializer.write(elem)))))

  def expire(elem: A, after: FiniteDuration): BAG[OK] =
    bag.point(core.remove(elem, after.fromNow))

  def expire(elem: A, at: Deadline): BAG[OK] =
    bag.point(core.remove(elem, at))

  def expire(from: A, to: A, after: FiniteDuration): BAG[OK] =
    bag.point(core.remove(from, to, after.fromNow))

  def expire(from: A, to: A, at: Deadline): BAG[OK] =
    bag.point(core.remove(from, to, at))

  def expire(elems: (A, Deadline)*): BAG[OK] =
    expire(elems)

  def expire(elems: Stream[(A, Deadline)]): BAG[OK] =
  //    bag.flatMap(elems.materialize)(expire)
    ???

  def expire(elems: Iterable[(A, Deadline)]): BAG[OK] =
    expire(elems.iterator)

  def expire(elems: Iterator[(A, Deadline)]): BAG[OK] =
    bag.point {
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

  def clear(): BAG[OK] =
    bag.point(core.clear(core.readStates.get()))

  def registerFunction[PF <: F](function: PF)(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]): BAG[OK] =
    core.registerFunction(function.id, SwayDB.toCoreFunction(function))

  def applyFunction[PF <: F](from: A, to: A, function: PF)(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]): BAG[OK] =
    bag.point(core.function(from, to, function.id))

  def applyFunction[PF <: F](elem: A, function: PF)(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]): BAG[OK] =
    bag.point(core.function(elem, function.id))

  def commit[PF <: F](prepare: Prepare[A, Nothing, PF]*)(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]): BAG[OK] =
    bag.point(core.put(preparesToUntyped(prepare).iterator))

  def commit[PF <: F](prepare: Stream[Prepare[A, Nothing, PF]])(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]): BAG[OK] =
  //    bag.flatMap(prepare.materialize) {
  //      statements =>
  //        commit(statements)
  //    }
    ???

  def commit[PF <: F](prepare: Iterable[Prepare[A, Nothing, PF]])(implicit ev: PF <:< swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]): BAG[OK] =
    bag.point(core.put(preparesToUntyped(prepare).iterator))

  def levelZeroMeter: LevelZeroMeter =
    core.levelZeroMeter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    core.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    core.sizeOfSegments

  def elemSize(elem: A): Int =
    (elem: Slice[Byte]).size

  def expiration(elem: A): BAG[Option[Deadline]] =
    bag.point(core.deadline(elem, core.readStates.get()))

  def timeLeft(elem: A): BAG[Option[FiniteDuration]] =
    bag.map(expiration(elem))(_.map(_.timeLeft))

  def from(key: A): Set[A, F, BAG] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: A): Set[A, F, BAG] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: A): Set[A, F, BAG] =
    copy(from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: A): Set[A, F, BAG] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: A): Set[A, F, BAG] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)))

  override def headOption[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[A]] =
  //    headOption(core.readStates.get())
    ???

  protected def headOption(readState: ThreadReadState): BAG[Option[A]] =
    bag.map {
      from match {
        case Some(from) =>
          val fromKeyBytes: Slice[Byte] = from.key
          if (from.before)
            core.beforeKey(fromKeyBytes, readState)
          else if (from.after)
            core.afterKey(fromKeyBytes, readState)
          else
            bag.flatMap(core.getKey(fromKeyBytes, readState)) {
              case Some(key) =>
                bag.success(Some(key)): BAG[Option[Slice[Byte]]]

              case _ =>
                if (from.orAfter)
                  core.afterKey(fromKeyBytes, readState)
                else if (from.orBefore)
                  core.beforeKey(fromKeyBytes, readState)
                else
                  bag.success(None): BAG[Option[Slice[Byte]]]
            }

        case None =>
          if (reverseIteration) core.lastKey(readState) else core.headKey(readState)

      }
    }(_.map(_.read[A]))

  override def drop(count: Int): Stream[A] =
    stream drop count

  override def dropWhile(f: A => Boolean): Stream[A] =
    stream dropWhile f

  override def take(count: Int): Stream[A] =
    stream take count

  override def takeWhile(f: A => Boolean): Stream[A] =
    stream takeWhile f

  override def map[B](f: A => B): Stream[B] =
    stream map f

  override def flatMap[B](f: A => Stream[B]): Stream[B] =
    stream flatMap f

  override def foreach[U](f: A => U): Stream[Unit] =
    stream foreach f

  override def filter(f: A => Boolean): Stream[A] =
    stream filter f

  override def filterNot(f: A => Boolean): Stream[A] =
    stream filterNot f

  override def foldLeft[B, BAG[_]](initial: B)(f: (B, A) => B)(implicit bag: Bag[BAG]): BAG[B] =
    stream.foldLeft(initial)(f)

  def size[BAG[_]](implicit bag: Bag[BAG]): BAG[Int] =
    stream.size

  def stream: Stream[A] =
  //    new Stream[A] {
  //      override def headOption: BAG[Option[A]] =
  //        self.headOption
  //
  //      override private[swaydb] def next(previous: A): BAG[Option[A]] =
  //        bag.map {
  //          if (reverseIteration)
  //            core.beforeKey(serializer.write(previous), core.readStates.get())
  //          else
  //            core.afterKey(serializer.write(previous), core.readStates.get())
  //
  //        }(_.map(_.read[A]))
  //    }
    ???

  def streamer: Streamer[A] =
  //    stream.streamer
    ???

  def sizeOfBloomFilterEntries: BAG[Int] =
    bag.point(core.bloomFilterKeyValueCount)

  def isEmpty: BAG[Boolean] =
    bag.map(core.headKey(core.readStates.get()))(_.isEmpty)

  def nonEmpty: BAG[Boolean] =
    bag.map(isEmpty)(!_)

  def lastOption[BAG[_]](implicit bag: Bag[BAG]): BAG[Option[A]] =
  //    if (reverseIteration)
  //      bag.map(core.headKey(core.readStates.get()))(_.map(_.read[A]))
  //    else
  //      bag.map(core.lastKey(core.readStates.get()))(_.map(_.read[A]))
    ???

  def reverse: Set[A, F, BAG] =
    copy(reverseIteration = true)

  def toBag[X[_]](implicit bag: Bag[X]): Set[A, F, X] =
    copy(core = core.toBag[X])

  def asScala: scala.collection.mutable.Set[A] =
    ScalaSet[A, F](toBag[IO.ApiIO])

  def close(): BAG[Unit] =
    bag.point(core.close())

  def delete(): BAG[Unit] =
    bag.point(core.delete())

  override def toString(): String =
    classOf[Map[_, _, _, BAG]].getClass.getSimpleName
}