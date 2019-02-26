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
import swaydb.data.IO
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.slice.Slice
import swaydb.data.transaction.Prepare
import swaydb.serializers.{Serializer, _}

object Set {
  def apply[T](api: SwayDB)(implicit serializer: Serializer[T]): Set[T] =
    new Set(api, None)
}

/**
  * Set database API.
  *
  * For documentation check - http://swaydb.io/api/
  */
case class Set[T](private val db: SwayDB,
                  private val from: Option[From[T]],
                  private[swaydb] val reverse: Boolean = false,
                  private val till: T => Boolean = (_: T) => true)(implicit serializer: Serializer[T]) extends Iterable[T] {

  def get(elem: T): IO[Option[T]] =
    db.getKey(elem).map(_.map(_.read[T]))

  def contains(elem: T): IO[Boolean] =
    db contains elem

  def mightContain(elem: T): IO[Boolean] =
    db mightContain elem

  def add(elem: T): IO[Level0Meter] =
    db.put(key = elem)

  def add(elem: T, expireAt: Deadline): IO[Level0Meter] =
    db.put(elem, None, expireAt)

  def add(elem: T, expireAfter: FiniteDuration): IO[Level0Meter] =
    db.put(elem, None, expireAfter.fromNow)

  def add(elems: T*): IO[Level0Meter] =
    add(elems)

  def add(elems: Iterable[T]): IO[Level0Meter] =
    db.commit(elems.map(elem => Prepare.Put(key = serializer.write(elem), value = None, deadline = None)))

  def remove(elem: T): IO[Level0Meter] =
    db.remove(elem)

  def remove(from: T, to: T): IO[Level0Meter] =
    db.remove(from, to)

  def remove(elems: T*): IO[Level0Meter] =
    remove(elems)

  def remove(elems: Iterable[T]): IO[Level0Meter] =
    db.commit(elems.map(elem => Prepare.Remove(serializer.write(elem))))

  def expire(elem: T, after: FiniteDuration): IO[Level0Meter] =
    db.expire(elem, after.fromNow)

  def expire(elem: T, at: Deadline): IO[Level0Meter] =
    db.expire(elem, at)

  def expire(from: T, to: T, after: FiniteDuration): IO[Level0Meter] =
    db.expire(from, to, after.fromNow)

  def expire(from: T, to: T, at: Deadline): IO[Level0Meter] =
    db.expire(from, to, at)

  def expire(elems: (T, Deadline)*): IO[Level0Meter] =
    expire(elems)

  def expire(elems: Iterable[(T, Deadline)]): IO[Level0Meter] =
    db.commit {
      elems map {
        elemWithExpire =>
          Prepare.Remove(
            from = serializer.write(elemWithExpire._1),
            to = None,
            deadline = Some(elemWithExpire._2)
          )
      }
    }

  def registerFunction(functionID: T, function: T => Apply.Set[T]): T = {
    db.registerFunction(functionID, SwayDB.toCoreFunction(function))
    functionID
  }

  def registerFunction(functionID: T, function: (T, Option[Deadline]) => Apply.Set[T]): T = {
    db.registerFunction(functionID, SwayDB.toCoreFunction(function))
    functionID
  }

  def applyFunction(from: T, to: T, function: T): IO[Level0Meter] =
    db.function(from, to, function)

  def applyFunction(elem: T, function: T): IO[Level0Meter] =
    db.function(elem, function)

  def commit(prepare: Prepare[T, Nothing]*): IO[Level0Meter] =
    db.commit(prepare)

  def commit(prepare: Iterable[Prepare[T, Nothing]]): IO[Level0Meter] =
    db.commit(prepare)

  def level0Meter: Level0Meter =
    db.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    db.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    db.sizeOfSegments

  def elemSize(elem: T): Int =
    (elem: Slice[Byte]).size

  def expiration(elem: T): IO[Option[Deadline]] =
    db deadline elem

  def timeLeft(elem: T): IO[Option[FiniteDuration]] =
    expiration(elem).map(_.map(_.timeLeft))

  def from(key: T): Set[T] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: T) =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: T) =
    copy(from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: T) =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: T) =
    copy(from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)))

  def till(condition: T => Boolean) =
    copy(till = condition)

  override def iterator = new Iterator[T] {

    private var started: Boolean = false
    private var nextKeyBytes: Slice[Byte] = _
    private var nextKeyTyped: T = _

    private def start: IO[Option[Slice[Byte]]] =
      from match {
        case Some(from) =>
          val fromKeyBytes: Slice[Byte] = from.key
          if (from.before)
            db.beforeKey(fromKeyBytes)
          else if (from.after)
            db.afterKey(fromKeyBytes)
          else
            db.getKey(fromKeyBytes)
              .flatMap {
                case Some(key) =>
                  IO.Success(Some(key))

                case _ =>
                  if (from.orAfter)
                    db.afterKey(fromKeyBytes)
                  else if (from.orBefore)
                    db.beforeKey(fromKeyBytes)
                  else
                    IO.Success(None)
              }

        case None =>
          if (reverse)
            db.lastKey
          else
            db.headKey
      }

    override def hasNext: Boolean =
      if (started) {
        if (nextKeyBytes == null)
          false
        else {
          val next =
            if (reverse)
              db.beforeKey(nextKeyBytes)
            else
              db.afterKey(nextKeyBytes)

          next match {
            case IO.Success(key) =>
              key match {
                case Some(key) =>
                  val keyT = key.read[T]
                  if (till(keyT)) {
                    nextKeyBytes = key
                    nextKeyTyped = keyT
                    true
                  } else
                    false

                case _ =>
                  false
              }
            case IO.Failure(error) =>
              System.err.println("Failed to iterate", error)
              throw error.exception
          }
        }
      } else
        start match {
          case IO.Success(value) =>
            started = true
            value match {
              case Some(key) =>
                val keyT = key.read[T]
                if (till(keyT)) {
                  nextKeyBytes = key
                  nextKeyTyped = keyT
                  true
                } else
                  false

              case _ =>
                false
            }
          case IO.Failure(error) =>
            System.err.println("Failed to start Key iterator", error)
            throw error.exception
        }

    override def next(): T =
      nextKeyTyped

    override def toString(): String =
      classOf[Set[_]].getClass.getSimpleName
  }

  override def head: T =
    headOption.get

  override def last: T =
    lastOption.get

  override def size: Int =
    db.keyValueCount.get

  override def isEmpty: Boolean =
    db.headKey.get.isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

  override def headOption: Option[T] =
    if (from.isDefined)
      this.take(1).headOption
    else
      db.headKey.map(_.map(_.read[T])).get

  override def lastOption: Option[T] =
    db.lastKey.map(_.map(_.read[T])).get

  def foreachRight[U](f: T => U): Unit =
    copy(reverse = true) foreach f

  def mapRight[B, C](f: T => B)(implicit bf: CanBuildFrom[Iterable[T], B, C]): C = {
    copy(reverse = true) map f
  }

  override def foldRight[B](z: B)(op: (T, B) => B): B =
    copy(reverse = true).foldLeft(z) {
      case (b, k) =>
        op(k, b)
    }

  override def takeRight(n: Int): Iterable[T] =
    copy(reverse = true).take(n)

  override def dropRight(n: Int): Iterable[T] =
    copy(reverse = true).drop(n)

  override def reduceRight[B >: T](op: (T, B) => B): B =
    copy(reverse = true).reduceLeft[B] {
      case (b, k) =>
        op(k, b)
    }

  override def reduceRightOption[B >: T](op: (T, B) => B): Option[B] =
    copy(reverse = true).reduceLeftOption[B] {
      case (b, k) =>
        op(k, b)
    }

  override def scanRight[B, That](z: B)(op: (T, B) => B)(implicit bf: CanBuildFrom[Iterable[T], B, That]): That =
    copy(reverse = true).scanLeft(z) {
      case (z, k) =>
        op(k, z)
    }

  def closeDatabase(): IO[Unit] =
    db.close

  override def toString(): String =
    classOf[Map[_, _]].getClass.getSimpleName
}