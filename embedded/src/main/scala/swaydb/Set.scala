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
import scala.util.{Failure, Success, Try}
import swaydb.BatchImplicits._
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.request
import swaydb.data.slice.Slice
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

  def get(elem: T): Try[Option[T]] =
    db.getKey(elem).map(_.map(_.read[T]))

  def contains(elem: T): Try[Boolean] =
    db contains elem

  def mightContain(elem: T): Try[Boolean] =
    db mightContain elem

  def add(elem: T): Try[Level0Meter] =
    db.put(key = elem)

  def add(elem: T, expireAt: Deadline): Try[Level0Meter] =
    db.put(elem, None, expireAt)

  def add(elem: T, expireAfter: FiniteDuration): Try[Level0Meter] =
    db.put(elem, None, expireAfter.fromNow)

  def remove(elem: T): Try[Level0Meter] =
    db.remove(elem)

  def remove(from: T, to: T): Try[Level0Meter] =
    db.remove(from, to)

  def expire(elem: T, after: FiniteDuration): Try[Level0Meter] =
    db.expire(elem, after.fromNow)

  def expire(elem: T, at: Deadline): Try[Level0Meter] =
    db.expire(elem, at)

  def expire(from: T, to: T, after: FiniteDuration): Try[Level0Meter] =
    db.expire(from, to, after.fromNow)

  def expire(from: T, to: T, at: Deadline): Try[Level0Meter] =
    db.expire(from, to, at)

  def batch(batch: Batch[T, Nothing]*): Try[Level0Meter] =
    db.batch(batch)

  def batch(batch: Iterable[Batch[T, Nothing]]): Try[Level0Meter] =
    db.batch(batch)

  def batchAdd(elems: T*): Try[Level0Meter] =
    batchAdd(elems)

  def batchAdd(elems: Iterable[T]): Try[Level0Meter] =
    db.batch(elems.map(elem => request.Batch.Put(elem, None, None)))

  def batchRemove(elems: T*): Try[Level0Meter] =
    batchRemove(elems)

  def batchRemove(elems: Iterable[T]): Try[Level0Meter] =
    db.batch(elems.map(elem => request.Batch.Remove(elem, None)))

  def batchExpire(elems: (T, Deadline)*): Try[Level0Meter] =
    batchExpire(elems)

  def batchExpire(elems: Iterable[(T, Deadline)]): Try[Level0Meter] =
    db.batch(elems.map(elemWithExpire => request.Batch.Remove(elemWithExpire._1, Some(elemWithExpire._2))))

  def level0Meter: Level0Meter =
    db.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    db.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    db.sizeOfSegments

  def elemSize(elem: T): Int =
    (elem: Slice[Byte]).size

  def expiration(elem: T): Try[Option[Deadline]] =
    db deadline elem

  def timeLeft(elem: T): Try[Option[FiniteDuration]] =
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

    private def start: Try[Option[Slice[Byte]]] =
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
                  Success(Some(key))

                case _ =>
                  if (from.orAfter)
                    db.afterKey(fromKeyBytes)
                  else if (from.orBefore)
                    db.beforeKey(fromKeyBytes)
                  else
                    Success(None)
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
            case Success(key) =>
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
            case Failure(exception) =>
              System.err.println("Failed to iterate", exception)
              throw exception
          }
        }
      } else
        start match {
          case Success(value) =>
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
          case Failure(exception) =>
            System.err.println("Failed to start Key iterator", exception)
            throw exception
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

  override def toString(): String =
    classOf[Map[_, _]].getClass.getSimpleName
}