/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb.iterator

import swaydb.SwayDB
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer
import swaydb.serializers._

import scala.collection.generic.CanBuildFrom
import scala.util.{Failure, Success, Try}

/**
  * Implements APIs for key only databases iteration.
  *
  * Documentation on iteration API - http://www.swaydb.io/api/iteration-api
  *
  * This iterator and [[DBIterator]] share a lot of the same code. A higher type is required.
  */
case class DBKeysIterator[K](private[iterator] val db: SwayDB,
                             private[iterator] val from: Option[From[K]],
                             private val reverse: Boolean = false,
                             private val till: K => Boolean = (_: K) => true)(implicit serializer: Serializer[K]) extends Iterable[K] {

  def from(key: K): DBKeysIterator[K] =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = false)))

  def before(key: K) =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = true, after = false)))

  def fromOrBefore(key: K) =
    copy(from = Some(From(key = key, orBefore = true, orAfter = false, before = false, after = false)))

  def after(key: K) =
    copy(from = Some(From(key = key, orBefore = false, orAfter = false, before = false, after = true)))

  def fromOrAfter(key: K) =
    copy(from = Some(From(key = key, orBefore = false, orAfter = true, before = false, after = false)))

  def till(condition: K => Boolean) =
    copy(till = condition)

  override def iterator = new Iterator[K] {

    private var started: Boolean = false
    private var nextKeyBytes: Slice[Byte] = _
    private var nextKeyTyped: K = _

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
                  val keyT = key.read[K]
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
                val keyT = key.read[K]
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

    override def next(): K =
      nextKeyTyped

    override def toString(): String =
      classOf[DBKeysIterator[_]].getClass.getSimpleName
  }

  override def head: K =
    headOption.get

  override def last: K =
    lastOption.get

  override def size: Int =
    db.keyValueCount.get

  override def isEmpty: Boolean =
    db.headKey.get.isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

  override def headOption: Option[K] =
    if (from.isDefined)
      this.take(1).headOption
    else
      db.headKey.map(_.map(_.read[K])).get

  override def lastOption: Option[K] =
    db.lastKey.map(_.map(_.read[K])).get

  def foreachRight[U](f: K => U): Unit =
    copy(reverse = true) foreach f

  def mapRight[B, T](f: K => B)(implicit bf: CanBuildFrom[Iterable[K], B, T]): T = {
    copy(reverse = true) map f
  }

  override def foldRight[B](z: B)(op: (K, B) => B): B =
    copy(reverse = true).foldLeft(z) {
      case (b, k) =>
        op(k, b)
    }

  override def takeRight(n: Int): Iterable[K] =
    copy(reverse = true).take(n)

  override def dropRight(n: Int): Iterable[K] =
    copy(reverse = true).drop(n)

  override def reduceRight[B >: K](op: (K, B) => B): B =
    copy(reverse = true).reduceLeft[B] {
      case (b, k) =>
        op(k, b)
    }

  override def reduceRightOption[B >: K](op: (K, B) => B): Option[B] =
    copy(reverse = true).reduceLeftOption[B] {
      case (b, k) =>
        op(k, b)
    }

  override def scanRight[B, That](z: B)(op: (K, B) => B)(implicit bf: CanBuildFrom[Iterable[K], B, That]): That =
    copy(reverse = true).scanLeft(z) {
      case (z, k) =>
        op(k, z)
    }

  override def toString(): String =
    classOf[DBKeysIterator[_]].getClass.getSimpleName
}
