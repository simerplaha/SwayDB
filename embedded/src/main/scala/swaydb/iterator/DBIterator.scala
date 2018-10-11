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
import swaydb.serializers._

import scala.collection.generic.CanBuildFrom
import scala.util.{Failure, Success, Try}

/**
  * Implements APIs for databases iteration.
  *
  * Documentation on iteration API - http://www.swaydb.io/api/iteration-api
  *
  * This iterator and [[KeysIterator]] share a lot of the same code. A higher type is required.
  */
case class DBIterator[K, V](private val db: SwayDB,
                            private val from: Option[From[K]],
                            private val reverse: Boolean = false,
                            private val till: (K, V) => Boolean = (_: K, _: V) => true)(implicit keySerializer: Serializer[K],
                                                                                        valueSerializer: Serializer[V]) extends Iterable[(K, V)] {

  def from(key: K): DBIterator[K, V] =
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

  override def iterator = new Iterator[(K, V)] {

    private var started: Boolean = false
    private var nextKeyValueBytes: (Slice[Byte], Option[Slice[Byte]]) = _
    private var nextKeyValueTyped: (K, V) = _

    private def start: Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
      from match {
        case Some(from) =>
          val fromKeyBytes: Slice[Byte] = from.key
          if (from.before)
            db.before(fromKeyBytes)
          else if (from.after)
            db.after(fromKeyBytes)
          else
            db.getKeyValue(fromKeyBytes)
              .flatMap {
                case Some((key, valueOption)) =>
                  Success(Some(key, valueOption))
                case _ =>
                  if (from.orAfter)
                    db.after(fromKeyBytes)
                  else if (from.orBefore)
                    db.before(fromKeyBytes)
                  else
                    Success(None)
              }

        case None =>
          if (reverse)
            db.last
          else
            db.head
      }

    override def hasNext: Boolean =
      if (started)
        if (nextKeyValueBytes == null)
          false
        else {
          val next =
            if (reverse)
              db.before(nextKeyValueBytes._1)
            else
              db.after(nextKeyValueBytes._1)

          next match {
            case Success(value) =>
              value match {
                case Some(keyValue @ (key, value)) =>
                  val keyT = key.read[K]
                  val valueT = value.read[V]
                  if (till(keyT, valueT)) {
                    nextKeyValueBytes = keyValue
                    nextKeyValueTyped = (keyT, valueT)
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
      else
        start match {
          case Success(value) =>
            started = true
            value match {
              case Some(keyValue @ (key, value)) =>
                val keyT = key.read[K]
                val valueT = value.read[V]
                if (till(keyT, valueT)) {
                  nextKeyValueBytes = keyValue
                  nextKeyValueTyped = (keyT, valueT)
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

    override def next(): (K, V) =
      nextKeyValueTyped

    override def toString(): String =
      classOf[DBIterator[_, _]].getClass.getSimpleName
  }

  override def size: Int =
    sizeTry.get

  def sizeTry: Try[Int] =
    db.keyValueCount

  override def isEmpty: Boolean =
    db.headKey.get.isEmpty

  override def nonEmpty: Boolean =
    !isEmpty

  override def head: (K, V) =
    headOption.get

  override def last: (K, V) =
    lastOption.get

  override def headOption: Option[(K, V)] =
    headOptionTry.get

  private def headOptionTry: Try[Option[(K, V)]] =
    if (from.isDefined)
      Try(this.take(1).headOption)
    else
      db.head map {
        case Some((key, value)) =>
          Some(key.read[K], value.read[V])
        case _ =>
          None
      }

  override def lastOption: Option[(K, V)] =
    lastOptionTry.get

  private def lastOptionTry: Try[Option[(K, V)]] =
    db.last map {
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

  def mapRight[B, T](f: (K, V) => B)(implicit bf: CanBuildFrom[Iterable[(K, V)], B, T]): T = {
    copy(reverse = true) map {
      case (k, v) =>
        f(k, v)
    }
  }

  override def foldRight[B](z: B)(op: ((K, V), B) => B): B =
    copy(reverse = true).foldLeft(z) {
      case (b, (k, v)) =>
        op((k, v), b)
    }

  override def takeRight(n: Int): Iterable[(K, V)] =
    copy(reverse = true).take(n)

  override def dropRight(n: Int): Iterable[(K, V)] =
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

  override def scanRight[B, That](z: B)(op: ((K, V), B) => B)(implicit bf: CanBuildFrom[Iterable[(K, V)], B, That]): That =
    copy(reverse = true).scanLeft(z) {
      case (z, (k, v)) =>
        op((k, v), z)
    }

  override def toString(): String =
    classOf[DBIterator[_, _]].getClass.getSimpleName
}
