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
import swaydb.data.accelerate.Level0Meter
import swaydb.data.compaction.LevelMeter
import swaydb.data.io.IO
import swaydb.data.slice.Slice
import swaydb.data.transaction.Prepare
import swaydb.serializers.{Serializer, _}

object Map {

  def apply[K, V](db: SwayDB)(implicit keySerializer: Serializer[K],
                              valueSerializer: Serializer[V]): Map[K, V] = {
    new Map[K, V](db, None)
  }
}

/**
  * Map database API.
  *
  * For documentation check - http://swaydb.io/api/
  */
case class Map[K, V](private[swaydb] val db: SwayDB,
                     private val from: Option[From[K]],
                     private[swaydb] val reverse: Boolean = false,
                     private val till: (K, V) => Boolean = (_: K, _: V) => true)(implicit keySerializer: Serializer[K],
                                                                                 valueSerializer: Serializer[V]) extends Iterable[(K, V)] {

  def put(key: K, value: V): IO[Level0Meter] =
    db.put(key = key, value = Some(value))

  def put(key: K, value: V, expireAfter: FiniteDuration): IO[Level0Meter] =
    db.put(key, Some(value), expireAfter.fromNow)

  def put(key: K, value: V, expireAt: Deadline): IO[Level0Meter] =
    db.put(key, Some(value), expireAt)

  def put(keyValues: (K, V)*): IO[Level0Meter] =
    put(keyValues)

  def put(keyValues: Iterable[(K, V)]): IO[Level0Meter] =
    db.commit {
      keyValues map {
        case (key, value) =>
          Prepare.Put(keySerializer.write(key), Some(valueSerializer.write(value)), None)
      }
    }

  def remove(key: K): IO[Level0Meter] =
    db.remove(key)

  def remove(from: K, to: K): IO[Level0Meter] =
    db.remove(from, to)

  def remove(keys: K*): IO[Level0Meter] =
    remove(keys)

  def remove(keys: Iterable[K]): IO[Level0Meter] =
    db.commit(keys.map(key => Prepare.Remove(keySerializer.write(key))))

  def expire(key: K, after: FiniteDuration): IO[Level0Meter] =
    db.expire(key, after.fromNow)

  def expire(key: K, at: Deadline): IO[Level0Meter] =
    db.expire(key, at)

  def expire(from: K, to: K, after: FiniteDuration): IO[Level0Meter] =
    db.expire(from, to, after.fromNow)

  def expire(from: K, to: K, at: Deadline): IO[Level0Meter] =
    db.expire(from, to, at)

  def expire(keys: (K, Deadline)*): IO[Level0Meter] =
    expire(keys)

  def expire(keys: Iterable[(K, Deadline)]): IO[Level0Meter] =
    db.commit {
      keys map {
        keyDeadline =>
          Prepare.Remove(
            from = keySerializer.write(keyDeadline._1),
            to = None,
            deadline = Some(keyDeadline._2)
          )
      }
    }

  def update(key: K, value: V): IO[Level0Meter] =
    db.update(key, Some(value))

  def update(from: K, to: K, value: V): IO[Level0Meter] =
    db.update(from, to, Some(value))

  def update(keyValues: (K, V)*): IO[Level0Meter] =
    update(keyValues)

  def update(keyValues: Iterable[(K, V)]): IO[Level0Meter] =
    db.commit {
      keyValues map {
        case (key, value) =>
          Prepare.Update(keySerializer.write(key), Some(valueSerializer.write(value)))
      }
    }

  def registerFunction(functionID: K, function: V => Apply.Map[V]): K = {
    db.registerFunction(functionID, SwayDB.toCoreFunction(function))
    functionID
  }

  def registerFunction(functionID: K, function: (K, Option[Deadline]) => Apply.Map[V]): K = {
    db.registerFunction(functionID, SwayDB.toCoreFunction(function))
    functionID
  }

  def registerFunction(functionID: K, function: (K, V, Option[Deadline]) => Apply.Map[V]): K = {
    db.registerFunction(functionID, SwayDB.toCoreFunction(function))
    functionID
  }

  def applyFunction(key: K, functionID: K): IO[Level0Meter] =
    db.function(key, functionID)

  def applyFunction(from: K, to: K, functionID: K): IO[Level0Meter] =
    db.function(from, to, functionID)

  def commit(prepare: Prepare[K, V]*): IO[Level0Meter] =
    db.commit(prepare)

  def commit(prepare: Iterable[Prepare[K, V]]): IO[Level0Meter] =
    db.commit(prepare)

  /**
    * Returns target value for the input key.
    */
  def get(key: K): IO[Option[V]] =
    db.get(key).map(_.map(_.read[V]))

  /**
    * Returns target full key for the input partial key.
    *
    * This function is mostly used for Set databases where partial ordering on the Key is provided.
    */
  def getKey(key: K): IO[Option[K]] =
    db.getKey(key).map(_.map(_.read[K]))

  def getKeyValue(key: K): IO[Option[(K, V)]] =
    db.getKeyValue(key).map(_.map {
      case (key, value) =>
        (key.read[K], value.read[V])
    })

  def contains(key: K): IO[Boolean] =
    db contains key

  def mightContain(key: K): IO[Boolean] =
    db mightContain key

  def keys: Set[K] =
    Set[K](db, None)(keySerializer)

  def level0Meter: Level0Meter =
    db.level0Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    db.levelMeter(levelNumber)

  def sizeOfSegments: Long =
    db.sizeOfSegments

  def keySize(key: K): Int =
    (key: Slice[Byte]).size

  def valueSize(value: V): Int =
    (value: Slice[Byte]).size

  def expiration(key: K): IO[Option[Deadline]] =
    db deadline key

  def timeLeft(key: K): IO[Option[FiniteDuration]] =
    expiration(key).map(_.map(_.timeLeft))

  def from(key: K): Map[K, V] =
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

    private def start: IO[Option[(Slice[Byte], Option[Slice[Byte]])]] =
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
                  IO.Success(Some(key, valueOption))
                case _ =>
                  if (from.orAfter)
                    db.after(fromKeyBytes)
                  else if (from.orBefore)
                    db.before(fromKeyBytes)
                  else
                    IO.Success(None)
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
            case IO.Success(value) =>
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
            case IO.Failure(error) =>
              System.err.println("Failed to iterate", error)
              throw error.exception
          }
        }
      else
        start match {
          case IO.Success(value) =>
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
          case IO.Failure(error) =>
            System.err.println("Failed to start Key iterator", error)
            throw error.exception
        }

    override def next(): (K, V) =
      nextKeyValueTyped

    override def toString(): String =
      classOf[Map[_, _]].getClass.getSimpleName
  }

  override def size: Int =
    sizeIO.get

  def sizeIO: IO[Int] =
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
    headOptionIO.get

  private def headOptionIO: IO[Option[(K, V)]] =
    if (from.isDefined)
      IO(this.take(1).headOption)
    else
      db.head map {
        case Some((key, value)) =>
          Some(key.read[K], value.read[V])
        case _ =>
          None
      }

  override def lastOption: Option[(K, V)] =
    lastOptionIO.get

  private def lastOptionIO: IO[Option[(K, V)]] =
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
    classOf[Map[_, _]].getClass.getSimpleName
}