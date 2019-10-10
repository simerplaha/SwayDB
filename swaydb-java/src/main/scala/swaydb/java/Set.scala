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

package swaydb.java

import java.util
import java.util.Optional
import java.util.function.{Consumer, Predicate}

import swaydb.Prepare
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.java.data.util.Javaz._
import swaydb.java.data.util.Pair

import scala.collection.JavaConverters._
import scala.compat.java8.DurationConverters._
import scala.compat.java8.OptionConverters._
import scala.concurrent.duration.Deadline

/**
 * Set database API.
 *
 * For documentation check - http://swaydb.io/
 */
case class Set[A, F](asScala: swaydb.Set[A, F, swaydb.IO.ThrowableIO]) {

  implicit val exceptionHandler = swaydb.IO.ExceptionHandler.Throwable

  private implicit def toIO[Throwable, R](io: swaydb.IO[scala.Throwable, R]): IO[scala.Throwable, R] = IO.toIO(io)

  def get(elem: A): IO[scala.Throwable, Optional[A]] =
    asScala.get(elem).map(_.asJava)

  def contains(elem: A): IO[scala.Throwable, Boolean] =
    asScala.contains(elem)

  def mightContain(elem: A): IO[scala.Throwable, Boolean] =
    asScala.mightContain(elem)

  def mightContainFunction(functionId: A): IO[scala.Throwable, Boolean] =
    asScala mightContainFunction functionId

  def add(elem: A): IO[scala.Throwable, swaydb.IO.Done] =
    asScala add elem

  def add(elem: A, expireAt: Deadline): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.add(elem, expireAt)

  def add(elem: A, expireAfter: java.time.Duration): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.add(elem, expireAfter.toScala)

  def add(elems: A*): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.add(elems)

  def add(elems: Stream[A]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.add(elems.asScala)

  def add(elems: java.util.Iterator[A]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.add(elems.asScala.toIterable)

  def remove(elem: A): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.remove(elem)

  def remove(from: A, to: A): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.remove(from, to)

  def remove(elems: A*): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.remove(elems)

  def remove(elems: Stream[A]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.remove(elems.asScala)

  def remove(elems: java.util.Iterator[A]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.remove(elems.asScala.toIterable)

  def expire(elem: A, after: java.time.Duration): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.expire(elem, after.toScala)

  def expire(from: A, to: A, after: java.time.Duration): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.expire(from, to, after.toScala)

  def expire(elems: Pair[A, java.time.Duration]*): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.expire {
      elems.map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def expire(elems: Stream[Pair[A, java.time.Duration]]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.expire {
      elems.asScala.map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def expire(elems: java.util.Iterator[Pair[A, java.time.Duration]]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.expire {
      elems.asScala map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      } toIterable
    }

  def clear(): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.clear()

  def registerFunction(function: F with swaydb.Function.GetKey[A, Nothing]): Unit =
    asScala.registerFunction(function)

  def applyFunction(from: A, to: A, function: F with swaydb.Function.GetKey[A, Nothing]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.applyFunction(from, to, function)

  def applyFunction(elem: A, function: F with swaydb.Function.GetKey[A, Nothing]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.applyFunction(elem, function)

  def commit(prepare: Prepare[A, Nothing]*): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.commit(prepare)

  def commit(prepare: Stream[Prepare[A, Nothing]]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.commit(prepare.asScala)

  def commit(prepare: java.util.Iterator[Prepare[A, Nothing]]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.commit(prepare.asScala.toIterable)

  def levelZeroMeter: LevelZeroMeter =
    asScala.levelZeroMeter

  def levelMeter(levelNumber: Int): Optional[LevelMeter] =
    asScala.levelMeter(levelNumber).asJava

  def sizeOfSegments: Long =
    asScala.sizeOfSegments

  def elemSize(elem: A): Int =
    asScala.elemSize(elem)

  def expiration(elem: A): IO[scala.Throwable, Optional[Deadline]] =
    asScala.expiration(elem).map(_.asJava)

  def timeLeft(elem: A): IO[scala.Throwable, Optional[java.time.Duration]] =
    asScala.timeLeft(elem).map {
      case Some(timeLeft) =>
        Optional.of(timeLeft.toJava)

      case None =>
        Optional.empty[java.time.Duration]()
    }

  def from(key: A): Set[A, F] =
    Set(asScala.from(key))

  def before(key: A): Set[A, F] =
    Set(asScala.before(key))

  def fromOrBefore(key: A): Set[A, F] =
    Set(asScala.fromOrBefore(key))

  def after(key: A): Set[A, F] =
    Set(asScala.after(key))

  def fromOrAfter(key: A): Set[A, F] =
    Set(asScala.fromOrAfter(key))

  def headOptional: IO[scala.Throwable, Optional[A]] =
    asScala.headOption.map(_.asJava)

  def drop(count: Int): Stream[A] =
    Stream(asScala.drop(count))

  def dropWhile(predicate: Predicate[A]): Stream[A] =
    Stream(asScala.dropWhile(predicate.test))

  def take(count: Int): Stream[A] =
    Stream(asScala.take(count))

  def takeWhile(predicate: Predicate[A]): Stream[A] =
    Stream(asScala.takeWhile(predicate.test))

  def map[B](function: JavaFunction[A, B]): Stream[B] =
    Stream(asScala.map(function.apply))

  def flatMap[B](function: JavaFunction[A, Stream[B]]): Stream[B] =
    Stream(
      asScala.flatMap {
        item =>
          function.apply(item).asScala
      }
    )

  def forEach(consumer: Consumer[A]): Stream[Unit] =
    Stream(asScala.foreach(consumer.accept))

  def filter(predicate: Predicate[A]): Stream[A] =
    Stream(asScala.filter(predicate.test))

  def filterNot(predicate: Predicate[A]): Stream[A] =
    Stream(asScala.filterNot(predicate.test))

  def foldLeft[B](initial: B, fold: JavaFunction[Pair[B, A], B]): IO[scala.Throwable, B] =
    asScala.foldLeft(initial) {
      case (b, a) =>
        fold.apply(Pair(b, a))
    }

  def size: IO[scala.Throwable, Int] =
    stream.size

  def stream: Stream[A] =
    Stream(asScala.stream)

  def sizeOfBloomFilterEntries: IO[scala.Throwable, Int] =
    asScala.sizeOfBloomFilterEntries

  def isEmpty: IO[scala.Throwable, Boolean] =
    asScala.isEmpty

  def nonEmpty: IO[scala.Throwable, Boolean] =
    asScala.nonEmpty

  def lastOptional: IO[scala.Throwable, Optional[A]] =
    asScala.lastOption.map(_.asJava)

  def reverse: Set[A, F] =
    Set(asScala.reverse)

  def asJava: util.Set[A] =
    asScala.asScala.asJava

  def close(): IO[scala.Throwable, Unit] =
    asScala.close()

  def delete(): IO[scala.Throwable, Unit] =
    asScala.delete()

  override def toString(): String =
    asScala.toString()
}
