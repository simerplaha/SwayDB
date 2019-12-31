/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.java

import java.util
import java.util.Optional
import java.util.function.{Consumer, Predicate}

import swaydb.Apply
import swaydb.IO.ThrowableIO
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.java.ScalaMapToJavaMapOutputConverter._
import swaydb.java.data.util.Java._

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ListBuffer
import scala.compat.java8.DurationConverters._

/**
 * Set database API.
 *
 * For documentation check - http://swaydb.io/
 */
case class SetIO[A, F <: swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]](private val _asScala: swaydb.Set[A, _, swaydb.IO.ThrowableIO]) {

  implicit val exceptionHandler = swaydb.IO.ExceptionHandler.Throwable

  val asScala: swaydb.Set[A, swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]], ThrowableIO] =
    _asScala.asInstanceOf[swaydb.Set[A, swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]], swaydb.IO.ThrowableIO]]

  def get(elem: A): IO[scala.Throwable, Optional[A]] =
    asScala.get(elem).transform(_.asJava)

  def contains(elem: A): IO[scala.Throwable, java.lang.Boolean] =
    asScala.contains(elem)

  def mightContain(elem: A): IO[scala.Throwable, java.lang.Boolean] =
    asScala.mightContain(elem)

  def mightContainFunction(function: A): IO[scala.Throwable, java.lang.Boolean] =
    (asScala mightContainFunction function)

  def add(elem: A): IO[scala.Throwable, swaydb.Done] =
    asScala add elem

  def add(elem: A, expireAfter: java.time.Duration): IO[scala.Throwable, swaydb.Done] =
    asScala.add(elem, expireAfter.toScala)

  def add(elems: java.util.List[A]): IO[scala.Throwable, swaydb.Done] =
    asScala.add(elems.asScala)

  def add(elems: StreamIO[A]): IO[scala.Throwable, swaydb.Done] =
    asScala.add(elems.asScala)

  def add(elems: java.util.Iterator[A]): IO[scala.Throwable, swaydb.Done] =
    asScala.add(elems.asScala.toIterable)

  def remove(elem: A): IO[scala.Throwable, swaydb.Done] =
    asScala.remove(elem)

  def remove(from: A, to: A): IO[scala.Throwable, swaydb.Done] =
    asScala.remove(from, to)

  def remove(elems: java.util.List[A]): IO[scala.Throwable, swaydb.Done] =
    asScala.remove(elems.asScala)

  def remove(elems: StreamIO[A]): IO[scala.Throwable, swaydb.Done] =
    asScala.remove(elems.asScala)

  def remove(elems: java.util.Iterator[A]): IO[scala.Throwable, swaydb.Done] =
    asScala.remove(elems.asScala.toIterable)

  def expire(elem: A, after: java.time.Duration): IO[scala.Throwable, swaydb.Done] =
    asScala.expire(elem, after.toScala)

  def expire(from: A, to: A, after: java.time.Duration): IO[scala.Throwable, swaydb.Done] =
    asScala.expire(from, to, after.toScala)

  def expire(elems: java.util.List[Pair[A, java.time.Duration]]): IO[scala.Throwable, swaydb.Done] =
    asScala.expire {
      elems.asScala.map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def expire(elems: StreamIO[Pair[A, java.time.Duration]]): IO[scala.Throwable, swaydb.Done] =
    asScala.expire {
      elems.asScala.map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def expire(elems: java.util.Iterator[Pair[A, java.time.Duration]]): IO[scala.Throwable, swaydb.Done] =
    asScala.expire {
      elems.asScala map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      } toIterable
    }

  def clear(): IO[scala.Throwable, swaydb.Done] =
    asScala.clear()

  def registerFunction(function: F): IO[scala.Throwable, swaydb.Done] =
    asScala.registerFunction(PureFunction.asScala(function))

  def applyFunction(from: A, to: A, function: F): IO[scala.Throwable, swaydb.Done] =
    asScala.applyFunction(from, to, PureFunction.asScala(function))

  def applyFunction(elem: A, function: F): IO[scala.Throwable, swaydb.Done] =
    asScala.applyFunction(elem, PureFunction.asScala(function))

  def commit[P <: Prepare.Set[A, F]](prepare: java.util.List[P]): IO[scala.Throwable, swaydb.Done] =
    commit[P](prepare.iterator())

  def commit[P <: Prepare.Set[A, F]](prepare: StreamIO[P]): IO[scala.Throwable, swaydb.Done] =
    prepare
      .asScala
      .foldLeft(ListBuffer.empty[swaydb.Prepare[A, Nothing, swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]]])(_ += Prepare.toScala(_))
      .flatMap {
        statements =>
          asScala.commit(statements)
      }

  def commit[P <: Prepare.Set[A, F]](prepare: java.util.Iterator[P]): IO[scala.Throwable, swaydb.Done] = {
    val prepareStatements =
      prepare
        .asScala
        .foldLeft(ListBuffer.empty[swaydb.Prepare[A, Nothing, swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]]])(_ += Prepare.toScala(_))

    asScala commit prepareStatements
  }

  def levelZeroMeter: LevelZeroMeter =
    asScala.levelZeroMeter

  def levelMeter(levelNumber: Integer): Optional[LevelMeter] =
    asScala.levelMeter(levelNumber).asJava

  def sizeOfSegments: Long =
    asScala.sizeOfSegments

  def elemSize(elem: A): Integer =
    asScala.elemSize(elem)

  def expiration(elem: A): IO[scala.Throwable, Optional[Deadline]] =
    asScala.expiration(elem).transform(_.asJavaMap(_.asJava))

  def timeLeft(elem: A): IO[scala.Throwable, Optional[java.time.Duration]] =
    asScala.timeLeft(elem).transform(_.asJavaMap(_.toJava))

  def from(key: A): SetIO[A, F] =
    SetIO(asScala.from(key))

  def before(key: A): SetIO[A, F] =
    SetIO(asScala.before(key))

  def fromOrBefore(key: A): SetIO[A, F] =
    SetIO(asScala.fromOrBefore(key))

  def after(key: A): SetIO[A, F] =
    SetIO(asScala.after(key))

  def fromOrAfter(key: A): SetIO[A, F] =
    SetIO(asScala.fromOrAfter(key))

  def headOptional: IO[scala.Throwable, Optional[A]] =
    asScala.headOption.transform(_.asJava)

  def drop(count: Integer): StreamIO[A] =
    Stream.fromScala(asScala.drop(count))

  def dropWhile(predicate: Predicate[A]): StreamIO[A] =
    Stream.fromScala(asScala.dropWhile(predicate.test))

  def take(count: Integer): StreamIO[A] =
    Stream.fromScala(asScala.take(count))

  def takeWhile(predicate: Predicate[A]): StreamIO[A] =
    Stream.fromScala(asScala.takeWhile(predicate.test))

  def map[B](function: JavaFunction[A, B]): StreamIO[B] =
    Stream.fromScala(asScala.map(function.apply))

  def flatMap[B](function: JavaFunction[A, StreamIO[B]]): StreamIO[B] =
    Stream.fromScala(
      asScala.flatMap {
        item =>
          function.apply(item).asScala
      }
    )

  def forEach(consumer: Consumer[A]): StreamIO[Void] =
    Stream.fromScala(asScala.foreach(consumer.accept)).asInstanceOf[StreamIO[Void]]

  def filter(predicate: Predicate[A]): StreamIO[A] =
    Stream.fromScala(asScala.filter(predicate.test))

  def filterNot(predicate: Predicate[A]): StreamIO[A] =
    Stream.fromScala(asScala.filterNot(predicate.test))

  def foldLeft[B](initial: B, fold: JavaFunction[Pair[B, A], B]): IO[scala.Throwable, B] =
    asScala.foldLeft(initial) {
      case (b, a) =>
        fold.apply(Pair(b, a))
    }

  def size: IO[scala.Throwable, Integer] =
    stream.size

  def stream: StreamIO[A] =
    Stream.fromScala(asScala.stream)

  def sizeOfBloomFilterEntries: IO[scala.Throwable, Integer] =
    asScala.sizeOfBloomFilterEntries

  def isEmpty: IO[scala.Throwable, java.lang.Boolean] =
    asScala.isEmpty

  def nonEmpty: IO[scala.Throwable, java.lang.Boolean] =
    asScala.nonEmpty

  def lastOptional: IO[scala.Throwable, Optional[A]] =
    asScala.lastOption.transform(_.asJava)

  def reverse: SetIO[A, F] =
    SetIO(asScala.reverse)

  def asJava: util.Set[A] =
    asScala.asScala.asJava

  def close(): IO[scala.Throwable, Unit] =
    asScala.close()

  def delete(): IO[scala.Throwable, Unit] =
    asScala.delete()

  override def toString(): String =
    asScala.toString()
}
