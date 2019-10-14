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

import swaydb.IO.ThrowableIO
import swaydb.Prepare
import swaydb.data.accelerate.LevelZeroMeter
import swaydb.data.compaction.LevelMeter
import swaydb.java.data.util.Java._
import swaydb.java.data.util.Pair

import scala.collection.JavaConverters._
import scala.compat.java8.DurationConverters._
import scala.compat.java8.OptionConverters._

/**
 * Set database API.
 *
 * For documentation check - http://swaydb.io/
 */
case class SetIO[A, F](asScala: swaydb.Set[A, _, swaydb.IO.ThrowableIO]) {

  implicit val exceptionHandler = swaydb.IO.ExceptionHandler.Throwable

  private implicit def toIO[Throwable, R](io: swaydb.IO[scala.Throwable, R]): IO[scala.Throwable, R] = new IO[scala.Throwable, R](io)

  private def asScalaTypeCast: swaydb.Set[A, swaydb.PureFunction.OnKey[A, java.lang.Void], ThrowableIO] =
    asScala.asInstanceOf[swaydb.Set[A, swaydb.PureFunction.OnKey[A, java.lang.Void], swaydb.IO.ThrowableIO]]

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

  def add(elem: A, expireAfter: java.time.Duration): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.add(elem, expireAfter.toScala)

  def add(elems: java.util.List[A]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.add(elems.asScala)

  def add(elems: StreamIO[A]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.add(elems.asScala)

  def add(elems: java.util.Iterator[A]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.add(elems.asScala.toIterable)

  def remove(elem: A): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.remove(elem)

  def remove(from: A, to: A): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.remove(from, to)

  def remove(elems: java.util.List[A]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.remove(elems.asScala)

  def remove(elems: StreamIO[A]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.remove(elems.asScala)

  def remove(elems: java.util.Iterator[A]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.remove(elems.asScala.toIterable)

  def expire(elem: A, after: java.time.Duration): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.expire(elem, after.toScala)

  def expire(from: A, to: A, after: java.time.Duration): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.expire(from, to, after.toScala)

  def expire(elems: java.util.List[Pair[A, java.time.Duration]]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.expire {
      elems.asScala.map {
        pair =>
          (pair.left, pair.right.toScala.fromNow)
      }
    }

  def expire(elems: StreamIO[Pair[A, java.time.Duration]]): IO[scala.Throwable, swaydb.IO.Done] =
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

  def registerFunction[PF <: F with swaydb.java.PureFunction.OnKey[A, java.lang.Void]](function: PF): IO[scala.Throwable, swaydb.IO.Done] =
    asScalaTypeCast.registerFunction(PureFunction.asScala(function))

  def applyFunction[PF <: F with swaydb.java.PureFunction.OnKey[A, java.lang.Void]](from: A, to: A, function: PF): IO[scala.Throwable, swaydb.IO.Done] =
    asScalaTypeCast.applyFunction(from, to, PureFunction.asScala(function))

  def applyFunction[PF <: F with swaydb.java.PureFunction.OnKey[A, java.lang.Void]](elem: A, function: PF): IO[scala.Throwable, swaydb.IO.Done] =
    asScalaTypeCast.applyFunction(elem, PureFunction.asScala(function))

  def commit(prepare: java.util.List[Prepare[A, java.lang.Void]]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.commit(prepare.asInstanceOf[Seq[Prepare[A, Nothing]]])

  def commit(prepare: StreamIO[Prepare[A, java.lang.Void]]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.commit(prepare.asScala.asInstanceOf[swaydb.Stream[Prepare[A, Nothing], swaydb.IO.ThrowableIO]])

  def commit(prepare: java.util.Iterator[Prepare[A, java.lang.Void]]): IO[scala.Throwable, swaydb.IO.Done] =
    asScala.commit(prepare.asScala.asInstanceOf[Iterator[Prepare[A, Nothing]]].toIterable)

  def levelZeroMeter: LevelZeroMeter =
    asScala.levelZeroMeter

  def levelMeter(levelNumber: Integer): Optional[LevelMeter] =
    asScala.levelMeter(levelNumber).asJava

  def sizeOfSegments: Long =
    asScala.sizeOfSegments

  def elemSize(elem: A): Integer =
    asScala.elemSize(elem)

  def expiration(elem: A): IO[scala.Throwable, Optional[Deadline]] =
    asScala.expiration(elem).map(_.map(_.asJava).asJava)

  def timeLeft(elem: A): IO[scala.Throwable, Optional[java.time.Duration]] =
    asScala.timeLeft(elem).map {
      case Some(timeLeft) =>
        Optional.of(timeLeft.toJava)

      case None =>
        Optional.empty[java.time.Duration]()
    }

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
    asScala.headOption.map(_.asJava)

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

  def forEach(consumer: Consumer[A]): StreamIO[java.lang.Void] =
    Stream.fromScala(asScala.foreach(consumer.accept)).asInstanceOf[StreamIO[java.lang.Void]]

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
    stream.size.asInstanceOf[IO[scala.Throwable, Integer]]

  def stream: StreamIO[A] =
    Stream.fromScala(asScala.stream)

  def sizeOfBloomFilterEntries: IO[scala.Throwable, Integer] =
    asScala.sizeOfBloomFilterEntries.asInstanceOf[IO[scala.Throwable, Integer]]

  def isEmpty: IO[scala.Throwable, Boolean] =
    asScala.isEmpty

  def nonEmpty: IO[scala.Throwable, Boolean] =
    asScala.nonEmpty

  def lastOptional: IO[scala.Throwable, Optional[A]] =
    asScala.lastOption.map(_.asJava)

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
