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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb

import java.util
import java.util.Optional
import java.util.function.{BiFunction, Consumer, Predicate}

import swaydb.IO.ThrowableIO
import swaydb.data.util.Javaz.JavaFunction

import scala.compat.java8.FunctionConverters._
import scala.compat.java8.OptionConverters._
import scala.collection.JavaConverters._

object StreamJIO {
  def apply[A](stream: Stream[A, ThrowableIO]): StreamJIO[A] = new StreamJIO(stream)
}

class StreamJIO[A](protected[swaydb] val asScala: Stream[A, IO.ThrowableIO]) {
  def foreach(f: Consumer[A]): StreamJIO[Unit] =
    new StreamJIO[Unit](asScala.foreach(f.asScala))

  def map[B](f: JavaFunction[A, B]): StreamJIO[B] =
    StreamJIO(asScala.map(f.asScala))

  def flatMap[B](f: JavaFunction[A, StreamJIO[B]]): StreamJIO[B] =
    StreamJIO(asScala.flatMap(f.asScala(_).asScala))

  def drop(count: Int): StreamJIO[A] =
    StreamJIO(asScala.drop(count))

  def dropWhile(f: Predicate[A]): StreamJIO[A] =
    StreamJIO(asScala.dropWhile(f.asScala))

  def take(count: Int): StreamJIO[A] =
    StreamJIO(asScala.take(count))

  def takeWhile(f: Predicate[A]): StreamJIO[A] =
    StreamJIO(asScala.takeWhile(f.asScala))

  def filter(f: Predicate[A]): StreamJIO[A] =
    StreamJIO(asScala.filter(f.asScala))

  def filterNot(f: Predicate[A]): StreamJIO[A] =
    StreamJIO(asScala.filterNot(f.asScala))

  def lastOption: IO[Throwable, Optional[A]] =
    asScala.lastOption.map(_.asJava)

  def headOption: IO[Throwable, Optional[A]] =
    asScala.headOption.map(_.asJava)

  def foldLeft[B](initial: B, f: BiFunction[B, A, B]): IO[Throwable, B] =
    asScala.foldLeft(initial)(f.asScala)

  def size: IO[Throwable, Int] =
    asScala.size

  def materialize: IO[Throwable, util.List[A]] =
    asScala.materialize.map(_.asJava)
}
