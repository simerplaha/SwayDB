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

class StreamJIO[A](protected[swaydb] val stream: Stream[A, IO.ThrowableIO]) {
  def foreach(f: Consumer[A]): StreamJIO[Unit] =
    new StreamJIO[Unit](stream.foreach(f.asScala))

  def map[B](f: JavaFunction[A, B]): StreamJIO[B] =
    StreamJIO(stream.map(f.asScala))

  def flatMap[B](f: JavaFunction[A, StreamJIO[B]]): StreamJIO[B] =
    StreamJIO(stream.flatMap(f.asScala(_).stream))

  def drop(count: Int): StreamJIO[A] =
    StreamJIO(stream.drop(count))

  def dropWhile(f: Predicate[A]): StreamJIO[A] =
    StreamJIO(stream.dropWhile(f.asScala))

  def take(count: Int): StreamJIO[A] =
    StreamJIO(stream.take(count))

  def takeWhile(f: Predicate[A]): StreamJIO[A] =
    StreamJIO(stream.takeWhile(f.asScala))

  def filter(f: Predicate[A]): StreamJIO[A] =
    StreamJIO(stream.filter(f.asScala))

  def filterNot(f: Predicate[A]): StreamJIO[A] =
    StreamJIO(stream.filterNot(f.asScala))

  def lastOption: IO[Throwable, Optional[A]] =
    stream.lastOption.map(_.asJava)

  def headOption: IO[Throwable, Optional[A]] =
    stream.headOption.map(_.asJava)

  def foldLeft[B](initial: B, f: BiFunction[B, A, B]): IO[Throwable, B] =
    stream.foldLeft(initial)(f.asScala)

  def size: IO[Throwable, Int] =
    stream.size

  def materialize: IO[Throwable, util.List[A]] =
    stream.materialize.map(_.asJava)
}
