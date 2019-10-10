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

package swaydb.java

import java.util.Optional
import java.util.function.{BiFunction, Consumer, Predicate}

import swaydb.java.data.util.JavaConversions.JavaFunction

import scala.collection.JavaConverters._
import scala.compat.java8.FunctionConverters._
import scala.compat.java8.OptionConverters._

object Stream {
  def apply[A](stream: swaydb.Stream[A, swaydb.IO.ThrowableIO]): Stream[A] =
    new Stream(stream)
}

class Stream[A](val asScala: swaydb.Stream[A, swaydb.IO.ThrowableIO]) {
  def forEach(consumer: Consumer[A]): Stream[Unit] =
    new Stream[Unit](asScala.foreach(consumer.asScala))

  def map[B](function: JavaFunction[A, B]): Stream[B] =
    Stream(asScala.map(function.asScala))

  def flatMap[B](function: JavaFunction[A, Stream[B]]): Stream[B] =
    Stream(asScala.flatMap(function.asScala(_).asScala))

  def drop(count: Int): Stream[A] =
    Stream(asScala.drop(count))

  def dropWhile(predicate: Predicate[A]): Stream[A] =
    Stream(asScala.dropWhile(predicate.asScala))

  def take(count: Int): Stream[A] =
    Stream(asScala.take(count))

  def takeWhile(predicate: Predicate[A]): Stream[A] =
    Stream(asScala.takeWhile(predicate.asScala))

  def filter(predicate: Predicate[A]): Stream[A] =
    Stream(asScala.filter(predicate.asScala))

  def filterNot(predicate: Predicate[A]): Stream[A] =
    Stream(asScala.filterNot(predicate.asScala))

  def lastOption: IO[Throwable, Optional[A]] =
    IO(asScala.lastOption.map(_.asJava))

  def headOption: IO[Throwable, Optional[A]] =
    IO(asScala.headOption.map(_.asJava))

  def foldLeft[B](initial: B, function: BiFunction[B, A, B]): IO[Throwable, B] =
    IO(asScala.foldLeft(initial)(function.asScala))

  def size: IO[Throwable, Int] =
    IO(asScala.size)

  def materialize: IO[Throwable, java.util.List[A]] =
    IO(asScala.materialize.map(_.asJava))
}
