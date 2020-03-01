/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.Bag
import swaydb.data.util.Java._

import scala.compat.java8.FunctionConverters._
import scala.jdk.CollectionConverters._
import scala.collection.compat._

object Stream {
  def fromScala[A](stream: swaydb.Stream[A]): Stream[A] =
    new Stream(stream)

  def create[A](iterator: java.util.Iterator[A]): Stream[A] =
    new Stream[A](swaydb.Stream(iterator.asScala.to(Iterable)))

  def create[A](iterator: java.util.List[A]): Stream[A] =
    new Stream[A](swaydb.Stream(iterator.asScala))

  def create[A](iterator: java.util.Collection[A]): Stream[A] =
    new Stream[A](swaydb.Stream(iterator.asScala))

  def range(from: Integer, to: Integer): Stream[Integer] =
    new Stream(swaydb.Stream.range(from, to).asInstanceOf[swaydb.Stream[Integer]])

  def rangeUntil(from: Integer, toExclusive: Integer): Stream[Integer] =
    new Stream(swaydb.Stream.range(from, toExclusive).asInstanceOf[swaydb.Stream[Integer]])

  def range(from: Character, to: Character): Stream[Character] =
    new Stream(swaydb.Stream.range(from, to).asInstanceOf[swaydb.Stream[Character]])

  def rangeUntil(from: Character, toExclusive: Character): Stream[Character] =
    new Stream(swaydb.Stream.range(from, toExclusive).asInstanceOf[swaydb.Stream[Character]])

  def tabulate[T](count: Int, function: JavaFunction[Int, T]): Stream[T] =
    new Stream(swaydb.Stream.tabulate[T](count)(function.apply))
}

class Stream[A](val asScala: swaydb.Stream[A]) {
  implicit val javaThrowableExceptionHandler = swaydb.java.IO.throwableExceptionHandler
  implicit val bag = Bag.less

  def forEach(consumer: Consumer[A]): Unit =
    asScala.foreach(consumer.asScala)(bag)

  def map[B](function: JavaFunction[A, B]): Stream[B] =
    Stream.fromScala(asScala.map(function.asScala))

  def flatMap[B](function: JavaFunction[A, Stream[B]]): Stream[B] =
    Stream.fromScala(asScala.flatMap(function.asScala(_).asScala))

  def drop(count: Int): Stream[A] =
    Stream.fromScala(asScala.drop(count))

  def dropWhile(predicate: Predicate[A]): Stream[A] =
    Stream.fromScala(asScala.dropWhile(predicate.test))

  def take(count: Int): Stream[A] =
    Stream.fromScala(asScala.take(count))

  def takeWhile(predicate: Predicate[A]): Stream[A] =
    Stream.fromScala(asScala.takeWhile(predicate.test))

  def filter(predicate: Predicate[A]): Stream[A] =
    Stream.fromScala(asScala.filter(predicate.test))

  def filterNot(predicate: Predicate[A]): Stream[A] =
    Stream.fromScala(asScala.filterNot(predicate.test))

  def lastOption: Optional[A] =
    asScala.lastOption.asJava

  def headOption: Optional[A] =
    asScala.headOption.asJava

  def foldLeft[B](initial: B, function: BiFunction[B, A, B]): B =
    asScala.foldLeft(initial)(function.asScala)(bag)

  def count(predicate: Predicate[A]): Int =
    asScala.count(predicate.test)(bag)

  def size: Int =
    asScala.size(bag)

  def materialize: java.util.List[A] =
    asScala.materialize.asJava
}
