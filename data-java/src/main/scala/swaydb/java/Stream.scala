/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.java

import swaydb.utils.Java._
import swaydb.{Bag, Glass, Pair}

import java.util.Optional
import java.util.function.{BiFunction, Consumer, Predicate}
import java.{lang, util}
import scala.compat.java8.FunctionConverters._
import scala.jdk.CollectionConverters._

object Stream {

  private implicit val bag = Bag.glass

  @inline def fromScala[A](stream: swaydb.Stream[A, Glass]): Stream[A] =
    new Stream[A] {
      override def asScalaStream: swaydb.Stream[A, Glass] =
        stream
    }

  def of[A](iterator: java.util.Iterator[A]): Stream[A] =
    Stream.fromScala[A](swaydb.Stream(iterator.asScala))

  def of[A](iterable: java.lang.Iterable[A]): Stream[A] =
    Stream.fromScala[A](swaydb.Stream(iterable.asScala))

  def range(from: Int, to: Int): Stream[Integer] =
    Stream.fromScala(swaydb.Stream.range(from, to).asInstanceOf[swaydb.Stream[Integer, Glass]])

  def rangeUntil(from: Int, toExclusive: Int): Stream[Integer] =
    Stream.fromScala(swaydb.Stream.range(from, toExclusive).asInstanceOf[swaydb.Stream[Integer, Glass]])

  def range(from: Char, to: Char): Stream[Character] =
    Stream.fromScala(swaydb.Stream.range(from, to).asInstanceOf[swaydb.Stream[Character, Glass]])

  def rangeUntil(from: Char, toExclusive: Char): Stream[Character] =
    Stream.fromScala(swaydb.Stream.range(from, toExclusive).asInstanceOf[swaydb.Stream[Character, Glass]])

  def tabulate[T](count: Int, function: JavaFunction[Int, T]): Stream[T] =
    Stream.fromScala(swaydb.Stream.tabulate[T, Glass](count)(function.apply))
}

trait Stream[A] {

  def asScalaStream: swaydb.Stream[A, Glass]

  def forEach(consumer: Consumer[A]): Unit =
    asScalaStream.foreach(consumer.asScala)

  def map[B](function: JavaFunction[A, B]): Stream[B] =
    Stream.fromScala(asScalaStream.map(function.asScala))

  def flatMap[B](function: JavaFunction[A, Stream[B]]): Stream[B] =
    Stream.fromScala(asScalaStream.flatMap(function.asScala(_).asScalaStream))

  def drop(count: Int): Stream[A] =
    Stream.fromScala(asScalaStream.drop(count))

  def dropWhile(predicate: Predicate[A]): Stream[A] =
    Stream.fromScala(asScalaStream.dropWhile(predicate.test))

  def take(count: Int): Stream[A] =
    Stream.fromScala(asScalaStream.take(count))

  def takeWhile(predicate: Predicate[A]): Stream[A] =
    Stream.fromScala(asScalaStream.takeWhile(predicate.test))

  def filter(predicate: Predicate[A]): Stream[A] =
    Stream.fromScala(asScalaStream.filter(predicate.test))

  def filterNot(predicate: Predicate[A]): Stream[A] =
    Stream.fromScala(asScalaStream.filterNot(predicate.test))

  def partition[B](predicate: Predicate[A]): Pair[lang.Iterable[A], lang.Iterable[A]] =
    partitionList(predicate)

  def partitionList[B](predicate: Predicate[A]): Pair[util.List[A], util.List[A]] = {
    val (left, right) = asScalaStream.partitionBuffer(predicate.test)
    Pair(left.asJava, right.asJava)
  }

  def last: Optional[A] =
    asScalaStream.last.asJava

  def head: Optional[A] =
    asScalaStream.head.asJava

  def foldLeft[B](initial: B, function: BiFunction[B, A, B]): B =
    asScalaStream.foldLeft(initial)(function.asScala)

  def count(predicate: Predicate[A]): Int =
    asScalaStream.count(predicate.test)

  def iterator(): util.Iterator[A] =
    asScalaStream.iterator(Bag.glass).asJava

  def count: Int =
    asScalaStream.count

  def materialize: lang.Iterable[A] =
    asScalaStream.materialize.asJava

  def materializeList: util.List[A] =
    asScalaStream.materializeBuffer.asJava
}
