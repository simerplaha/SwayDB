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

import java.util.{Comparator, Optional}
import java.util.function.Predicate
import java.{lang, util}

import swaydb.data.slice.{Slice => ScalaSlice}
import swaydb.java.data.util.Pair
import swaydb.java.data.util.Javaz._

import scala.collection.JavaConverters._
import scala.compat.java8.OptionConverters._

object Slice {
  val emptyBytes: ScalaSlice[lang.Byte] = ScalaSlice.create[java.lang.Byte](0)
}

case class Slice[T](asScala: ScalaSlice[T]) extends java.lang.Iterable[T] {
  def size: Int =
    asScala.size

  def isEmpty =
    asScala.isEmpty

  def isFull =
    asScala.isFull

  def nonEmpty =
    asScala.nonEmpty

  def slice(fromOffset: Integer, toOffset: Integer): Slice[T] =
    Slice(asScala.slice(fromOffset, toOffset))

  def splitAt(index: Int): Pair[Slice[T], Slice[T]] = {
    val (slice1, slice2) = asScala.splitAt(index)
    Pair(Slice(slice1), Slice(slice2))
  }

  def grouped(size: Int): util.Iterator[ScalaSlice[T]] =
    asScala.grouped(size).asJava

  def groupedSlice(size: Int): Slice[Slice[T]] =
    Slice(asScala.groupedSlice(size).map(Slice(_)))

  def drop(count: Int): Slice[T] =
    Slice(asScala.drop(count))

  def dropTo(elem: T): Optional[Slice[T]] =
    asScala.dropTo(elem) match {
      case Some(slice) =>
        Optional.of(Slice(slice))
      case None =>
        Optional.empty()
    }

  def dropUntil(elem: T): Optional[Slice[T]] =
    asScala.dropUntil(elem) match {
      case Some(slice) =>
        Optional.of(Slice(slice))
      case None =>
        Optional.empty()
    }

  def dropRight(count: Int): Slice[T] =
    Slice(asScala.dropRight(count))

  def take(count: Int): Slice[T] =
    Slice(asScala.take(count))

  def take(fromIndex: Int, count: Int): Slice[T] =
    Slice(asScala.take(fromIndex, count))

  def takeRight(count: Int): Slice[T] =
    Slice(asScala.takeRight(count))

  def head: T =
    asScala.head

  def last: T =
    asScala.head

  def headOptional: Optional[T] =
    asScala.headOption.asJava

  def lastOptional: Optional[T] =
    asScala.lastOption.asJava

  def headSlice: Slice[T] =
    Slice(asScala.headSlice)

  def lastSlice: Slice[T] =
    Slice(asScala.lastSlice)

  def get(index: Int): T =
    asScala.get(index)

  def indexOf(elem: T): Optional[Int] =
    asScala.indexOf(elem).asJava

  def close(): Slice[T] =
    Slice(asScala.close())

  //todo - how to convert Scala array to Java without iteration?

  @throws[NotImplementedError]
  def toArray = ???

  @throws[NotImplementedError]
  def toArrayCopy = ???

  def isOriginalSlice: Boolean =
    asScala.isOriginalSlice

  def isOriginalFullSlice: Boolean =
    asScala.isOriginalFullSlice

  def arrayLength: Int =
    asScala.arrayLength

  def unslice: Slice[T] =
    Slice(asScala.unslice())

  def toOptionalUnsliced(): Optional[Slice[T]] =
    asScala.toOptionUnsliced() match {
      case Some(unsliced) =>
        Optional.of(Slice(unsliced))

      case None =>
        Optional.empty()
    }

  override def iterator(): util.Iterator[T] =
    asScala.iterator.asJava

  def reverse(): util.Iterator[T] =
    asScala.reverse.asJava

  def filterNot(predicate: Predicate[T]): Slice[T] =
    Slice(asScala.filterNot(predicate.test))

  def filter(predicate: Predicate[T]): Slice[T] =
    Slice(asScala.filter(predicate.test))

  def underlyingArraySize: Int =
    asScala.underlyingArraySize

  def sorted(comparator: Comparator[T]): Slice[T] =
    Slice(asScala.sorted(comparator.asScala))

  override def equals(obj: Any): Boolean =
    asScala.equals(obj)

  override def hashCode(): Int =
    asScala.hashCode()
}
