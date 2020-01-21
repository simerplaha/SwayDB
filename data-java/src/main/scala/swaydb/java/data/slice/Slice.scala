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

package swaydb.java.data.slice

import java.nio.charset.{Charset, StandardCharsets}
import java.util.function.{BiFunction, Predicate}
import java.util.{Comparator, Optional}
import java.{lang, util}

import swaydb.data.slice.{Slice => ScalaSlice}
import swaydb.java.Pair
import swaydb.java.data.util.Java._

import scala.jdk.CollectionConverters._

object Slice {
  val emptyBytes: Slice[lang.Byte] = Slice.create[lang.Byte](0)

  def writeInt(integer: Int): Slice[java.lang.Byte] =
    Slice(ScalaSlice.writeInt(integer).asInstanceOf[ScalaSlice[java.lang.Byte]])

  def writeBoolean(bool: Boolean): Slice[java.lang.Byte] =
    Slice(ScalaSlice.writeBoolean(bool).asInstanceOf[ScalaSlice[java.lang.Byte]])

  def writeUnsignedInt(integer: Int): Slice[java.lang.Byte] =
    Slice(ScalaSlice.writeUnsignedInt(integer).asInstanceOf[ScalaSlice[java.lang.Byte]])

  def writeLong(value: Long): Slice[java.lang.Byte] =
    Slice(ScalaSlice.writeLong(value).asInstanceOf[ScalaSlice[java.lang.Byte]])

  def writeUnsignedLong(value: Long): Slice[java.lang.Byte] =
    Slice(ScalaSlice.writeUnsignedLong(value).asInstanceOf[ScalaSlice[java.lang.Byte]])

  def writeString(string: String): Slice[java.lang.Byte] =
    Slice(ScalaSlice.writeString(string, StandardCharsets.UTF_8).asInstanceOf[ScalaSlice[java.lang.Byte]])

  def writeString(string: String, charsets: Charset): Slice[java.lang.Byte] =
    Slice(ScalaSlice.writeString(string, charsets).asInstanceOf[ScalaSlice[java.lang.Byte]])

  def fromByteArray(array: Array[java.lang.Byte]): Slice[java.lang.Byte] =
    Slice(swaydb.data.slice.Slice(array))

  def create[T](length: Int): Slice[T] =
    new Slice(ScalaSlice.create(length = length))

  def createFull[T](length: Int): Slice[T] =
    new Slice(ScalaSlice.create(length = length, isFull = true))
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

  @throws[ArrayIndexOutOfBoundsException]
  def add(value: T): Slice[T] = {
    asScala.add(value)
    this
  }

  @throws[ArrayIndexOutOfBoundsException]
  def addAll(value: Array[T]): Slice[T] = {
    asScala.addAll(value)
    this
  }

  @throws[ArrayIndexOutOfBoundsException]
  def addAll(value: Slice[T]): Slice[T] = {
    asScala.addAll(value.asScala)
    this
  }

  def toArray: Array[T] =
    asScala.toArray[T](asScala.classTag)

  def toArrayCopy: Array[T] =
    asScala.toArrayCopy(asScala.classTag)

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

  def foldLeft[B](initial: B, function: BiFunction[B, T, B]): B =
    asScala.foldLeft(initial) {
      case (b, t) =>
        function.apply(b, t)
    }

  def foldRight[B](initial: B, function: BiFunction[T, B, B]): B =
    asScala.foldRight(initial) {
      case (t, b) =>
        function.apply(t, b)
    }

  def map[B](function: JavaFunction[T, B]): Slice[B] = {
    val mapped = Slice.create[B](size)
    asScala.foreach {
      item =>
        mapped add function.apply(item)
    }
    mapped
  }

  def underlyingArraySize: Int =
    asScala.underlyingArraySize

  def sorted(comparator: Comparator[T]): Slice[T] =
    Slice(asScala.sorted(comparator.asScala))

  override def equals(obj: Any): Boolean =
    obj match {
      case slice: Slice[_] =>
        asScala.equals(slice.asScala)

      case _ => false
    }

  override def hashCode(): Int =
    asScala.hashCode()
}
