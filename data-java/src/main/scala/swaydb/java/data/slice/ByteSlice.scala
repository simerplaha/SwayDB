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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.java.data.slice

import java.nio.charset.{Charset, StandardCharsets}
import java.util.Optional
import java.util.function.{BiFunction, Predicate}
import java.{lang, util}

import swaydb.Pair
import swaydb.data.slice.Slice
import swaydb.data.slice.Slice._
import swaydb.data.util.Java._

import scala.jdk.CollectionConverters._

object ByteSlice {
  val emptyBytes: ByteSlice = ByteSlice.create(0)

  def writeInt(integer: Int): ByteSlice =
    ByteSlice(Slice.writeInt(integer).cast[java.lang.Byte])

  def writeBoolean(bool: Boolean): ByteSlice =
    ByteSlice(Slice.writeBoolean(bool).cast[java.lang.Byte])

  def writeUnsignedInt(integer: Int): ByteSlice =
    ByteSlice(Slice.writeUnsignedInt(integer).cast[java.lang.Byte])

  def writeLong(value: Long): ByteSlice =
    ByteSlice(Slice.writeLong(value).asInstanceOf[Sliced[java.lang.Byte]])

  def writeUnsignedLong(value: Long): ByteSlice =
    ByteSlice(Slice.writeUnsignedLong(value).asInstanceOf[Sliced[java.lang.Byte]])

  def writeString(string: String): ByteSlice =
    ByteSlice(Slice.writeString(string, StandardCharsets.UTF_8).asInstanceOf[Sliced[java.lang.Byte]])

  def writeString(string: String, charsets: Charset): ByteSlice =
    ByteSlice(Slice.writeString(string, charsets).asInstanceOf[Sliced[java.lang.Byte]])

  def fromByteArray(array: Array[java.lang.Byte]): ByteSlice =
    ByteSlice(swaydb.data.slice.Slice[java.lang.Byte](array))

  def create(length: Int): ByteSlice =
    new ByteSlice(Slice.create(length = length))

  def createFull(length: Int): ByteSlice =
    new ByteSlice(Slice.create(length = length, isFull = true))
}

case class ByteSlice(asScala: Sliced[java.lang.Byte]) extends java.lang.Iterable[java.lang.Byte] {
  def size: Int =
    asScala.size

  def fromOffset: Int =
    asScala.fromOffset

  def toOffset: Int =
    asScala.toOffset

  def isEmpty =
    asScala.isEmpty

  def isFull =
    asScala.isFull

  def nonEmpty =
    asScala.nonEmpty

  def slice(fromOffset: Integer, toOffset: Integer): ByteSlice =
    ByteSlice(asScala.slice(fromOffset, toOffset))

  def splitAt(index: Int): Pair[ByteSlice, ByteSlice] = {
    val (slice1, slice2) = asScala.splitAt(index)
    Pair(ByteSlice(slice1), ByteSlice(slice2))
  }

  def drop(count: Int): ByteSlice =
    ByteSlice(asScala.drop(count))

  def dropRight(count: Int): ByteSlice =
    ByteSlice(asScala.dropRight(count))

  def take(count: Int): ByteSlice =
    ByteSlice(asScala.take(count))

  def take(fromIndex: Int, count: Int): ByteSlice =
    ByteSlice(asScala.take(fromIndex, count))

  def takeRight(count: Int): ByteSlice =
    ByteSlice(asScala.takeRight(count))

  def head: lang.Byte =
    asScala.head

  def last =
    asScala.head

  def headOptional: Optional[java.lang.Byte] =
    asScala.headOption.asJava

  def lastOptional: Optional[java.lang.Byte] =
    asScala.lastOption.asJava

  def headSlice: ByteSlice =
    ByteSlice(asScala.headSlice)

  def lastSlice: ByteSlice =
    ByteSlice(asScala.lastSlice)

  def get(index: Int): java.lang.Byte =
    asScala.get(index)

  def indexOf(elem: java.lang.Byte): Optional[Int] =
    asScala.indexOf(elem).asJava

  def close(): ByteSlice =
    ByteSlice(asScala.close())

  def add(value: java.lang.Byte): ByteSlice = {
    asScala.cast[Byte].add(value)
    this
  }

  def addAll(value: Array[java.lang.Byte]): ByteSlice = {
    asScala.cast[Byte].addAll(value.asInstanceOf[Array[Byte]])
    this
  }

  def addAll(value: ByteSlice): ByteSlice = {
    asScala.cast[Byte].addAll(value.asScala.cast[Byte])
    this
  }

  def toArray: Array[java.lang.Byte] =
    asScala.toArray[java.lang.Byte](asScala.classTag)

  def toArrayCopy: Array[java.lang.Byte] =
    asScala.toArrayCopy(asScala.classTag)

  def isOriginalSlice: Boolean =
    asScala.isOriginalSlice

  def isOriginalFullSlice: Boolean =
    asScala.isOriginalFullSlice

  def arrayLength: Int =
    asScala.arrayLength

  def unslice: ByteSlice =
    ByteSlice(asScala.unslice())

  def toOptionalUnsliced(): Optional[ByteSlice] =
    asScala.toOptionUnsliced() match {
      case Some(unsliced) =>
        Optional.of(ByteSlice(unsliced))

      case None =>
        Optional.empty()
    }

  def toBuilder: ByteSliceBuilder =
    ByteSliceBuilder(this)

  def createReader(): SliceReader =
    SliceReader(asScala.cast[Byte].createReader())

  override def iterator(): util.Iterator[java.lang.Byte] =
    asScala.iterator.asJava

  def reverse(): util.Iterator[java.lang.Byte] =
    asScala.reverse.asJava

  def filterNot(predicate: Predicate[java.lang.Byte]): ByteSlice =
    ByteSlice(asScala.filterNot(predicate.test))

  def filter(predicate: Predicate[java.lang.Byte]): ByteSlice =
    ByteSlice(asScala.filter(predicate.test))

  def foldLeft[B](initial: B, function: BiFunction[B, java.lang.Byte, B]): B =
    asScala.foldLeft(initial) {
      case (b, t) =>
        function.apply(b, t)
    }

  def foldRight[B](initial: B, function: BiFunction[java.lang.Byte, B, B]): B =
    asScala.foldRight(initial) {
      case (t, b) =>
        function.apply(t, b)
    }

  def map(function: JavaFunction[java.lang.Byte, java.lang.Byte]): ByteSlice = {
    val mapped = ByteSlice.create(size)
    asScala.foreach {
      item =>
        mapped add function.apply(item)
    }
    mapped
  }

  def underlyingArraySize: Int =
    asScala.underlyingArraySize

  override def equals(obj: Any): Boolean =
    obj match {
      case slice: ByteSlice =>
        asScala.equals(slice.asScala)

      case _ => false
    }

  override def hashCode(): Int =
    asScala.hashCode()
}
