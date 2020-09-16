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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.data.slice

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import swaydb.Aggregator
import swaydb.data.order.KeyOrder
import swaydb.data.util.{ByteOps, ByteSizeOf}
import swaydb.data.{MaxKey, slice}

import scala.collection.{Iterable, Iterator, Seq}
import scala.reflect.ClassTag

/**
 * Base companion implementation for both Scala 2.12 and 2.13's [[Sliced]] companion objects.
 */
trait SliceCompanionBase {

  val emptyBytes = Slice.create[Byte](0)

  val emptyJavaBytes = Slice.create[java.lang.Byte](0)

  val someEmptyBytes = Some(emptyBytes)

  private[swaydb] val emptyEmptyBytes: Slice[Slice[Byte]] = Slice.empty[Slice[Byte]]

  @inline final def empty[T: ClassTag] =
    Slice.create[T](0)

  final def range(from: Int, to: Int): Slice[Int] = {
    val slice = Slice.create[Int](to - from + 1)
    (from to to) foreach slice.add
    slice
  }

  final def range(from: Char, to: Char): Slice[Char] = {
    val slice = Slice.create[Char](26)
    (from to to) foreach slice.add
    slice.close()
  }

  final def range(from: Byte, to: Byte): Slice[Byte] = {
    val slice = Slice.create[Byte](to - from + 1)
    (from to to) foreach {
      i =>
        slice add i.toByte
    }
    slice.close()
  }

  def fill[T: ClassTag](length: Int)(elem: => T): Slice[T] =
    new Slice[T](
      array = Array.fill(length)(elem),
      fromOffset = 0,
      toOffset = if (length == 0) -1 else length - 1,
      written = length
    )

  def createScalaBytes(length: Int): Slice[Byte] =
    Slice.create[Byte](length)

  def createJavaBytes(length: Int): Slice[java.lang.Byte] =
    Slice.create[java.lang.Byte](length)

  @inline final def create[T: ClassTag](length: Int, isFull: Boolean = false): Slice[T] =
    new Slice(
      array = new Array[T](length),
      fromOffset = 0,
      toOffset = if (length == 0) -1 else length - 1,
      written = if (isFull) length else 0
    )

  def createForJava(array: Array[java.lang.Byte]): Slice[java.lang.Byte] =
    apply(array)

  def createForScala(array: Array[Byte]): Slice[Byte] =
    apply(array)

  def apply[T: ClassTag](data: Array[T]): Slice[T] =
    if (data.length == 0)
      Slice.create[T](0)
    else
      new Slice[T](
        array = data,
        fromOffset = 0,
        toOffset = data.length - 1,
        written = data.length
      )

  def from[T: ClassTag](iterator: Iterator[T], size: Int): Slice[T] = {
    val slice = Slice.create[T](size)
    iterator foreach slice.add
    slice
  }

  def from[T: ClassTag](iterator: Iterable[T], size: Int): Slice[T] = {
    val slice = Slice.create[T](size)
    iterator foreach slice.add
    slice
  }

  def fromBufferForJava(byteBuffer: ByteBuffer): Slice[java.lang.Byte] =
    new Slice[java.lang.Byte](
      array = byteBuffer.array().asInstanceOf[Array[java.lang.Byte]],
      fromOffset = byteBuffer.arrayOffset(),
      toOffset = byteBuffer.position() - 1,
      written = byteBuffer.position()
    )

  def fromBufferForJava(byteBuffer: ByteBuffer, from: Int, to: Int): Slice[java.lang.Byte] =
    new Slice[java.lang.Byte](
      array = byteBuffer.array().asInstanceOf[Array[java.lang.Byte]],
      fromOffset = from,
      toOffset = to,
      written = to - from + 1
    )

  def fromBufferForScala(byteBuffer: ByteBuffer): Slice[Byte] =
    new Slice[Byte](
      array = byteBuffer.array(),
      fromOffset = byteBuffer.arrayOffset(),
      toOffset = byteBuffer.position() - 1,
      written = byteBuffer.position()
    )

  def fromBufferForScala(byteBuffer: ByteBuffer, from: Int, to: Int): Slice[Byte] =
    new Slice[Byte](
      array = byteBuffer.array(),
      fromOffset = from,
      toOffset = to,
      written = to - from + 1
    )

  @inline final def apply[T: ClassTag](data: T*): Slice[T] =
    Slice(data.toArray)

  @inline final def writeInt[B](integer: Int)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](ByteSizeOf.int)(byteOps.classTag).addInt(integer)

  @inline final def writeUnsignedInt[B](integer: Int)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](ByteSizeOf.varInt)(byteOps.classTag).addUnsignedInt(integer).close()

  @inline final def writeSignedInt[B](integer: Int)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](ByteSizeOf.varInt)(byteOps.classTag).addSignedInt(integer).close()

  @inline final def writeLong[B](num: Long)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](ByteSizeOf.long)(byteOps.classTag).addLong(num)

  @inline final def writeUnsignedLong[B](num: Long)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](ByteSizeOf.varLong)(byteOps.classTag).addUnsignedLong(num).close()

  @inline final def writeSignedLong[B](num: Long)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](ByteSizeOf.varLong)(byteOps.classTag).addSignedLong(num).close()

  @inline final def writeBoolean[B](bool: Boolean)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](1)(byteOps.classTag).addBoolean(bool)

  @inline final def writeString[B](string: String, charsets: Charset = StandardCharsets.UTF_8)(implicit byteOps: ByteOps[B]): Slice[B] =
    byteOps.writeString(string, charsets)

  @inline final def writeStringUTF8[B](string: String)(implicit byteOps: ByteOps[B]): Slice[B] =
    byteOps.writeString(string, StandardCharsets.UTF_8)

  @inline final def intersects[T](range1: (Slice[T], Slice[T]),
                                  range2: (Slice[T], Slice[T]))(implicit ordering: Ordering[Slice[T]]): Boolean =
    intersects((range1._1, range1._2, true), (range2._1, range2._2, true))

  def within[T](key: Slice[T],
                minKey: Slice[T],
                maxKey: MaxKey[Slice[T]])(implicit keyOrder: KeyOrder[Slice[T]]): Boolean = {
    import keyOrder._
    key >= minKey && {
      maxKey match {
        case swaydb.data.MaxKey.Fixed(maxKey) =>
          key <= maxKey
        case swaydb.data.MaxKey.Range(_, maxKey) =>
          key < maxKey
      }
    }
  }

  def minMax[T](left: Option[(Slice[T], Slice[T], Boolean)],
                right: Option[(Slice[T], Slice[T], Boolean)])(implicit keyOrder: Ordering[Slice[T]]): Option[(Slice[T], Slice[T], Boolean)] = {
    for {
      lft <- left
      rht <- right
    } yield minMax(lft, rht)
  } orElse left.orElse(right)

  def minMax[T](left: (Slice[T], Slice[T], Boolean),
                right: (Slice[T], Slice[T], Boolean))(implicit keyOrder: Ordering[Slice[T]]): (Slice[T], Slice[T], Boolean) = {
    val min = keyOrder.min(left._1, right._1)
    val maxCompare = keyOrder.compare(left._2, right._2)
    if (maxCompare == 0)
      (min, left._2, left._3 || right._3)
    else if (maxCompare < 0)
      (min, right._2, right._3)
    else
      (min, left._2, left._3)
  }

  /**
   * Boolean indicates if the toKey is inclusive.
   */
  def intersects[T](range1: (Slice[T], Slice[T], Boolean),
                    range2: (Slice[T], Slice[T], Boolean))(implicit ordering: Ordering[Slice[T]]): Boolean = {
    import ordering._

    def check(range1: (Slice[T], Slice[T], Boolean),
              range2: (Slice[T], Slice[T], Boolean)): Boolean =
      if (range1._3 && range2._3)
        range1._1 >= range2._1 && range1._1 <= range2._2 ||
          range1._2 >= range2._1 && range1._2 <= range2._2
      else if (!range1._3 && range2._3)
        range1._1 >= range2._1 && range1._1 <= range2._2 ||
          range1._2 > range2._1 && range1._2 < range2._2
      else if (range1._3 && !range2._3)
        range1._1 >= range2._1 && range1._1 < range2._2 ||
          range1._2 >= range2._1 && range1._2 < range2._2
      else //both are false
        range1._1 >= range2._1 && range1._1 < range2._2 ||
          range1._2 > range2._1 && range1._2 < range2._2

    check(range1, range2) || check(range2, range1)
  }

  implicit class SlicesImplicits[T: ClassTag](slices: Slice[Slice[T]]) {
    /**
     * Closes this Slice and children Slices which disables
     * more data to be written to any of the Slices.
     */
    @inline final def closeAll(): Slice[Slice[T]] = {
      val newSlices = Slice.create[Slice[T]](slices.close().size)
      slices foreach {
        slice =>
          newSlices.add(slice.close())
      }
      newSlices
    }
  }

  implicit class OptionByteSliceImplicits(slice: Option[Slice[Byte]]) {

    @inline final def unslice(): Option[Slice[Byte]] =
      slice flatMap {
        slice =>
          if (slice.isEmpty)
            None
          else
            Some(slice.unslice())
      }
  }

  implicit class SeqByteSliceImplicits(slice: Seq[Slice[Byte]]) {

    @inline final def unslice(): Seq[Slice[Byte]] =
      if (slice.isEmpty)
        slice
      else
        slice.map(_.unslice())
  }

  implicit class JavaByteSliced(sliced: Slice[java.lang.Byte]) {
    def cast: Slice[Byte] =
      sliced.asInstanceOf[Slice[Byte]]
  }

  implicit class ScalaByteSliced(sliced: Slice[Byte]) {
    def cast: Slice[java.lang.Byte] =
      sliced.asInstanceOf[Slice[java.lang.Byte]]
  }

  @inline final def newBuilder[T: ClassTag](sizeHint: Int): Slice.SliceBuilder[T] =
    new slice.Slice.SliceBuilder[T](sizeHint)

  private[swaydb] def newAggregator[T: ClassTag](sizeHint: Int): Aggregator[T, Slice[T]] =
    Aggregator.fromBuilder[T, Slice[T]](newBuilder[T](sizeHint))
}
