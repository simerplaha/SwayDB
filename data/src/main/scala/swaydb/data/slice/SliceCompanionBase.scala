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

package swaydb.data.slice

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import swaydb.data.MaxKey
import swaydb.data.order.KeyOrder
import swaydb.data.util.{ByteSizeOf, Bytez}

import scala.reflect.ClassTag

/**
 * Base companion implementation for both Scala 2.12 and 2.13's [[Slice]] companion objects.
 */
trait SliceCompanionBase {

  val emptyBytes = Slice.create[Byte](0)

  val someEmptyBytes = Some(emptyBytes)

  private[swaydb] val emptyEmptyBytes: Slice[Slice[Byte]] = Slice.empty[Slice[Byte]]

  @inline final def empty[T: ClassTag] =
    Slice.create[T](0)

  def fill[T: ClassTag](length: Int)(elem: => T): Slice[T] =
    new Slice[T](
      array = Array.fill(length)(elem),
      fromOffset = 0,
      toOffset = if (length == 0) -1 else length - 1,
      written = length
    )

  def create[T: ClassTag](length: Int, isFull: Boolean = false): Slice[T] =
    new Slice(
      array = new Array[T](length),
      fromOffset = 0,
      toOffset = if (length == 0) -1 else length - 1,
      written = if (isFull) length else 0
    )

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

  def from[T: ClassTag](iterator: Iterator[T]): Slice[T] = {
    val slice = Slice.create[T](iterator.size)
    iterator foreach slice.add
    slice
  }

  def from[T: ClassTag](iterator: Iterable[T]): Slice[T] = {
    val slice = Slice.create[T](iterator.size)
    iterator foreach slice.add
    slice
  }

  def from(byteBuffer: ByteBuffer) =
    new Slice[Byte](
      array = byteBuffer.array(),
      fromOffset = byteBuffer.arrayOffset(),
      toOffset = byteBuffer.position() - 1,
      written = byteBuffer.position()
    )

  def from(byteBuffer: ByteBuffer, from: Int, to: Int) =
    new Slice[Byte](
      array = byteBuffer.array(),
      fromOffset = from,
      toOffset = to,
      written = to - from + 1
    )

  def apply[T: ClassTag](data: T*): Slice[T] =
    Slice(data.toArray)

  def writeInt(int: Int): Slice[Byte] =
    Slice.create[Byte](ByteSizeOf.int).addInt(int)

  def writeBoolean(boolean: Boolean): Slice[Byte] =
    Slice.create[Byte](1).addBoolean(boolean)

  def writeUnsignedInt(int: Int): Slice[Byte] =
    Slice.create[Byte](ByteSizeOf.varInt).addUnsignedInt(int).close()

  def writeLong(long: Long): Slice[Byte] =
    Slice.create[Byte](ByteSizeOf.long).addLong(long)

  def writeUnsignedLong(long: Long): Slice[Byte] =
    Slice.create[Byte](ByteSizeOf.varLong).addUnsignedLong(long).close()

  def writeString(string: String, charsets: Charset = StandardCharsets.UTF_8): Slice[Byte] =
    Slice(string.getBytes(charsets))

  def intersects[T](range1: (Slice[T], Slice[T]),
                    range2: (Slice[T], Slice[T]))(implicit ordering: Ordering[Slice[T]]): Boolean =
    intersects((range1._1, range1._2, true), (range2._1, range2._2, true))

  def within(key: Slice[Byte],
             minKey: Slice[Byte],
             maxKey: MaxKey[Slice[Byte]])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
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

  def minMax(left: Option[(Slice[Byte], Slice[Byte], Boolean)],
             right: Option[(Slice[Byte], Slice[Byte], Boolean)])(implicit keyOrder: Ordering[Slice[Byte]]): Option[(Slice[Byte], Slice[Byte], Boolean)] = {
    for {
      lft <- left
      rht <- right
    } yield minMax(lft, rht)
  } orElse left.orElse(right)

  def minMax(left: (Slice[Byte], Slice[Byte], Boolean),
             right: (Slice[Byte], Slice[Byte], Boolean))(implicit keyOrder: Ordering[Slice[Byte]]): (Slice[Byte], Slice[Byte], Boolean) = {
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
    def closeAll(): Slice[Slice[T]] = {
      val newSlices = Slice.create[Slice[T]](slices.close().size)
      slices foreach {
        slice =>
          newSlices.insert(slice.close())
      }
      newSlices
    }
  }

  implicit class OptionByteSliceImplicits(slice: Option[Slice[Byte]]) {

    @inline def unslice(): Option[Slice[Byte]] =
      slice flatMap {
        slice =>
          if (slice.isEmpty)
            None
          else
            Some(slice.unslice())
      }
  }

  implicit class SeqByteSliceImplicits(slice: Seq[Slice[Byte]]) {

    def unslice(): Seq[Slice[Byte]] =
      if (slice.isEmpty)
        slice
      else
        slice.map(_.unslice())
  }

  /**
   * http://www.swaydb.io/slice/byte-slice
   */
  implicit class ByteSliceImplicits(slice: Slice[Byte]) {

    @inline def addByte(value: Byte): Slice[Byte] = {
      slice insert value
      slice
    }

    @inline def addBytes(anotherSlice: Slice[Byte]): Slice[Byte] = {
      slice.addAll(anotherSlice)
      slice
    }

    @inline def addBoolean(boolean: Boolean): Slice[Byte] = {
      slice insert (if (boolean) 1.toByte else 0.toByte)
      slice
    }

    @inline def readBoolean(): Boolean =
      slice.get(0) == 1

    @inline def addInt(int: Int): Slice[Byte] = {
      Bytez.writeInt(int, slice)
      slice
    }

    @inline def readInt(): Int =
      Bytez.readInt(slice)

    @inline def dropUnsignedInt(): Slice[Byte] = {
      val (_, byteSize) = readUnsignedIntWithByteSize()
      slice drop byteSize
    }

    @inline def addSignedInt(int: Int): Slice[Byte] = {
      Bytez.writeSignedInt(int, slice)
      slice
    }

    @inline def readSignedInt(): Int =
      Bytez.readSignedInt(slice)

    @inline def addUnsignedInt(int: Int): Slice[Byte] = {
      Bytez.writeUnsignedInt(int, slice)
      slice
    }

    @inline def readUnsignedInt(): Int =
      Bytez.readUnsignedInt(slice)

    @inline def readUnsignedIntWithByteSize(): (Int, Int) =
      Bytez.readUnsignedIntWithByteSize(slice)

    @inline def addLong(long: Long): Slice[Byte] = {
      Bytez.writeLong(long, slice)
      slice
    }

    @inline def readLong(): Long =
      Bytez.readLong(slice)

    @inline def addUnsignedLong(long: Long): Slice[Byte] = {
      Bytez.writeUnsignedLong(long, slice)
      slice
    }

    @inline def readUnsignedLong(): Long =
      Bytez.readUnsignedLong(slice)

    @inline def readUnsignedLongWithByteSize(): (Long, Int) =
      Bytez.readUnsignedLongWithByteSize(slice)

    @inline def addSignedLong(long: Long): Slice[Byte] = {
      Bytez.writeSignedLong(long, slice)
      slice
    }

    @inline def readSignedLong(): Long =
      Bytez.readSignedLong(slice)

    @inline def addString(string: String, charsets: Charset = StandardCharsets.UTF_8): Slice[Byte] = {
      string.getBytes(charsets) foreach slice.add
      slice
    }

    @inline def readString(charset: Charset = StandardCharsets.UTF_8): String =
      Bytez.readString(slice, charset)

    @inline def toByteBufferWrap: ByteBuffer =
      slice.toByteBufferWrap

    @inline def toByteBufferDirect: ByteBuffer =
      slice.toByteBufferDirect

    @inline def toByteArrayOutputStream =
      slice.toByteArrayInputStream

    @inline def createReader() =
      SliceReader(slice)
  }

  implicit class SliceImplicit[T](slice: Slice[T]) {
    @inline def add(value: T): Slice[T] = {
      slice.insert(value)
      slice
    }

    @inline def addAll(values: Slice[T]): Slice[T] = {
      if (values.nonEmpty) slice.insertAll(values)
      slice
    }

    @inline def addAll(values: Array[T]): Slice[T] = {
      if (values.nonEmpty) slice.insertAll(values)
      slice
    }
  }

  implicit class SliceImplicitClassTag[T: ClassTag](slice: Slice[T]) {
    def append(other: Slice[T]): Slice[T] = {
      val merged = Slice.create[T](slice.size + other.size)
      merged addAll slice
      merged addAll other
      merged
    }

    def append(other: T): Slice[T] = {
      val merged = Slice.create[T](slice.size + 1)
      merged addAll slice
      merged add other
      merged
    }
  }

}
