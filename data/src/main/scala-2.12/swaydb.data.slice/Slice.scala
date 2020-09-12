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

package swaydb.data.slice

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import swaydb.Aggregator
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice.Sliced
import swaydb.data.{MaxKey, slice}
import swaydb.data.util.{ByteSizeOf, Bytez, SomeOrNoneCovariant}

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.collection.{Iterable, IterableLike, Iterator, Seq, mutable}
import scala.reflect.ClassTag

/**
 * Documentation - http://swaydb.io/slice
 */
sealed trait SliceOption[+T] extends SomeOrNoneCovariant[SliceOption[T], Sliced[T]] {
  override def noneC: SliceOption[Nothing] = Slice.Null

  def isUnslicedOption: Boolean

  def asSliceOption(): SliceOption[T]

  def unsliceOption(): SliceOption[T] =
    if (this.isNoneC || this.getC.isEmpty)
      Slice.Null
    else
      this.getC.unslice()
}

object Slice {

  final case object Null extends SliceOption[Nothing] {
    override val isNoneC: Boolean = true
    override def getC: Sliced[Nothing] = throw new Exception("Slice is of type Null")
    override def isUnslicedOption: Boolean = true
    override def asSliceOption(): SliceOption[Nothing] = this
  }

  class SliceBuilder[T: ClassTag](sizeHint: Int) extends mutable.Builder[T, Sliced[T]] {
    //max is used to in-case sizeHit == 0 which is possible for cases where (None ++ Some(Sliced[T](...)))
    protected var slice: Sliced[T] = Slice.create[T]((sizeHint * 2) max 100)

    def extendSlice(by: Int) = {
      val extendedSlice = Slice.create[T](slice.size * by)
      extendedSlice addAll slice
      slice = extendedSlice
    }

    @tailrec
    final def +=(x: T): this.type =
      try {
        slice add x
        this
      } catch {
        case _: ArrayIndexOutOfBoundsException => //Extend slice.
          extendSlice(by = 2)
          +=(x)
      }

    def clear() =
      slice = Slice.create[T](slice.size)

    def result: Sliced[T] =
      slice.close()
  }

  implicit def canBuildFrom[T: ClassTag]: CanBuildFrom[Sliced[_], T, Sliced[T]] =
    new CanBuildFrom[Sliced[_], T, Sliced[T]] {
      def apply(from: Sliced[_]) =
        new SliceBuilder[T](from.size)

      def apply() =
        new SliceBuilder[T](100)
    }

  val emptyBytes = Slice.create[Byte](0)

  val someEmptyBytes = Some(emptyBytes)

  private[swaydb] val emptyEmptyBytes: Sliced[Sliced[Byte]] = Slice.empty[Sliced[Byte]]

  @inline final def empty[T: ClassTag] =
    Slice.create[T](0)

  final def range(from: Int, to: Int): Sliced[Int] = {
    val slice = Slice.create[Int](to - from + 1)
    (from to to) foreach slice.add
    slice
  }

  final def range(from: Char, to: Char): Sliced[Char] = {
    val slice = Slice.create[Char](26)
    (from to to) foreach slice.add
    slice.close()
  }

  final def range(from: Byte, to: Byte): Sliced[Byte] = {
    val slice = Slice.create[Byte](to - from + 1)
    (from to to) foreach {
      i =>
        slice add i.toByte
    }
    slice.close()
  }

  def fill[T: ClassTag](length: Int)(elem: => T): Sliced[T] =
    new Sliced[T](
      array = Array.fill(length)(elem),
      fromOffset = 0,
      toOffset = if (length == 0) -1 else length - 1,
      written = length
    )

  @inline final def create[T: ClassTag](length: Int, isFull: Boolean = false): Sliced[T] =
    new Sliced(
      array = new Array[T](length),
      fromOffset = 0,
      toOffset = if (length == 0) -1 else length - 1,
      written = if (isFull) length else 0
    )

  def apply[T: ClassTag](data: Array[T]): Sliced[T] =
    if (data.length == 0)
      Slice.create[T](0)
    else
      new Sliced[T](
        array = data,
        fromOffset = 0,
        toOffset = data.length - 1,
        written = data.length
      )

  def from[T: ClassTag](iterator: Iterator[T], size: Int): Sliced[T] = {
    val slice = Slice.create[T](size)
    iterator foreach slice.add
    slice
  }

  def from[T: ClassTag](iterator: Iterable[T], size: Int): Sliced[T] = {
    val slice = Slice.create[T](size)
    iterator foreach slice.add
    slice
  }

  def from(byteBuffer: ByteBuffer) =
    new Sliced[Byte](
      array = byteBuffer.array(),
      fromOffset = byteBuffer.arrayOffset(),
      toOffset = byteBuffer.position() - 1,
      written = byteBuffer.position()
    )

  def from(byteBuffer: ByteBuffer, from: Int, to: Int) =
    new Sliced[Byte](
      array = byteBuffer.array(),
      fromOffset = from,
      toOffset = to,
      written = to - from + 1
    )

  @inline final def apply[T: ClassTag](data: T*): Sliced[T] =
    Slice(data.toArray)

  @inline final def writeInt(int: Int): Sliced[Byte] =
    Slice.create[Byte](ByteSizeOf.int).addInt(int)

  @inline final def writeBoolean(boolean: Boolean): Sliced[Byte] =
    Slice.create[Byte](1).addBoolean(boolean)

  @inline final def writeUnsignedInt(int: Int): Sliced[Byte] =
    Slice.create[Byte](ByteSizeOf.varInt).addUnsignedInt(int).close()

  @inline final def writeLong(long: Long): Sliced[Byte] =
    Slice.create[Byte](ByteSizeOf.long).addLong(long)

  @inline final def writeUnsignedLong(long: Long): Sliced[Byte] =
    Slice.create[Byte](ByteSizeOf.varLong).addUnsignedLong(long).close()

  @inline final def writeString(string: String, charsets: Charset = StandardCharsets.UTF_8): Sliced[Byte] =
    Slice(string.getBytes(charsets))

  @inline final def intersects[T](range1: (Sliced[T], Sliced[T]),
                                  range2: (Sliced[T], Sliced[T]))(implicit ordering: Ordering[Sliced[T]]): Boolean =
    intersects((range1._1, range1._2, true), (range2._1, range2._2, true))

  def within(key: Sliced[Byte],
             minKey: Sliced[Byte],
             maxKey: MaxKey[Sliced[Byte]])(implicit keyOrder: KeyOrder[Sliced[Byte]]): Boolean = {
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

  def minMax(left: Option[(Sliced[Byte], Sliced[Byte], Boolean)],
             right: Option[(Sliced[Byte], Sliced[Byte], Boolean)])(implicit keyOrder: Ordering[Sliced[Byte]]): Option[(Sliced[Byte], Sliced[Byte], Boolean)] = {
    for {
      lft <- left
      rht <- right
    } yield minMax(lft, rht)
  } orElse left.orElse(right)

  def minMax(left: (Sliced[Byte], Sliced[Byte], Boolean),
             right: (Sliced[Byte], Sliced[Byte], Boolean))(implicit keyOrder: Ordering[Sliced[Byte]]): (Sliced[Byte], Sliced[Byte], Boolean) = {
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
  def intersects[T](range1: (Sliced[T], Sliced[T], Boolean),
                    range2: (Sliced[T], Sliced[T], Boolean))(implicit ordering: Ordering[Sliced[T]]): Boolean = {
    import ordering._

    def check(range1: (Sliced[T], Sliced[T], Boolean),
              range2: (Sliced[T], Sliced[T], Boolean)): Boolean =
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

  implicit class SlicesImplicits[T: ClassTag](slices: Sliced[Sliced[T]]) {
    /**
     * Closes this Slice and children Slices which disables
     * more data to be written to any of the Slices.
     */
    @inline final def closeAll(): Sliced[Sliced[T]] = {
      val newSlices = Slice.create[Sliced[T]](slices.close().size)
      slices foreach {
        slice =>
          newSlices.insert(slice.close())
      }
      newSlices
    }
  }

  implicit class OptionByteSliceImplicits(slice: Option[Sliced[Byte]]) {

    @inline final def unslice(): Option[Sliced[Byte]] =
      slice flatMap {
        slice =>
          if (slice.isEmpty)
            None
          else
            Some(slice.unslice())
      }
  }

  implicit class SeqByteSliceImplicits(slice: Seq[Sliced[Byte]]) {

    @inline final def unslice(): Seq[Sliced[Byte]] =
      if (slice.isEmpty)
        slice
      else
        slice.map(_.unslice())
  }

  /**
   * http://www.swaydb.io/slice/byte-slice
   */
  implicit class ByteSliceImplicits(slice: Sliced[Byte]) {

    @inline final def addByte(value: Byte): Sliced[Byte] = {
      slice insert value
      slice
    }

    @inline final def addBytes(anotherSlice: Sliced[Byte]): Sliced[Byte] = {
      slice.addAll(anotherSlice)
      slice
    }

    @inline final def addBoolean(boolean: Boolean): Sliced[Byte] = {
      slice insert (if (boolean) 1.toByte else 0.toByte)
      slice
    }

    @inline final def readBoolean(): Boolean =
      slice.get(0) == 1

    @inline final def addInt(int: Int): Sliced[Byte] = {
      Bytez.writeInt(int, slice)
      slice
    }

    @inline final def readInt(): Int =
      Bytez.readInt(slice)

    @inline final def dropUnsignedInt(): Sliced[Byte] = {
      val (_, byteSize) = readUnsignedIntWithByteSize()
      slice drop byteSize
    }

    @inline final def addSignedInt(int: Int): Sliced[Byte] = {
      Bytez.writeSignedInt(int, slice)
      slice
    }

    @inline final def readSignedInt(): Int =
      Bytez.readSignedInt(slice)

    @inline final def addUnsignedInt(int: Int): Sliced[Byte] = {
      Bytez.writeUnsignedInt(int, slice)
      slice
    }

    @inline final def addNonZeroUnsignedInt(int: Int): Sliced[Byte] = {
      Bytez.writeUnsignedIntNonZero(int, slice)
      slice
    }

    @inline final def readUnsignedInt(): Int =
      Bytez.readUnsignedInt(slice)

    @inline final def readUnsignedIntWithByteSize(): (Int, Int) =
      Bytez.readUnsignedIntWithByteSize(slice)

    @inline final def readNonZeroUnsignedIntWithByteSize(): (Int, Int) =
      Bytez.readUnsignedIntNonZeroWithByteSize(slice)

    @inline final def addLong(long: Long): Sliced[Byte] = {
      Bytez.writeLong(long, slice)
      slice
    }

    @inline final def readLong(): Long =
      Bytez.readLong(slice)

    @inline final def addUnsignedLong(long: Long): Sliced[Byte] = {
      Bytez.writeUnsignedLong(long, slice)
      slice
    }

    @inline final def readUnsignedLong(): Long =
      Bytez.readUnsignedLong(slice)

    @inline final def readUnsignedLongWithByteSize(): (Long, Int) =
      Bytez.readUnsignedLongWithByteSize(slice)

    @inline final def readUnsignedLongByteSize(): Int =
      Bytez.readUnsignedLongByteSize(slice)

    @inline final def addSignedLong(long: Long): Sliced[Byte] = {
      Bytez.writeSignedLong(long, slice)
      slice
    }

    @inline final def readSignedLong(): Long =
      Bytez.readSignedLong(slice)

    @inline final def addString(string: String, charsets: Charset = StandardCharsets.UTF_8): Sliced[Byte] = {
      string.getBytes(charsets) foreach slice.add
      slice
    }

    @inline final def readString(charset: Charset = StandardCharsets.UTF_8): String =
      Bytez.readString(slice, charset)

    @inline final def toByteBufferWrap: ByteBuffer =
      slice.toByteBufferWrap

    @inline final def toByteBufferDirect: ByteBuffer =
      slice.toByteBufferDirect

    @inline final def toByteArrayOutputStream =
      slice.toByteArrayInputStream

    @inline final def createReader() =
      SliceReader(slice)
  }

  implicit class SliceImplicit[T](slice: Sliced[T]) {
    @inline final def add(value: T): Sliced[T] = {
      slice.insert(value)
      slice
    }

    @inline final def addAll(values: Sliced[T]): Sliced[T] = {
      if (values.nonEmpty) slice.insertAll(values)
      slice
    }

    @inline final def addAll(values: Array[T]): Sliced[T] = {
      if (values.nonEmpty) slice.insertAll(values)
      slice
    }
  }

  implicit class SliceImplicitClassTag[T: ClassTag](slice: Sliced[T]) {
    @inline final def append(other: Sliced[T]): Sliced[T] = {
      val merged = Slice.create[T](slice.size + other.size)
      merged addAll slice
      merged addAll other
      merged
    }

    @inline final def append(other: T): Sliced[T] = {
      val merged = Slice.create[T](slice.size + 1)
      merged addAll slice
      merged add other
      merged
    }
  }

  @inline final def newBuilder[T: ClassTag](sizeHint: Int): Slice.SliceBuilder[T] =
    new slice.Slice.SliceBuilder[T](sizeHint)

  private[swaydb] def newAggregator[T: ClassTag](sizeHint: Int): Aggregator[T, Sliced[T]] =
    Aggregator.fromBuilder[T, Sliced[T]](newBuilder[T](sizeHint))


  /**
   * An Iterable type that holds offset references to an Array without creating copies of the original array when creating
   * sub-slices.
   *
   * @param array      Array to create Slices for
   * @param fromOffset start offset
   * @param toOffset   end offset
   * @param written    items written
   * @tparam T The type of this Slice
   */
  //@formatter:off
  class Sliced[+T] private[slice](array: Array[T],
                                  fromOffset: Int,
                                  toOffset: Int,
                                  written: Int)(implicit classTag: ClassTag[T]) extends SliceBase[T](array, fromOffset, toOffset, written)
                                                                                   with SliceOption[T]
                                                                                   with IterableLike[T, Sliced[T]] {
                                                                                   //@formatter:on

    override val isNoneC: Boolean =
      false

    override def getC: Sliced[T] =
      this

    override def selfSlice: Sliced[T] =
      this

    override protected[this] def newBuilder: scala.collection.mutable.Builder[T, Sliced[T]] =
      new Slice.SliceBuilder[T](array.length max 1)
  }
}
