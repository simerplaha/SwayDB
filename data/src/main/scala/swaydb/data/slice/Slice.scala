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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.slice

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}
import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.collection.{IterableLike, mutable}
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3
import swaydb.data.io.IO
import swaydb.data.order.KeyOrder
import swaydb.data.repairAppendix.MaxKey
import swaydb.data.util.{ByteSizeOf, ByteUtil}

/**
  * Documentation - http://swaydb.io/slice
  */
object Slice {

  def empty[T: ClassTag] =
    Slice.create[T](0)

  val emptyBytes = Slice.create[Byte](0)

  val someEmptyBytes = Some(emptyBytes)

  def fill[T: ClassTag](length: Int)(elem: => T): Slice[T] =
    new Slice(Array.fill(length)(elem), fromOffset = 0, toOffset = if (length == 0) -1 else length - 1, length)

  def create[T: ClassTag](length: Int): Slice[T] =
    new Slice(new Array[T](length), fromOffset = 0, toOffset = if (length == 0) -1 else length - 1, 0)

  def apply[T: ClassTag](data: Array[T]): Slice[T] =
    if (data.length == 0)
      Slice.create[T](0)
    else
      new Slice[T](data, 0, data.length - 1, data.length)

  def apply[T: ClassTag](data: T*): Slice[T] =
    Slice(data.toArray)

  def writeInt(int: Int): Slice[Byte] =
    Slice.create[Byte](ByteSizeOf.int).addInt(int)

  def writeBoolean(boolean: Boolean): Slice[Byte] =
    Slice.create[Byte](1).addBoolean(boolean)

  def writeIntUnsigned(int: Int): Slice[Byte] =
    Slice.create[Byte](ByteSizeOf.int + 1).addIntUnsigned(int).close()

  def writeLong(long: Long): Slice[Byte] =
    Slice.create[Byte](ByteSizeOf.long).addLong(long)

  def writeLongUnsigned(long: Long): Slice[Byte] =
    Slice.create[Byte](ByteSizeOf.long + 1).addLongUnsigned(long).close()

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
        case swaydb.data.repairAppendix.MaxKey.Fixed(maxKey) =>
          key <= maxKey
        case swaydb.data.repairAppendix.MaxKey.Range(_, maxKey) =>
          key < maxKey
      }
    }
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

    def unslice(): Option[Slice[Byte]] =
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

    def addByte(value: Byte): Slice[Byte] = {
      slice insert value
      slice
    }

    def addBytes(anotherSlice: Slice[Byte]): Slice[Byte] = {
      slice.addAll(anotherSlice)
      slice
    }

    def addBoolean(boolean: Boolean): Slice[Byte] = {
      slice insert (if (boolean) 1.toByte else 0.toByte)
      slice
    }

    def readBoolean(): Boolean =
      slice.get(0) == 1

    def addInt(int: Int): Slice[Byte] = {
      ByteUtil.writeInt(int, slice)
      slice
    }

    def readInt(): Int =
      ByteUtil.readInt(slice)

    def addIntSigned(int: Int): Slice[Byte] = {
      ByteUtil.writeSignedInt(int, slice)
      slice
    }

    def readIntSigned(): IO[Int] =
      ByteUtil.readSignedInt(slice)

    def addIntUnsigned(int: Int): Slice[Byte] = {
      ByteUtil.writeUnsignedInt(int, slice)
      slice
    }

    def readIntUnsigned(): IO[Int] =
      ByteUtil.readUnsignedInt(slice)

    def addLong(long: Long): Slice[Byte] = {
      ByteUtil.writeLong(long, slice)
      slice
    }

    def readLong(): Long =
      ByteUtil.readLong(slice)

    def addLongUnsigned(long: Long): Slice[Byte] = {
      ByteUtil.writeUnsignedLong(long, slice)
      slice
    }

    def readLongUnsigned(): IO[Long] =
      ByteUtil.readUnsignedLong(slice)

    def addLongSigned(long: Long): Slice[Byte] = {
      ByteUtil.writeSignedLong(long, slice)
      slice
    }

    def readLongSigned(): IO[Long] =
      ByteUtil.readSignedLong(slice)

    def addString(string: String, charsets: Charset = StandardCharsets.UTF_8): Slice[Byte] = {
      string.getBytes(charsets) foreach slice.add
      slice
    }

    def readString(charset: Charset = StandardCharsets.UTF_8): String =
      ByteUtil.readString(slice, charset)

    def toByteBuffer: ByteBuffer =
      slice.toByteBuffer

    def toByteArrayOutputStream =
      slice.toByteArrayInputStream

    def createReader() =
      new BytesReader(slice)
  }

  implicit class SliceImplicit[T](slice: Slice[T]) {
    def add(value: T): Slice[T] = {
      slice.insert(value)
      slice
    }

    def addAll(values: Iterable[T]): Slice[T] = {
      if (values.nonEmpty) slice.insertAll(values)
      slice
    }

    def addAllWithSizeIntUnsigned(values: Iterable[T]): Slice[T] = {
      if (values.nonEmpty) slice.insertAll(values)
      slice
    }

    def addAll(values: Array[T]): Slice[T] = {
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

  class SliceBuilder[T: ClassTag](sizeHint: Int) extends mutable.Builder[T, Slice[T]] {
    //max is used to in-case sizeHit == 0 which is possible for cases where (None ++ Some(Slice[T](...)))
    protected var slice: Slice[T] = Slice.create[T](((sizeHint max 100) * 2.5).toInt)

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
        case ex: Throwable =>
          throw ex
      }

    def clear() =
      slice = Slice.empty[T]

    def result: Slice[T] =
      slice.close()
  }

  implicit def canBuildFrom[T: ClassTag]: CanBuildFrom[Slice[_], T, Slice[T]] =
    new CanBuildFrom[Slice[_], T, Slice[T]] {
      def apply(from: Slice[_]) =
        new SliceBuilder[T](from.size max 100) //max is used in-case from.size == 0

      def apply() =
        new SliceBuilder[T](100)
    }

}

/**
  * An Iterable type that holds offset references to an Array without creating copies of the original array when creating
  * sub-slices.
  *
  * @param array      Array to create Slices for
  * @param fromOffset start offset
  * @param toOffset   end offset
  * @param _written   write position
  * @tparam T The type of this Slice
  */
class Slice[+T: ClassTag](array: Array[T],
                          val fromOffset: Int,
                          val toOffset: Int,
                          private var _written: Int) extends Iterable[T] with IterableLike[T, Slice[T]] {

  private var writePosition = fromOffset + _written

  override val size =
    toOffset - fromOffset + 1

  def written =
    _written

  override def isEmpty =
    written == 0

  def isFull =
    written == size

  override def nonEmpty =
    !isEmpty

  /**
    * Create a new Slice for the offsets.
    *
    * @param fromOffset start offset
    * @param toOffset   end offset
    * @return Slice for the given offsets
    */
  @throws[ArrayIndexOutOfBoundsException]
  override def slice(fromOffset: Int, toOffset: Int): Slice[T] =
    if (toOffset < 0) {
      Slice.create[T](0)
    } else {
      //overflow check
      val fromOffsetAdjusted = fromOffset + this.fromOffset
      val toOffsetAdjusted = fromOffsetAdjusted + (toOffset - fromOffset)
      if (fromOffsetAdjusted < this.fromOffset) throw new ArrayIndexOutOfBoundsException(fromOffset)
      if (toOffsetAdjusted > this.toOffset) throw new ArrayIndexOutOfBoundsException(toOffset)
      if (fromOffsetAdjusted > toOffsetAdjusted) throw new ArrayIndexOutOfBoundsException(fromOffset)
      val sliceWritePosition =
        if (writePosition <= fromOffsetAdjusted) //not written
          0
        else if (writePosition > toOffsetAdjusted) //fully written
          toOffsetAdjusted - fromOffsetAdjusted + 1
        else //some written
          toOffsetAdjusted - writePosition + 1
      new Slice[T](array, fromOffsetAdjusted, toOffsetAdjusted, sliceWritePosition)
    }

  override def splitAt(index: Int): (Slice[T], Slice[T]) =
    if (index == 0) {
      (Slice.create[T](0), slice(0, size - 1))
    } else {
      val split1 = slice(0, index - 1)
      val split2 = slice(index, size - 1)
      (split1, split2)
    }

  override def grouped(size: Int): Iterator[Slice[T]] =
    groupedSlice(size).iterator

  def groupedSlice(size: Int): Slice[Slice[T]] = {
    @tailrec
    def group(groups: Slice[Slice[T]],
              slice: Slice[T],
              size: Int): Slice[Slice[T]] =
      if (size <= 1) {
        groups add slice
        groups
      }
      else {
        val (slice1, slice2) = slice.splitAt(slice.size / size)
        groups add slice1
        group(groups, slice2, size - 1)
      }

    group(Slice.create[Slice[T]](size), this, size)
  }

  override def drop(count: Int): Slice[T] =
    if (count >= size)
      Slice.create[T](0)
    else
      slice(count, size - 1)

  override def dropRight(count: Int): Slice[T] =
    if (count >= size)
      Slice.create[T](0)
    else
      slice(0, size - count - 1)

  override def take(count: Int): Slice[T] =
    slice(0, (size min count) - 1)

  override def takeRight(count: Int): Slice[T] =
    slice(size - count, size - 1)

  override def head: T =
    headOption.get

  override def last: T =
    lastOption.get

  override def headOption: Option[T] =
    if (_written <= 0)
      None
    else
      Some(array(fromOffset))

  override def lastOption: Option[T] =
    if (_written <= 0)
      None
    else
      Some(array(fromOffset + _written - 1))

  def headSlice: Slice[T] = slice(0, 0)

  def lastSlice: Slice[T] = slice(size - 1, size - 1)

  @throws[ArrayIndexOutOfBoundsException]
  def get(index: Int): T = {
    val adjustedIndex = fromOffset + index
    if (adjustedIndex < fromOffset || adjustedIndex > toOffset) throw new ArrayIndexOutOfBoundsException(index)
    array(adjustedIndex)
  }

  /**
    * Returns a new slice which is not writable.
    */
  def close(): Slice[T] =
    if (size == written)
      this
    else if (writePosition - 1 >= 0)
      slice(0, writePosition - 1)
    else if (writePosition == 0)
      Slice.create[T](0)
    else
      this

  def apply(index: Int): T =
    get(index)

  @throws[ArrayIndexOutOfBoundsException]
  private[slice] def insert(item: Any): Unit = {
    if (writePosition < fromOffset || writePosition > toOffset) throw new ArrayIndexOutOfBoundsException(writePosition)
    array(writePosition) = item.asInstanceOf[T]
    _written += 1
    writePosition += 1
  }

  @throws[ArrayIndexOutOfBoundsException]
  private[slice] def insertAll(items: Iterable[Any]): Unit = {
    val futurePosition = writePosition + items.size - 1
    if (futurePosition < fromOffset || futurePosition > toOffset) throw new ArrayIndexOutOfBoundsException(futurePosition)
    items.asInstanceOf[Iterable[T]] foreach {
      item =>
        array(writePosition) = item
        _written += 1
        writePosition += 1
    }
  }

  private[slice] def toByteBuffer: ByteBuffer =
    ByteBuffer.wrap(array.asInstanceOf[Array[Byte]], fromOffset, size)

  private[slice] def toByteArrayInputStream: ByteArrayInputStream =
    new ByteArrayInputStream(array.asInstanceOf[Array[Byte]], fromOffset, size)

  /**
    * Returns the original Array if Slice is not a sub Slice
    * else returns a new copied Array from the offsets defined for this Slice.
    */
  override def toArray[B >: T](implicit evidence$1: ClassTag[B]): Array[B] =
    if (size == array.length)
      array.asInstanceOf[Array[B]]
    else
      toArrayCopy

  def toArrayCopy[B >: T](implicit evidence$1: ClassTag[B]): Array[B] = {
    val newArray = new Array[B](size)
    Array.copy(array, fromOffset, newArray, 0, size)
    newArray
  }

  def unslice(): Slice[T] =
    Slice(toArray)

  override def iterator = new Iterator[T] {
    private var position = fromOffset

    override def hasNext: Boolean =
      position <= toOffset && position <= writePosition - 1

    override def next(): T = {
      val next = array(position)
      position += 1
      next
    }
  }

  def reverse: Iterator[T] = new Iterator[T] {
    private var position = toOffset min (writePosition - 1)

    override def hasNext: Boolean =
      position >= fromOffset

    override def next(): T = {
      val next = array(position)
      position -= 1
      next
    }
  }

  override def filterNot(p: T => Boolean): Slice[T] = {
    val filtered = Slice.create[T](size)
    this.foreach {
      item =>
        if (!p(item)) filtered add item
    }
    filtered.close()
  }

  override def filter(p: T => Boolean): Slice[T] = {
    val filtered = Slice.create[T](size)
    this.foreach {
      item =>
        if (p(item))
          filtered add item
    }
    filtered.close()
  }

  def underlyingArraySize =
    array.length

  override protected[this] def newBuilder: scala.collection.mutable.Builder[T, Slice[T]] =
    new Slice.SliceBuilder[T](array.length max 100)

  override def equals(that: Any): Boolean =
    that match {
      case other: Slice[T] =>
        this.size == other.size &&
          this.iterator.sameElements(other.iterator)

      case _ =>
        false
    }

  override def hashCode(): Int =
    MurmurHash3.orderedHash(this)
}
