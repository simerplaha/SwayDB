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

import java.io.ByteArrayInputStream
import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}

import swaydb.IO
import swaydb.data.{MaxKey, slice}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice.SliceFactory
import swaydb.data.util.{ByteSizeOf, Bytez}

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.{ClassTagIterableFactory, EvidenceIterableFactory, EvidenceIterableFactoryDefaults, IterableFactory, IterableFactoryDefaults, IterableOnce, IterableOps, StrictOptimizedIterableOps, immutable, mutable}
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

/**
 * Documentation - http://swaydb.io/slice
 */
object Slice {

  val emptyBytes = Slice.create[Byte](0)

  val someEmptyBytes = Some(emptyBytes)

  private[swaydb] val emptyEmptyBytes: Slice[Slice[Byte]] = Slice.empty[Slice[Byte]]

  @inline final def empty[T: ClassTag] =
    Slice.create[T](0)

  def fill[T: ClassTag](length: Int)(elem: => T): Slice[T] =
    new Slice(
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

  class SliceFactory(sizeHint: Int) extends ClassTagIterableFactory[Slice] { self =>

    def from[A](source: IterableOnce[A])(implicit evidence: ClassTag[A]): Slice[A] =
      (newBuilder[A] ++= source).result()

    def empty[A](implicit evidence: ClassTag[A]): Slice[A] =
      Slice.create[A](sizeHint)

    def newBuilder[A](implicit evidence: ClassTag[A]): mutable.Builder[A, Slice[A]] =
      new mutable.Builder[A, Slice[A]] {
        //max is used to in-case sizeHit == 0 which is possible for cases where (None ++ Some(Slice[T](...)))
        protected var slice: Slice[A] = Slice.create[A]((self.sizeHint * 2) max 100)

        def extendSlice(by: Int) = {
          val extendedSlice = Slice.create[A](slice.size * by)
          extendedSlice addAll slice
          slice = extendedSlice
        }

        @tailrec
        override def addOne(x: A): this.type =
          try {
            slice add x
            this
          } catch {
            case _: ArrayIndexOutOfBoundsException => //Extend slice.
              extendSlice(by = 2)
              addOne(x)
          }

        def clear() =
          slice = Slice.empty[A]

        def result: Slice[A] =
          slice.close()
      }
  }
}

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
class Slice[+T] private(array: Array[T],
                        val fromOffset: Int,
                        val toOffset: Int,
                        private var written: Int)(implicit val iterableEvidence: ClassTag[T]@uncheckedVariance) extends collection.immutable.Iterable[T]
                                                                                                                 with IterableOps[T, Slice, Slice[T]]
                                                                                                                 with EvidenceIterableFactoryDefaults[T, Slice, ClassTag]
                                                                                                                 with StrictOptimizedIterableOps[T, Slice, Slice[T]] { self =>
//@formatter:on

  private var writePosition = fromOffset + written

  def classTag: ClassTag[_] =
    this.classTag

  val allocatedSize =
    toOffset - fromOffset + 1

  override def size: Int =
    written

  override def isEmpty =
    size == 0

  def isFull =
    size == allocatedSize

  override def nonEmpty =
    !isEmpty

  @`inline` def :+[B >: T](elem: B): Slice[B] = {
    insert(elem)
    this
  }

  /**
   * Create a new Slice for the offsets.
   *
   * @param fromOffset start offset
   * @param toOffset   end offset
   * @return Slice for the given offsets
   */
  override def slice(fromOffset: Int, toOffset: Int): Slice[T] =
    if (toOffset < 0) {
      Slice.empty[T]
    } else {
      //overflow check
      var fromOffsetAdjusted = fromOffset + this.fromOffset
      var toOffsetAdjusted = fromOffsetAdjusted + (toOffset - fromOffset)

      if (fromOffsetAdjusted < this.fromOffset)
        fromOffsetAdjusted = this.fromOffset

      if (toOffsetAdjusted > this.toOffset)
        toOffsetAdjusted = this.toOffset

      if (fromOffsetAdjusted > toOffsetAdjusted) {
        Slice.empty
      } else {
        val actualWritePosition = this.fromOffset + written //in-case the slice was manually moved.
        val sliceWritePosition =
          if (actualWritePosition <= fromOffsetAdjusted) //not written
            0
          else if (actualWritePosition > toOffsetAdjusted) //fully written
            toOffsetAdjusted - fromOffsetAdjusted + 1
          else //partially written
            actualWritePosition - fromOffsetAdjusted

        new Slice[T](
          array = array,
          fromOffset = fromOffsetAdjusted,
          toOffset = toOffsetAdjusted,
          written = sliceWritePosition
        )
      }
    }

  private def splitAt(index: Int, size: Int): (Slice[T], Slice[T]) =
    if (index == 0) {
      (Slice.empty[T], slice(0, size - 1))
    } else {
      val split1 = slice(0, index - 1)
      val split2 = slice(index, size - 1)
      (split1, split2)
    }

  def splitInnerArrayAt(index: Int): (Slice[T], Slice[T]) =
    splitAt(index, allocatedSize)

  override def splitAt(index: Int): (Slice[T], Slice[T]) =
    splitAt(index, size)

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

    if (size == 0)
      Slice(this)
    else
      group(Slice.create[Slice[T]](size), this, size)
  }

  @throws[ArrayIndexOutOfBoundsException]
  private[swaydb] def moveWritePosition(writePosition: Int): Unit = {
    val adjustedPosition = fromOffset + writePosition
    //+1 because write position can be a step ahead for the next write but cannot over over toOffset.
    if (adjustedPosition > toOffset + 1) throw new ArrayIndexOutOfBoundsException(adjustedPosition)
    this.writePosition = adjustedPosition
    written = adjustedPosition max written
  }

  private[swaydb] def openEnd(): Slice[T] =
    new Slice[T](
      array = array,
      fromOffset = fromOffset,
      toOffset = array.length - 1,
      written = array.length - fromOffset
    )

  override def drop(count: Int): Slice[T] =
    if (count <= 0)
      this
    else if (count >= size)
      Slice.empty[T]
    else
      slice(count, size - 1)

  def dropHead(): Slice[T] =
    drop(1)

  /**
   * @return Elements after the input element
   *         Returns None if input element is not found.
   */
  def dropTo[B >: T](elem: B): Option[Slice[T]] =
    indexOf(elem) map {
      index =>
        drop(index + 1)
    }

  /**
   * @return input Element and elements after the input element.
   *         Returns None if input element is not found.
   */
  def dropUntil[B >: T](elem: B): Option[Slice[T]] =
    indexOf(elem) map {
      index =>
        drop(index)
    }

  override def dropRight(count: Int): Slice[T] =
    if (count <= 0)
      this
    else if (count >= size)
      Slice.empty[T]
    else
      slice(0, size - count - 1)

  override def take(count: Int): Slice[T] =
    if (size == count)
      this
    else
      slice(0, (size min count) - 1)

  def take(fromIndex: Int, count: Int): Slice[T] =
    if (count == 0)
      Slice.empty
    else
      slice(fromIndex, fromIndex + count - 1)

  override def takeRight(count: Int): Slice[T] =
    if (size == count)
      this
    else
      slice(size - count, size - 1)

  override def head: T =
    headOption.get

  override def last: T =
    lastOption.get

  override def headOption: Option[T] =
    if (written <= 0)
      None
    else
      Some(array(fromOffset))

  override def lastOption: Option[T] =
    if (written <= 0)
      None
    else
      Some(array(fromOffset + written - 1))

  def headSlice: Slice[T] = slice(0, 0)

  def lastSlice: Slice[T] = slice(size - 1, size - 1)

  @throws[ArrayIndexOutOfBoundsException]
  def get(index: Int): T = {
    val adjustedIndex = fromOffset + index
    //    if (adjustedIndex < fromOffset || adjustedIndex > toOffset) throw new ArrayIndexOutOfBoundsException(index)
    array(adjustedIndex)
  }

  def indexOf[B >: T](elem: B): Option[Int] = {
    var index = 0
    var found = Option.empty[Int]
    while (found.isEmpty && index < size) {
      val next = get(index)
      if (elem == next)
        found = Some(index)
      else
        None
      index += 1
    }
    found
  }

  /**
   * Returns a new non-writable slice. Unless position is moved manually.
   */
  def close(): Slice[T] =
    if (allocatedSize == size)
      this
    else
      slice(0, size - 1)

  def apply(index: Int): T =
    get(index)

  @throws[ArrayIndexOutOfBoundsException]
  private[slice] def insert(item: Any): Unit = {
    if (writePosition < fromOffset || writePosition > toOffset) throw new ArrayIndexOutOfBoundsException(writePosition)
    array(writePosition) = item.asInstanceOf[T]
    writePosition += 1
    written = (writePosition - fromOffset) max written
  }

  @throws[ArrayIndexOutOfBoundsException]
  private[slice] def insertAll(items: Iterable[Any]): Unit = {
    val futurePosition = writePosition + items.size - 1
    if (futurePosition < fromOffset || futurePosition > toOffset) throw new ArrayIndexOutOfBoundsException(futurePosition)
    items match {
      case array: mutable.WrappedArray[T] =>
        Array.copy(array.array, 0, this.array, currentWritePosition, items.size)

      case items: Slice[T] =>
        Array.copy(items.unsafeInnerArray, items.fromOffset, this.array, currentWritePosition, items.size)

      case _ =>
        throw IO.throwable(s"Iterable is neither an Array or Slice. ${items.getClass.getName}")
    }
    writePosition += items.size
    written = (writePosition - fromOffset) max written
  }

  private[slice] def toByteBufferWrap: ByteBuffer =
    ByteBuffer.wrap(array.asInstanceOf[Array[Byte]], fromOffset, size)

  private[slice] def toByteBufferDirect: ByteBuffer =
    ByteBuffer
      .allocateDirect(size)
      .put(array.asInstanceOf[Array[Byte]], 0, size)

  private[slice] def toByteArrayInputStream: ByteArrayInputStream =
    new ByteArrayInputStream(array.asInstanceOf[Array[Byte]], fromOffset, size)

  private def unsafeInnerArray: Array[_] =
    this.array.asInstanceOf[Array[_]]

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

  def isOriginalSlice =
    array.length == size

  def isOriginalFullSlice =
    isOriginalSlice && isFull

  def arrayLength =
    array.length

  def unslice(): Slice[T] =
    Slice(toArray)

  def toOptionUnsliced(): Option[Slice[T]] = {
    val slice = unslice()
    if (slice.isEmpty)
      None
    else
      Some(slice)
  }

  def toOption: Option[Slice[T]] =
    if (this.isEmpty)
      None
    else
      Some(this)

  override def iterator = new Iterator[T] {
    private val writtenPosition = fromOffset + self.size - 1
    private var index = fromOffset

    override def hasNext: Boolean =
      index <= toOffset && index <= writtenPosition

    override def next(): T = {
      val next = array(index)
      index += 1
      next
    }
  }

  def reverse: Iterator[T] = new Iterator[T] {
    private var position = toOffset min (fromOffset + self.size - 1)

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

  def ++[B >: T : ClassTag](other: Slice[B]): Slice[B] = {
    val slice = Slice.create[B](size + other.size)
    slice addAll this
    slice addAll other
  }

  def ++[B >: T : ClassTag](other: Array[B]): Slice[B] = {
    val slice = Slice.create[B](size + other.length)
    slice addAll this
    slice addAll other
  }

  def underlyingArraySize =
    array.length

  private[swaydb] def underlyingWrittenArrayUnsafe[X >: T]: (Array[X], Int, Int) =
    (array.asInstanceOf[Array[X]], fromOffset, size)

  /**
   * Return a new ordered Slice.
   */
  def sorted[B >: T](implicit ordering: Ordering[B]): Slice[B] =
    Slice(toArrayCopy.sorted(ordering))

  def currentWritePosition =
    writePosition

  override def evidenceIterableFactory: SliceFactory =
    new SliceFactory(size)

  //Ok - why is iterableFactory required when there is ClassTagIterableFactory.
  override def iterableFactory: IterableFactory[Slice] =
    new ClassTagIterableFactory.AnyIterableDelegate[Slice](evidenceIterableFactory)

  override def equals(that: Any): Boolean =
    that match {
      case other: Slice[T] =>
        this.size == other.size &&
          this.iterator.sameElements(other.iterator)

      case _ =>
        false
    }

  override def hashCode(): Int = {
    var seed = MurmurHash3.arraySeed
    var i = fromOffset
    val end = fromOffset + self.size
    while (i < end) {
      seed = MurmurHash3.mix(seed, array(i).##)
      i += 1
    }
    MurmurHash3.finalizeHash(seed, size)
  }

}
