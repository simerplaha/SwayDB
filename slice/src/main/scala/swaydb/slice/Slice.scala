/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.slice

import swaydb.slice.utils.ByteOps
import swaydb.utils.SomeOrNoneCovariant

import java.io.ByteArrayInputStream
import java.lang
import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}
import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.compat.IterableOnce
import scala.collection.mutable.ListBuffer
import scala.collection.{Iterable, Iterator, mutable}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

/**
 * Provides base implementation for [[Slice]] to be used as Option.
 *
 * Slice is used heavily by core and every other API so we cannot use
 * `Option[Slice[T]]` as it increases memory allocations.
 */
sealed trait SliceOption[@specialized(Byte) +T] extends SomeOrNoneCovariant[SliceOption[T], Slice[T]] {
  override def noneC: SliceOption[Nothing] = Slice.Null

  def isNullOrNonEmptyCut: Boolean

  def asSliceOption(): SliceOption[T]

  def cutToSliceOption(): SliceOption[T] =
    if (this.isNoneC || this.getC.isEmpty)
      Slice.Null
    else
      this.getC.cut()
}

case object Slice extends SliceCompanion {

  final case object Null extends SliceOption[Nothing] {
    override val isNoneC: Boolean = true
    override def getC: Slice[Nothing] = throw new Exception(s"${Slice.productPrefix} is of type ${Slice.Null.productPrefix}")
    override def isNullOrNonEmptyCut: Boolean = true
    override def asSliceOption(): SliceOption[Nothing] = this
  }

}

/**
 * Mutable slice. This instance implements APIs that can mutate the internal [[array]].
 *
 * [[Slice]] allows managing a large [[Array]] without copying it's values unless needed.
 *
 * [[Slice]] is used extensively by all modules including core so it's performance is critical.
 */
final class SliceMut[@specialized(Byte) +T](protected[this] override val array: Array[T],
                                            val fromOffset: Int,
                                            val toOffset: Int,
                                            private var _written: Int)(override implicit val classTag: ClassTag[T]@uncheckedVariance) extends Slice[T] { self =>

  override type This = SliceMut[T]@uncheckedVariance

  private var writePosition =
    fromOffset + _written

  override def size: Int =
    _written

  override def written: Int =
    _written

  @inline override def asMut(): SliceMut[T] =
    this

  override protected[this] def createEmpty: SliceMut[T] =
    Slice.of[T](0)

  override protected[this] def createNew(array: Array[T]): SliceMut[T] =
    Slice(array).asMut()

  override protected[this] def createNew(array: Array[T],
                                         fromOffset: Int,
                                         toOffset: Int,
                                         written: Int): SliceMut[T] =
    new SliceMut[T](
      array = array,
      fromOffset = fromOffset,
      toOffset = toOffset,
      _written = written
    )

  @throws[ArrayIndexOutOfBoundsException]
  private[swaydb] def moveWritePosition(writePosition: Int): Unit = {
    val adjustedPosition = fromOffset + writePosition
    //+1 because write position can be a step ahead for the next write but cannot over over toOffset.
    if (adjustedPosition > toOffset + 1) throw new ArrayIndexOutOfBoundsException(adjustedPosition)
    this.writePosition = adjustedPosition
    _written = adjustedPosition max _written
  }

  def add(item: T@uncheckedVariance): SliceMut[T] = {
    if (writePosition < fromOffset || writePosition > toOffset) throw new ArrayIndexOutOfBoundsException(writePosition)
    array(writePosition) = item
    writePosition += 1
    _written = (writePosition - fromOffset) max _written
    self
  }

  @tailrec
  def addAllOrNew(items: scala.collection.compat.IterableOnce[T]@uncheckedVariance, expandBy: Int): SliceMut[T] =
    items match {
      case array: mutable.WrappedArray[T] =>
        if (hasSpace(array.length)) {
          this.copyAll(array.array, 0, array.length)
        } else {
          //TODO - core's code make sure that this does not occur often.
          val newSlice = Slice.of[T]((this.size + array.length) * expandBy)
          newSlice addAll self
          newSlice addAll array.array.asInstanceOf[Array[T]]
        }

      case items: Slice[T] =>
        if (hasSpace(items.size)) {
          addAll[T](items)
        } else {
          //TODO - core's code make sure that this does not occur often.
          val newSlice = Slice.of[T]((this.size + items.size) * expandBy)
          newSlice addAll self
          newSlice addAll items
        }

      case items: Iterable[T] =>
        addAllOrNew(items.toArray[T], expandBy)

      case items =>
        val buffer = ListBuffer.empty[T]
        buffer ++= items

        val newSlice = Slice.of[T]((self.size + buffer.size) * expandBy)

        newSlice addAll self
        newSlice addAll buffer.toArray

        newSlice
    }

  def addAll[B >: T](items: Array[B]): SliceMut[B] =
    this.copyAll(items, 0, items.length)

  def addAll[B >: T](items: Slice[B]): SliceMut[B] =
    this.copyAll(items.unsafeInnerArray, items.fromOffset, items.size)

  def hasSpace(size: Int): Boolean = {
    val futurePosition = writePosition + size - 1
    futurePosition >= fromOffset && futurePosition <= toOffset
  }

  private def copyAll[B >: T](items: Array[_], fromPosition: Int, itemsSize: Int): SliceMut[B] =
    if (itemsSize > 0) {
      val futurePosition = writePosition + itemsSize - 1
      if (futurePosition < fromOffset || futurePosition > toOffset) throw new ArrayIndexOutOfBoundsException(futurePosition)
      Array.copy(items, fromPosition, this.array, currentWritePosition, itemsSize)
      writePosition += itemsSize
      _written = (writePosition - fromOffset) max _written
      self
    } else {
      self
    }

  def currentWritePosition =
    writePosition

  def currentWritePositionInThisSlice: Int =
    writePosition - fromOffset

  @inline def addBoolean[B >: T](bool: Boolean)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeBoolean(bool, self)
    self
  }

  @inline def addInt[B >: T](integer: Int)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeInt(integer, self)
    self
  }

  @inline def addSignedInt[B >: T](integer: Int)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeSignedInt(integer, self)
    self
  }

  @inline def addUnsignedInt[B >: T](integer: Int)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeUnsignedInt(integer, self)
    self
  }

  @inline def addNonZeroUnsignedInt[B >: T](integer: Int)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeUnsignedIntNonZero(integer, self)
    self
  }

  @inline def addLong[B >: T](num: Long)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeLong(num, self)
    self
  }

  @inline def addUnsignedLong[B >: T](num: Long)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeUnsignedLong(num, self)
    self
  }

  @inline def addSignedLong[B >: T](num: Long)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeSignedLong(num, self)
    self
  }

  @inline def addString[B >: T](string: String, charsets: Charset = StandardCharsets.UTF_8)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeString(string, self, charsets)
    self
  }

  @inline def addStringUTF8[B >: T](string: String)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeString(string, self, StandardCharsets.UTF_8)
    self
  }

  @inline def addStringUTF8WithSize[B >: T](string: String)(implicit byteOps: ByteOps[B]): SliceMut[T] = {
    byteOps.writeStringWithSize(string, self, StandardCharsets.UTF_8)
    self
  }

}

/**
 * Read-only [[Slice]] type.
 *
 * This can either be [[Slice]] or [[Slices]].
 */
sealed trait SliceRO[+A] extends Iterable[A] {

  /**
   * Get element at index after doing bound checks.
   */
  def get(index: Int): A

  /**
   * Get element at index without doing bound checks.
   */
  private[swaydb] def getUnchecked_Unsafe(index: Int): A

  /**
   * Returns this [[Slice]] if this [[Slice]] is not a sub-slice
   * else copies the content of this [[Slice]] into a new slice.
   */
  def cut(): Slice[A]

  def take(fromIndex: Int, count: Int): SliceRO[A]

  def createReader[B >: A]()(implicit byteOps: ByteOps[B]): SliceReader[B]

  def toArray: Array[A]@uncheckedVariance

}

/**
 * Immutable Slice. Can be casted to mutable with [[asMut]]
 *
 * [[Slice]] allows managing a large [[Array]] without copying it's values unless needed.
 *
 * [[Slice]] is used extensively by all modules including core so it's performance is critical.
 */
sealed trait Slice[@specialized(Byte) +T] extends SliceRO[T] with SliceOption[T] { self =>

  type This <: Slice[T]@uncheckedVariance

  val fromOffset: Int

  val toOffset: Int

  val allocatedSize: Int =
    toOffset - fromOffset + 1

  /**
   * Make parent implement written so [[size]]
   * is ensured to not invoke [[Iterable.size]]
   * for older versions of Scala.
   */
  def written: Int

  implicit def classTag: ClassTag[T]@uncheckedVariance

  protected[this] def array: Array[T]

  protected[this] def createEmpty: This

  protected[this] def createNew(array: Array[T]): This

  protected[this] def createNew(array: Array[T],
                                fromOffset: Int,
                                toOffset: Int,
                                written: Int): This

  /**
   * Returns the current slice as mutable.
   */
  @inline def asMut(): SliceMut[T]


  override def size: Int =
    written

  override val isNoneC: Boolean =
    false

  override def getC: Slice[T] =
    this

  override def isEmpty: Boolean =
    size == 0

  override def nonEmpty: Boolean =
    !isEmpty

  def isFull: Boolean =
    size == allocatedSize

  def isNullOrNonEmptyCut: Boolean =
    nonEmpty && isOriginalFullSlice

  override def asSliceOption(): SliceOption[T] =
    if (isEmpty)
      Slice.Null
    else
      self

  /**
   * Create a new SliceMut for the offsets.
   *
   * @param fromOffset start offset
   * @param toOffset   end offset
   * @return Slice for the given offsets
   */
  override def slice(fromOffset: Int, toOffset: Int): This =
    if (toOffset < 0) {
      this.createEmpty
    } else {
      //overflow check
      var fromOffsetAdjusted = fromOffset + this.fromOffset
      var toOffsetAdjusted = fromOffsetAdjusted + (toOffset - fromOffset)

      if (fromOffsetAdjusted < this.fromOffset)
        fromOffsetAdjusted = this.fromOffset

      if (toOffsetAdjusted > this.toOffset)
        toOffsetAdjusted = this.toOffset

      if (fromOffsetAdjusted > toOffsetAdjusted) {
        this.createEmpty
      } else {
        val actualWritePosition = this.fromOffset + self.size //in-case the slice was manually moved.
        val sliceWritePosition =
          if (actualWritePosition <= fromOffsetAdjusted) //not self.size
            0
          else if (actualWritePosition > toOffsetAdjusted) //fully written
            toOffsetAdjusted - fromOffsetAdjusted + 1
          else //partially written
            actualWritePosition - fromOffsetAdjusted

        self.createNew(
          array = array,
          fromOffset = fromOffsetAdjusted,
          toOffset = toOffsetAdjusted,
          written = sliceWritePosition
        )
      }
    }

  private def splitAt(index: Int, size: Int): (This, This) =
    if (index == 0) {
      (this.createEmpty, slice(0, size - 1))
    } else {
      val split1 = slice(0, index - 1)
      val split2 = slice(index, size - 1)
      (split1, split2)
    }

  def splitInnerArrayAt(index: Int): (This, This) =
    splitAt(index, allocatedSize)

  override def splitAt(index: Int): (This, This) =
    splitAt(index, size)

  override def grouped(size: Int): Iterator[Slice[T]] =
    groupedSlice(size).iterator

  def groupedSlice(size: Int): Slice[Slice[T]] = {
    @tailrec
    def group(groups: SliceMut[Slice[T]],
              slice: Slice[T],
              size: Int): Slice[Slice[T]] =
      if (size <= 1) {
        groups add slice
        groups
      } else {
        val (slice1, slice2) = slice.splitAt(slice.size / size)
        groups add slice1
        group(groups, slice2, size - 1)
      }

    if (size == 0)
      Slice(self)
    else
      group(Slice.of[Slice[T]](size), self, size)
  }

  def split[B >: T : ClassTag](blockSize: Int): Array[Slice[B]] = {
    val sliceSize = self.size
    if (sliceSize == 0) {
      Array.empty
    } else if (blockSize <= 0) {
      //there is no reason for blockSize to be <= 0. This is invalid input and
      //could result in data loss so empty returns from this function should never be allowed.
      throw new IllegalArgumentException(s"Invalid input '$blockSize'. blockSize should be > 0.")
    } else if (blockSize >= sliceSize) {
      Array(self)
    } else {
      @inline def loadFromSlice(array: Array[B], position: Int): Unit = {
        val arrayLength = array.length
        var i = 0
        while (i < arrayLength) {
          array(i) = self.get(i + position)
          i += 1
        }
      }

      val slicesCount = sliceSize / blockSize //minimum slices required
      val lastSliceLength = sliceSize % blockSize //last slices size

      val slices =
        if (lastSliceLength == 0) //no overflow. Smaller last slice not needed
          new Array[Slice[B]](slicesCount)
        else //needs another slice
          new Array[Slice[B]](slicesCount + 1)

      var i = 0
      val sliceCount = slices.length
      while (i < sliceCount) {
        val array =
          if (lastSliceLength != 0 && i == sliceCount - 1) //if last slice is of a smaller size
            new Array[B](lastSliceLength)
          else
            new Array[B](blockSize)

        loadFromSlice(array, i * blockSize)
        slices(i) = Slice(array)

        i += 1
      }

      slices
    }
  }

  /**
   * Extends the [[toOffset]] of the underlying [[array]]'s last index.
   */
  private[swaydb] def openEnd(): This =
    self.createNew(
      array = array,
      fromOffset = fromOffset,
      toOffset = array.length - 1,
      written = array.length - fromOffset
    )

  override def drop(count: Int): This =
    if (count <= 0)
      self.asInstanceOf[This]
    else if (count >= size)
      this.createEmpty
    else
      slice(count, size - 1)

  def dropHead(): This =
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

  override def dropRight(count: Int): This =
    if (count <= 0)
      self.asInstanceOf[This]
    else if (count >= size)
      this.createEmpty
    else
      slice(0, size - count - 1)

  override def take(count: Int): This =
    if (count <= 0)
      this.createEmpty
    else if (size == count)
      self.asInstanceOf[This]
    else
      slice(0, (size min count) - 1)

  def take(fromIndex: Int, count: Int): This =
    if (count == 0)
      this.createEmpty
    else
      slice(fromIndex, fromIndex + count - 1)

  override def takeRight(count: Int): This =
    if (count <= 0)
      this.createEmpty
    else if (size == count)
      self.asInstanceOf[This]
    else
      slice(size - count, size - 1)

  //For performance. To avoid creation of Some wrappers
  def headOrNull: T =
    if (self.size <= 0)
      null.asInstanceOf[T]
    else
      array(fromOffset)

  //For performance. To avoid creation of Some wrappers
  def lastOrNull: T =
    if (self.size <= 0)
      null.asInstanceOf[T]
    else
      array(fromOffset + self.size - 1)

  override def head: T = {
    val headValue = headOrNull
    if (headValue == null)
      throw new Exception(s"Slice is empty. Written: ${self.size}")
    else
      headValue
  }

  override def last: T = {
    val lastValue = lastOrNull
    if (lastValue == null)
      throw new Exception(s"Slice is empty. Written: ${self.size}")
    else
      lastValue
  }

  override def headOption: Option[T] =
    Option(headOrNull)

  override def lastOption: Option[T] =
    Option(lastOrNull)

  def headSlice: This = slice(0, 0)

  def lastSlice: This = slice(size - 1, size - 1)

  /**
   * @return the element at given index after running bound checks.
   */
  @throws[ArrayIndexOutOfBoundsException]
  def get(index: Int): T = {
    val adjustedIndex = fromOffset + index
    if (adjustedIndex < fromOffset || adjustedIndex > toOffset) throw new ArrayIndexOutOfBoundsException(index)
    array(adjustedIndex)
  }

  /**
   * Always prefer [[get]] unless there are strong guarantees that your code is
   * not reading outside this slice's offset bounds - [[toOffset]] & [[fromOffset]].
   *
   * Internally this used at performance critical areas.
   *
   * @return the element at given index without doing slice offset bound checks.
   */
  @throws[ArrayIndexOutOfBoundsException]
  @inline private[swaydb] def getUnchecked_Unsafe(index: Int): T =
    array(fromOffset + index)

  def indexOf[B >: T](elem: B): Option[Int] = {
    var index = 0
    var found = Option.empty[Int]

    while (found.isEmpty && index < size) {
      if (elem == get(index))
        found = Some(index)

      index += 1
    }

    found
  }

  /**
   * Returns a new non-writable slice. Unless position is moved manually.
   */
  def close(): This =
    if (allocatedSize == size)
      self.asInstanceOf[This]
    else
      slice(0, size - 1)

  def apply(index: Int): T =
    get(index)

  def toByteBufferWrap: ByteBuffer =
    ByteBuffer.wrap(array.asInstanceOf[Array[Byte]], fromOffset, size)

  def toByteBufferDirect: ByteBuffer =
    ByteBuffer
      .allocateDirect(size)
      .put(array.asInstanceOf[Array[Byte]], 0, size)

  def toByteArrayInputStream: ByteArrayInputStream =
    new ByteArrayInputStream(array.asInstanceOf[Array[Byte]], fromOffset, size)

  /**
   * WARNING: Do not access this directly. Use [[toArray]] or [[toArrayCopy]] instead.
   *
   * This returns the inner array maintained by this Slice instance which
   * could be a much larger than the offsets sets by the current Slice instance.
   *
   * Core module uses the function for performance critical areas only.
   */
  private[slice] def unsafeInnerArray: Array[_] =
    this.array.asInstanceOf[Array[_]]

  /**
   * Returns the original Array if Slice is not a sub Slice
   * else returns a new copied Array from the offsets defined for this Slice.
   */
  override def toArray[B >: T](implicit evidence$1: ClassTag[B]): Array[B] =
    if (size == array.length)
      if (size == 0)
        Array.empty
      else
        array.asInstanceOf[Array[B]]
    else
      toArrayCopy[B]

  //for java
  def toArray: Array[T]@uncheckedVariance =
    if (size == array.length)
      if (size == 0)
        Array.empty
      else
        array
    else
      toArrayCopy

  def toArrayCopy[B >: T](implicit evidence$1: ClassTag[B]): Array[B] =
    if (size == 0) {
      Array.empty
    } else {
      val newArray = new Array[B](size)
      Array.copy(array, fromOffset, newArray, 0, size)
      newArray
    }

  /**
   * Convenience function to convert Slice<Byte> to byte[] from Java
   */
  def toByteArray: Array[Byte] =
    try
      toArray.asInstanceOf[Array[Byte]]
    catch {
      case root: ClassCastException =>
        //converts ClassCastException to something that is not cryptic when calling from Java.
        val exception = new ClassCastException(s"${this.classTag.runtimeClass} cannot be casted to byte")
        exception.addSuppressed(root)
        throw exception
    }

  //for java
  def toArrayCopy: Array[T]@uncheckedVariance =
    if (size == 0) {
      Array.empty
    } else {
      val newArray = new Array[T](size)
      Array.copy(array, fromOffset, newArray, 0, size)
      newArray
    }

  def isOriginalSlice =
    array.length == size

  def isOriginalFullSlice =
    isOriginalSlice && isFull

  override def cut(): This =
    if (size == array.length)
      self.asInstanceOf[This]
    else
      createNew(toArray)

  def cutToOption(): Option[Slice[T]] = {
    val slice = cut()
    if (slice.isEmpty)
      None
    else
      Some(slice)
  }

  def toOption: Option[Slice[T]] =
    if (this.isEmpty)
      None
    else
      Some(self)

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

  def mapToSlice[B: ClassTag](f: T => B): Slice[B] = {
    val slice = Slice.of[B](self.size)
    val iterator = self.iterator

    while (iterator.hasNext)
      slice add f(iterator.next())

    slice
  }

  def flatMapToSliceSlow[B: ClassTag](f: T => IterableOnce[B]): Slice[B] = {
    val buffer = ListBuffer.empty[B]
    val iterator = self.iterator

    while (iterator.hasNext)
      buffer ++= f(iterator.next())

    Slice.from[B](buffer, buffer.size)
  }

  override def takeWhile(p: T => Boolean): This = {
    val filtered = Slice.of[T](self.size)
    val iterator = self.iterator

    var continue = true
    while (continue && iterator.hasNext) {
      val item = iterator.next()
      if (p(item))
        filtered add item
      else
        continue = false
    }

    filtered.close().asInstanceOf[This]
  }

  def collectToSlice[B: ClassTag](pf: PartialFunction[T, B]): Slice[B] =
    collectToSliceAndClose(Slice.of[B](self.size), pf)

  /**
   * Collects but also inserts the [[head]] item which is NOT by the partial function.
   */
  def collectToSlice[B: ClassTag](head: B)(pf: PartialFunction[T, B]): Slice[B] = {
    val target =
      Slice
        .of[B](self.size + 1)
        .add(head)

    collectToSliceAndClose(target, pf)
  }

  @inline private def collectToSliceAndClose[B: ClassTag](target: SliceMut[B], pf: PartialFunction[T, B]): Slice[B] = {
    val iterator = self.iterator

    while (iterator.hasNext) {
      val item = iterator.next()

      if (pf.isDefinedAt(item))
        target add pf(item)
    }

    target.close()
  }

  override def filterNot(p: T => Boolean): This = {
    val filtered = Slice.of[T](self.size)
    val iterator = self.iterator

    while (iterator.hasNext) {
      val item = iterator.next()
      if (!p(item)) filtered add item
    }

    filtered.close().asInstanceOf[This]
  }

  override def filter(p: T => Boolean): This = {
    val filtered = Slice.of[T](self.size)
    val iterator = self.iterator

    while (iterator.hasNext) {
      val item = iterator.next()
      if (p(item)) filtered add item
    }

    filtered.close().asInstanceOf[This]
  }

  def existsFor(forItems: Int, exists: T => Boolean): Boolean =
    take(forItems).exists(exists)

  def replaceHeadCopy[A >: T : ClassTag](newHead: A): Slice[A] =
    if (this.isEmpty) {
      throw new Exception("Slice is empty")
    } else if (this.size == 1) {
      Slice(newHead)
    } else {
      val updatedSlice = Slice.of[A](this.size)
      updatedSlice add newHead
      updatedSlice addAll this.dropHead()
    }

  def replaceLastCopy[A >: T : ClassTag](newLast: A): Slice[A] =
    if (this.isEmpty) {
      throw new Exception("Slice is empty")
    } else if (this.size == 1) {
      Slice(newLast)
    } else {
      val updatedSlice = Slice.of[A](this.size)
      updatedSlice addAll this.dropRight(1)
      updatedSlice add newLast
    }

  def updateBinarySearchCopy[A >: T : ClassTag](target: A, update: A)(implicit ordering: Ordering[A]): Slice[A] = {
    val index = binarySearchIndexOf(target)
    if (index == -1) {
      throw new Exception(s"Item $target not found")
    } else {
      val updatedSlice = Slice.of[A](this.size)
      if (index != 0) updatedSlice addAll this.take(index)
      updatedSlice add update
      updatedSlice addAll this.take(index + 1, this.size - (index + 1))
    }
  }

  def binarySearch[A >: T, N <: A](target: A, nullValue: N)(implicit ordering: Ordering[A]): A = {
    val index = binarySearchIndexOf(target)
    if (index == -1)
      nullValue
    else
      this.get(index)
  }

  def binarySearchIndexOf[A >: T](target: A)(implicit ordering: Ordering[A]): Int = {
    var start = 0
    var end = size - 1

    while (start <= end) {
      val mid = start + (end - start) / 2
      val element = get(mid)
      val compare = ordering.compare(element, target)
      if (compare == 0)
        return mid
      else if (compare < 0)
        start = mid + 1
      else
        end = mid - 1
    }

    -1
  }

  def ++[B >: T : ClassTag](other: Slice[B]): Slice[B] =
    if (other.isEmpty) {
      self
    } else if (self.isEmpty) {
      other
    } else {
      val slice = Slice.of[B](size + other.size)
      slice addAll self
      slice addAll other
    }

  def ++[B >: T : ClassTag](other: Array[B]): Slice[B] =
    if (other.isEmpty) {
      self
    } else if (self.isEmpty) {
      Slice(other)
    } else {
      val slice = Slice.of[B](size + other.length)
      slice addAll self
      slice addAll other
    }

  def ++[B >: T : ClassTag](other: Option[B]): Slice[B] =
    if (other.isEmpty) {
      self
    } else if (self.isEmpty) {
      Slice(other.get)
    } else {
      val slice = Slice.of[B](size + 1)
      slice addAll self
      slice add other.get
    }

  override def collectFirst[B](pf: PartialFunction[T, B]): Option[B] =
    iterator.collectFirst(pf)

  def underlyingArraySize =
    array.length

  private[swaydb] def underlyingWrittenArrayUnsafe[X >: T]: (Array[X], Int, Int) =
    (array.asInstanceOf[Array[X]], fromOffset, size)

  /**
   * Return a new ordered Slice.
   */
  def sorted[B >: T](implicit ordering: Ordering[B]): Slice[B] =
    Slice(toArrayCopy.sorted(ordering))

  def sortBy[B >: T, C](f: T => C)(implicit ordering: Ordering[C]): Slice[B] =
    Slice(toArrayCopy.sortBy(f)(ordering))

  /**
   * @return A tuple2 where _1 is written bytes and _2 is tail unwritten bytes.
   */
  def splitUnwritten(): (This, This) =
    (this.close(), unwrittenTail())

  def unwrittenTailSize() =
    toOffset - fromOffset - size

  def unwrittenTail(): This = {
    val from = fromOffset + size
    if (from > toOffset)
      this.createEmpty
    else
      self.createNew(
        array = array,
        fromOffset = from,
        toOffset = toOffset,
        written = 0
      )
  }

  def copy(): This =
    self.createNew(
      array = array,
      fromOffset = fromOffset,
      toOffset = toOffset,
      written = self.size
    )

  @inline def readBoolean[B >: T]()(implicit byteOps: ByteOps[B]): Boolean =
    byteOps.readBoolean(self)

  @inline def readInt[B >: T]()(implicit byteOps: ByteOps[B]): Int =
    byteOps.readInt(self)

  @inline def dropUnsignedInt[B >: T]()(implicit byteOps: ByteOps[B]): This = {
    val (_, byteSize) = readUnsignedIntWithByteSize[B]()
    self drop byteSize
  }

  @inline def readSignedInt[B >: T]()(implicit byteOps: ByteOps[B]): Int =
    byteOps.readSignedInt(self)

  @inline def readUnsignedInt[B >: T]()(implicit byteOps: ByteOps[B]): Int =
    byteOps.readUnsignedInt(self)

  @inline def readUnsignedIntWithByteSize[B >: T]()(implicit byteOps: ByteOps[B]): (Int, Int) =
    byteOps.readUnsignedIntWithByteSize(self)

  @inline def readNonZeroUnsignedIntWithByteSize[B >: T]()(implicit byteOps: ByteOps[B]): (Int, Int) =
    byteOps.readUnsignedIntNonZeroWithByteSize(self)

  @inline def readLong[B >: T]()(implicit byteOps: ByteOps[B]): Long =
    byteOps.readLong(self)

  @inline def readUnsignedLong[B >: T]()(implicit byteOps: ByteOps[B]): Long =
    byteOps.readUnsignedLong(self)

  @inline def readUnsignedLongWithByteSize[B >: T]()(implicit byteOps: ByteOps[B]): (Long, Int) =
    byteOps.readUnsignedLongWithByteSize(self)

  @inline def readUnsignedLongByteSize[B >: T]()(implicit byteOps: ByteOps[B]): Int =
    byteOps.readUnsignedLongByteSize(self)

  @inline def readSignedLong[B >: T]()(implicit byteOps: ByteOps[B]): Long =
    byteOps.readSignedLong(self)

  @inline def readString[B >: T](charset: Charset = StandardCharsets.UTF_8)(implicit byteOps: ByteOps[B]): String =
    byteOps.readString(self, charset)

  @inline def readStringUTF8[B >: T]()(implicit byteOps: ByteOps[B]): String =
    byteOps.readString(self, StandardCharsets.UTF_8)

  @inline def createReader[B >: T]()(implicit byteOps: ByteOps[B]): SliceReader[B] =
    SliceReader[B](self)

  @inline def append[B >: T : ClassTag](tail: Slice[B]): Slice[B] = {
    val merged = Slice.of[B](self.size + tail.size)
    merged addAll self
    merged addAll tail
    merged
  }

  @inline def append[B >: T : ClassTag](last: B): Slice[B] = {
    val merged = Slice.of[B](self.size + 1)
    merged addAll self
    merged add last
    merged
  }

  @inline def prepend[B >: T : ClassTag](head: B): Slice[B] = {
    val merged = Slice.of[B](self.size + 1)
    merged add head
    merged addAll self
    merged
  }

  /**
   * Avoid using the default flatMap implementation
   * so that we always restrict the API to use
   * Slices which is faster when copying arrays.
   *
   * Use [[flatMapSlow]] to allow any IterableOnce instance.
   */
  def flatMap[B: ClassTag](f: T => Slice[B]): Slice[B] =
    if (self.isEmpty) {
      Slice.empty[B]
    } else if (self.size == 1) {
      f(head)
    } else {
      val result = Slice.of[Slice[B]](self.size)

      this foreach {
        item =>
          result add f(item)
      }

      result.flatten[B]
    }

  def flatten[B: ClassTag](implicit evd: T <:< Slice[B]): Slice[B] = {
    var size = 0

    self foreach {
      innerSlice =>
        size += innerSlice.size
    }

    val newSlice = Slice.of[B](size)

    self foreach {
      innerSlice =>
        newSlice addAll innerSlice
    }

    newSlice
  }

  def asJava(): lang.Iterable[T]@uncheckedVariance =
    (this: Iterable[T]).asJava

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

/**
 *
 * Stores multiple slices and implements some functions to
 * provide similar functionality as [[Slice]].
 *
 * This type is mainly used by BlockCache which stores byte arrays
 * in the configured block-size via <a href="https://swaydb.io/configuration/memoryCache/?language=java">minIOSeekSize</a>.
 *
 * When a read is submitted to BlockCache which is greater than the configured `minIOSeekSize`
 * we do not want to copy byte arrays to form a single [[Slice]] but use [[Slices]] which can
 * hold multiple slices while still behaving like a single [[Slice]].
 *
 * [[cut]] can be used to convert this type to [[Slice]].
 *
 * Pre-Conditions: These checks are assumed and are not validated by this instance.
 *    - Head slices are always of equal size except the last slice which can be <= head slice.
 *    - [[slices]] cannot be empty.
 *
 */
case class Slices[A: ClassTag](slices: Array[Slice[A]]) extends SliceRO[A] {

  val blockSize: Int =
    slices.head.size

  //FIXME - implement without cut
  def createReader[B >: A]()(implicit byteOps: ByteOps[B]): SliceReader[B] =
    this.cut().createReader[B]()

  def get(index: Int): A =
    if (slices.length == 1)
      slices.head(index)
    else
    //slice(slot)(slotIndex)
      slices(index / blockSize).get(index % blockSize)

  override private[swaydb] def getUnchecked_Unsafe(index: Int) =
    if (slices.length == 1)
      slices.head(index)
    else
    //slice(slot)(slotIndex)
      slices(index / blockSize).getUnchecked_Unsafe(index % blockSize)

  override def take(n: Int): SliceRO[A] =
    take(fromIndex = 0, count = n)

  /**
   * Performs take without copying array elements.
   */
  override def take(fromIndex: Int, count: Int): SliceRO[A] = {
    var slot = fromIndex / blockSize //starting slot
    var slotIndex = fromIndex % blockSize //starting slot's index

    val buffer = ListBuffer.empty[Slice[A]] //collect slices in a buffer
    var taken = 0
    while (taken < count && slot < slices.length) {
      //fetch slice required
      val slotSlice = slices(slot).drop(slotIndex).take(count - taken)
      buffer += slotSlice
      taken += slotSlice.size
      slot += 1 //move to next slot
      slotIndex = 0 //set the next slot's index to be 0
    }

    if (buffer.length == 1)
      buffer.head
    else
      Slices(buffer.toArray)
  }

  override def cut(): Slice[A] =
    if (slices.length == 1)
      slices.head
    else
      Slice.from(slices)

  override def isEmpty: Boolean =
    slices.forall(_.isEmpty)

  override def nonEmpty: Boolean =
    !isEmpty

  override def size: Int =
    slices.foldLeft(0)(_ + _.size)

  override def iterator: Iterator[A] =
    slices.iterator.flatMap(_.iterator)

  def toArray: Array[A] =
    if (slices.length == 1) {
      slices.head.toArray
    } else {
      val array = new Array[A](size)
      var currentWritePosition = 0
      var i = 0
      while (i < slices.length) {
        val slice = slices(i)
        val (sliceArray, from, size) = slice.underlyingWrittenArrayUnsafe[A]
        Array.copy(sliceArray, from, array, currentWritePosition, size)
        currentWritePosition += sliceArray.length
        i += 1
      }
      array
    }

}
