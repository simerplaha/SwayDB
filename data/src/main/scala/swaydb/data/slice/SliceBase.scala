/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

import swaydb.IO

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection.mutable
import scala.reflect.ClassTag
import scala.util.hashing.MurmurHash3

/**
 * Base implementation for both Scala 2.12 and 2.13.
 */
abstract class SliceBase[+T](array: Array[T],
                             val fromOffset: Int,
                             val toOffset: Int,
                             private var written: Int)(implicit val classTag: ClassTag[T]@uncheckedVariance) extends Iterable[T] { self =>

  private var writePosition = fromOffset + written

  def selfSlice: Slice[T]

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

  def isUnslicedOptional: Boolean =
    nonEmpty && isOriginalFullSlice

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
      Slice(selfSlice)
    else
      group(Slice.create[Slice[T]](size), selfSlice, size)
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
      selfSlice
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
      selfSlice
    else if (count >= size)
      Slice.empty[T]
    else
      slice(0, size - count - 1)

  override def take(count: Int): Slice[T] =
    if (count <= 0)
      Slice.empty[T]
    else if (size == count)
      selfSlice
    else
      slice(0, (size min count) - 1)

  def take(fromIndex: Int, count: Int): Slice[T] =
    if (count == 0)
      Slice.empty
    else
      slice(fromIndex, fromIndex + count - 1)

  override def takeRight(count: Int): Slice[T] =
    if (count <= 0)
      Slice.empty[T]
    else if (size == count)
      selfSlice
    else
      slice(size - count, size - 1)

  //For performance. To avoid creation of Some wrappers
  private[swaydb] def headOrNull: T =
    if (written <= 0)
      null.asInstanceOf[T]
    else
      array(fromOffset)

  //For performance. To avoid creation of Some wrappers
  private[swaydb] def lastOrNull: T =
    if (written <= 0)
      null.asInstanceOf[T]
    else
      array(fromOffset + written - 1)

  override def head: T = {
    val headValue = headOrNull
    if (headValue == null)
      throw new Exception(s"Slice is empty. Written: $written")
    else
      headValue
  }

  override def last: T = {
    val lastValue = lastOrNull
    if (lastValue == null)
      throw new Exception(s"Slice is empty. Written: $written")
    else
      lastValue
  }

  override def headOption: Option[T] =
    Option(headOrNull)

  override def lastOption: Option[T] =
    Option(lastOrNull)

  def headSlice: Slice[T] = slice(0, 0)

  def lastSlice: Slice[T] = slice(size - 1, size - 1)

  @throws[ArrayIndexOutOfBoundsException]
  def get(index: Int): T = {
    val adjustedIndex = fromOffset + index
    if (adjustedIndex < fromOffset || adjustedIndex > toOffset) throw new ArrayIndexOutOfBoundsException(index)
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
      selfSlice
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

  private[slice] def unsafeInnerArray: Array[_] =
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
      Some(selfSlice)

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

  def existsFor(forItems: Int, exists: T => Boolean): Boolean =
    take(forItems).exists(exists)

  def ++[B >: T : ClassTag](other: Slice[B]): Slice[B] = {
    val slice = Slice.create[B](size + other.size)
    slice addAll selfSlice
    slice addAll other
  }

  def ++[B >: T : ClassTag](other: Array[B]): Slice[B] = {
    val slice = Slice.create[B](size + other.length)
    slice addAll selfSlice
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

  def currentWritePositionInThisSlice: Int =
    writePosition - fromOffset

  /**
   * @return A tuple2 where _1 is written bytes and _2 is tail unwritten bytes.
   */
  def splitUnwritten(): (Slice[T], Slice[T]) =
    (this.close(), unwrittenTail())

  def unwrittenTail(): Slice[T] = {
    val from = fromOffset + size
    if (from > toOffset)
      Slice.empty[T]
    else
      new Slice[T](
        array = array,
        fromOffset = from,
        toOffset = toOffset,
        written = 0
      )
  }

  def copy() =
    new Slice[T](
      array = array,
      fromOffset = fromOffset,
      toOffset = toOffset,
      written = written
    )

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
