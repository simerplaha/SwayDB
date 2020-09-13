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
import swaydb.data.slice.Slice.SliceFactory
import swaydb.data.util.{ByteOps, ByteSizeOf, SomeOrNoneCovariant}
import swaydb.data.{MaxKey, slice}

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection._
import scala.reflect.ClassTag

/**
 * Documentation - http://swaydb.io/slice
 */
sealed trait SliceOption[+T] extends SomeOrNoneCovariant[SliceOption[T], Slice[T]] {
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

  def createForJava(byteBuffer: ByteBuffer): Slice[java.lang.Byte] =
    new Slice[java.lang.Byte](
      array = byteBuffer.array().asInstanceOf[Array[java.lang.Byte]],
      fromOffset = byteBuffer.arrayOffset(),
      toOffset = byteBuffer.position() - 1,
      written = byteBuffer.position()
    )

  def createForJava(byteBuffer: ByteBuffer, from: Int, to: Int): Slice[java.lang.Byte] =
    new Slice[java.lang.Byte](
      array = byteBuffer.array().asInstanceOf[Array[java.lang.Byte]],
      fromOffset = from,
      toOffset = to,
      written = to - from + 1
    )

  def createForScala(byteBuffer: ByteBuffer): Slice[Byte] =
    new Slice[Byte](
      array = byteBuffer.array(),
      fromOffset = byteBuffer.arrayOffset(),
      toOffset = byteBuffer.position() - 1,
      written = byteBuffer.position()
    )

  def createForScala(byteBuffer: ByteBuffer, from: Int, to: Int): Slice[Byte] =
    new Slice[Byte](
      array = byteBuffer.array(),
      fromOffset = from,
      toOffset = to,
      written = to - from + 1
    )

  @inline final def apply[T: ClassTag](data: T*): Slice[T] =
    Slice(data.toArray)

  @inline final def writeInt[B: ClassTag](integer: Int)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](ByteSizeOf.int).addInt(integer)

  @inline final def writeBoolean[B: ClassTag](bool: Boolean)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](1).addBoolean(bool)

  @inline final def writeUnsignedInt[B: ClassTag](integer: Int)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](ByteSizeOf.varInt).addUnsignedInt(integer).close()

  @inline final def writeLong[B: ClassTag](num: Long)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](ByteSizeOf.long).addLong(num)

  @inline final def writeUnsignedLong[B: ClassTag](num: Long)(implicit byteOps: ByteOps[B]): Slice[B] =
    Slice.create[B](ByteSizeOf.varLong).addUnsignedLong(num).close()

  @inline final def writeString[B](string: String, charsets: Charset = StandardCharsets.UTF_8)(implicit byteOps: ByteOps[B]): Slice[B] =
    byteOps.writeString(string, charsets)

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

  final case object Null extends SliceOption[Nothing] {
    override val isNoneC: Boolean = true
    override def getC: Slice[Nothing] = throw new Exception("Slice is of type Null")
    override def isUnslicedOption: Boolean = true
    override def asSliceOption(): SliceOption[Nothing] = this
  }

  @inline final def newBuilder[T: ClassTag](sizeHint: Int): Slice.SliceBuilder[T] =
    new slice.Slice.SliceBuilder[T](sizeHint)

  private[swaydb] def newAggregator[T: ClassTag](sizeHint: Int): Aggregator[T, Slice[T]] =
    Aggregator.fromBuilder[T, Slice[T]](newBuilder[T](sizeHint))

  class SliceBuilder[A: ClassTag](sizeHint: Int) extends mutable.Builder[A, Slice[A]] {
    //max is used to in-case sizeHit == 0 which is possible for cases where (None ++ Some(Slice[T](...)))
    protected var slice: Slice[A] = Slice.create[A]((sizeHint * 2) max 100)

    def extendSlice(by: Int) = {
      val extendedSlice = Slice.create[A](slice.size * by)
      extendedSlice addAll slice
      slice = extendedSlice
    }

    @tailrec
    final override def addOne(x: A): this.type =
      try {
        slice add x
        this
      } catch {
        case _: ArrayIndexOutOfBoundsException => //Extend slice.
          extendSlice(by = 2)
          addOne(x)
      }

    def clear() =
      slice = Slice.create[A](slice.size)

    def result(): Slice[A] =
      slice.close()
  }

  class SliceFactory(sizeHint: Int) extends ClassTagIterableFactory[Slice] {

    def from[A](source: IterableOnce[A])(implicit evidence: ClassTag[A]): Slice[A] =
      (newBuilder[A] ++= source).result()

    def empty[A](implicit evidence: ClassTag[A]): Slice[A] =
      Slice.create[A](sizeHint)

    def newBuilder[A](implicit evidence: ClassTag[A]): mutable.Builder[A, Slice[A]] =
      new SliceBuilder[A](sizeHint)
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
class Slice[+T] private[slice](array: Array[T],
                               fromOffset: Int,
                               toOffset: Int,
                               written: Int)(implicit val iterableEvidence: ClassTag[T]@uncheckedVariance) extends SliceBase[T](array, fromOffset, toOffset, written)
                                                                                                              with SliceOption[T]
                                                                                                              with IterableOps[T, Slice, Slice[T]]
                                                                                                              with EvidenceIterableFactoryDefaults[T, Slice, ClassTag]
                                                                                                              with StrictOptimizedIterableOps[T, Slice, Slice[T]] {
//@formatter:on

  override val isNoneC: Boolean =
    false

  override def getC: Slice[T] =
    this

  override def selfSlice: Slice[T] =
    this

  override def evidenceIterableFactory: SliceFactory =
    new SliceFactory(size)

  //Ok - why is iterableFactory required when there is ClassTagIterableFactory.
  override def iterableFactory: IterableFactory[Slice] =
    new ClassTagIterableFactory.AnyIterableDelegate[Slice](evidenceIterableFactory)
}
