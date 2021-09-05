/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.slice

import swaydb.Aggregator
import swaydb.data.utils.ByteOps
import swaydb.data.{MaxKey, slice}
import swaydb.utils.ByteSizeOf

import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}
import scala.collection.{Iterable, Iterator, Seq}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
 * Base companion implementation for both Scala 2.12 and 2.13's [[Sliced]] companion objects.
 */
trait SliceCompanionBase {

  val emptyBytes = of[Byte](0)

  val emptyJavaBytes = of[java.lang.Byte](0)

  val someEmptyBytes = Some(emptyBytes)

  private[swaydb] val emptyEmptyBytes: Slice[Slice[Byte]] = empty[Slice[Byte]]

  @inline final def empty[T: ClassTag] =
    of[T](0)

  final def range(from: Int, to: Int): Slice[Int] = {
    val slice = of[Int](to - from + 1)
    (from to to) foreach slice.add
    slice
  }

  final def range(from: Char, to: Char): Slice[Char] = {
    val slice = of[Char](26)
    (from to to) foreach slice.add
    slice.close()
  }

  final def range(from: Byte, to: Byte): Slice[Byte] = {
    val slice = of[Byte](to - from + 1)
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

  def ofBytesScala(length: Int): Slice[Byte] =
    of[Byte](length)

  def ofBytesJava(length: Int): Slice[java.lang.Byte] =
    of[java.lang.Byte](length)

  @inline final def of[T: ClassTag](length: Int, isFull: Boolean = false): Slice[T] =
    new Slice(
      array = new Array[T](length),
      fromOffset = 0,
      toOffset = if (length == 0) -1 else length - 1,
      written = if (isFull) length else 0
    )

  def ofJava(array: Array[java.lang.Byte]): Slice[java.lang.Byte] =
    apply(array)

  @throws[ClassCastException]
  def ofJava(array: Array[Byte]): Slice[java.lang.Byte] =
    apply(array).asInstanceOf[Slice[java.lang.Byte]]

  def ofScala(array: Array[Byte]): Slice[Byte] =
    apply(array)

  def apply[T: ClassTag](data: Array[T]): Slice[T] =
    if (data.length == 0)
      of[T](0)
    else
      new Slice[T](
        array = data,
        fromOffset = 0,
        toOffset = data.length - 1,
        written = data.length
      )

  def from[T: ClassTag](iterator: Iterator[T], size: Int): Slice[T] = {
    val slice = of[T](size)
    iterator foreach slice.add
    slice
  }

  def from[T: ClassTag](iterable: Iterable[T], size: Int): Slice[T] = {
    val slice = of[T](size)
    iterable foreach slice.add
    slice
  }

  def from[T: ClassTag](iterable: Iterable[Slice[T]]): Slice[T] = {
    val slice = of[T](iterable.foldLeft(0)(_ + _.size))
    iterable foreach slice.addAll
    slice
  }

  def ofJava(byteBuffer: ByteBuffer): Slice[java.lang.Byte] =
    new Slice[java.lang.Byte](
      array = byteBuffer.array().asInstanceOf[Array[java.lang.Byte]],
      fromOffset = byteBuffer.arrayOffset(),
      toOffset = byteBuffer.position() - 1,
      written = byteBuffer.position()
    )

  def ofJava(byteBuffer: ByteBuffer, from: Int, to: Int): Slice[java.lang.Byte] =
    new Slice[java.lang.Byte](
      array = byteBuffer.array().asInstanceOf[Array[java.lang.Byte]],
      fromOffset = from,
      toOffset = to,
      written = to - from + 1
    )

  def ofScala(byteBuffer: ByteBuffer): Slice[Byte] =
    new Slice[Byte](
      array = byteBuffer.array(),
      fromOffset = byteBuffer.arrayOffset(),
      toOffset = byteBuffer.position() - 1,
      written = byteBuffer.position()
    )

  def ofScala(byteBuffer: ByteBuffer, from: Int, to: Int): Slice[Byte] =
    new Slice[Byte](
      array = byteBuffer.array(),
      fromOffset = from,
      toOffset = to,
      written = to - from + 1
    )

  @inline final def apply[T: ClassTag](data: T*): Slice[T] =
    Slice(data.toArray)

  @inline final def writeInt[B](integer: Int)(implicit byteOps: ByteOps[B]): Slice[B] =
    of[B](ByteSizeOf.int)(byteOps.classTag).addInt(integer)

  @inline final def writeUnsignedInt[B](integer: Int)(implicit byteOps: ByteOps[B]): Slice[B] =
    of[B](ByteSizeOf.varInt)(byteOps.classTag).addUnsignedInt(integer).close()

  @inline final def writeSignedInt[B](integer: Int)(implicit byteOps: ByteOps[B]): Slice[B] =
    of[B](ByteSizeOf.varInt)(byteOps.classTag).addSignedInt(integer).close()

  @inline final def writeLong[B](num: Long)(implicit byteOps: ByteOps[B]): Slice[B] =
    of[B](ByteSizeOf.long)(byteOps.classTag).addLong(num)

  @inline final def writeUnsignedLong[B](num: Long)(implicit byteOps: ByteOps[B]): Slice[B] =
    of[B](ByteSizeOf.varLong)(byteOps.classTag).addUnsignedLong(num).close()

  @inline final def writeSignedLong[B](num: Long)(implicit byteOps: ByteOps[B]): Slice[B] =
    of[B](ByteSizeOf.varLong)(byteOps.classTag).addSignedLong(num).close()

  @inline final def writeBoolean[B](bool: Boolean)(implicit byteOps: ByteOps[B]): Slice[B] =
    of[B](1)(byteOps.classTag).addBoolean(bool)

  @inline final def writeString[B](string: String, charsets: Charset = StandardCharsets.UTF_8)(implicit byteOps: ByteOps[B]): Slice[B] =
    byteOps.writeString(string, charsets)

  @inline final def writeStringUTF8[B](string: String)(implicit byteOps: ByteOps[B]): Slice[B] =
    byteOps.writeString(string, StandardCharsets.UTF_8)

  @inline final def intersects[T](range1: (Slice[T], Slice[T]),
                                  range2: (Slice[T], Slice[T]))(implicit ordering: Ordering[Slice[T]]): Boolean =
    intersects((range1._1, range1._2, true), (range2._1, range2._2, true))

  def within[T](key: Slice[T],
                minKey: Slice[T],
                maxKey: MaxKey[Slice[T]])(implicit keyOrder: Ordering[Slice[T]]): Boolean =
    within(
      key = key,
      minKey = minKey,
      maxKey = maxKey.maxKey,
      maxKeyInclusive = maxKey.inclusive
    )

  def within[T](key: Slice[T],
                minKey: Slice[T],
                maxKey: Slice[T],
                maxKeyInclusive: Boolean)(implicit keyOrder: Ordering[Slice[T]]): Boolean = {
    import keyOrder._
    key >= minKey && {
      if (maxKeyInclusive)
        key <= maxKey
      else
        key < maxKey
    }
  }

  def within[T](key: T,
                minKey: T,
                maxKey: T,
                maxKeyInclusive: Boolean)(implicit keyOrder: Ordering[T]): Boolean = {
    import keyOrder._
    key >= minKey && {
      if (maxKeyInclusive)
        key <= maxKey
      else
        key < maxKey
    }
  }

  def within[T](source: MaxKey[T],
                target: MaxKey[T])(implicit keyOrder: Ordering[T]): Boolean = {
    import keyOrder._

    source match {
      case MaxKey.Fixed(sourceMaxKey) =>
        target match {
          case MaxKey.Fixed(targetMaxKey) =>
            keyOrder.equiv(sourceMaxKey, targetMaxKey)

          case MaxKey.Range(targetFromKey, targetMaxKey) =>
            Slice.within(
              key = sourceMaxKey,
              minKey = targetFromKey,
              maxKey = targetMaxKey,
              maxKeyInclusive = false
            )
        }

      case MaxKey.Range(sourceFromKey, sourceMaxKey) =>
        target match {
          case MaxKey.Fixed(targetMaxKey) =>
            keyOrder.equiv(sourceFromKey, targetMaxKey) &&
              keyOrder.equiv(sourceMaxKey, targetMaxKey)

          case MaxKey.Range(targetFromKey, targetMaxKey) =>
            Slice.within(
              key = sourceFromKey,
              minKey = targetFromKey,
              maxKey = targetMaxKey,
              maxKeyInclusive = false
            ) && sourceMaxKey <= targetMaxKey
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
  def intersects[T](range1: (T, T, Boolean),
                    range2: (T, T, Boolean))(implicit ordering: Ordering[T]): Boolean = {
    import ordering._

    def check(range1: (T, T, Boolean),
              range2: (T, T, Boolean)): Boolean =
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
      val newSlices = of[Slice[T]](slices.close().size)
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
    @inline def cast: Slice[Byte] =
      sliced.asInstanceOf[Slice[Byte]]
  }

  implicit class ScalaByteSliced(sliced: Slice[Byte]) {
    @inline def cast: Slice[java.lang.Byte] =
      sliced.asInstanceOf[Slice[java.lang.Byte]]
  }

  final def sequence[A: ClassTag](in: Iterable[Future[A]])(implicit ec: ExecutionContext): Future[Slice[A]] =
    in.iterator.foldLeft(Future.successful(Slice.of[A](in.size))) {
      case (result, next) =>
        result.zipWith(next)(_ add _)
    }

  @inline final def newBuilder[T: ClassTag](maxSize: Int): Slice.SliceBuilder[T] =
    new slice.Slice.SliceBuilder[T](maxSize)

  @inline private[swaydb] final def newAggregator[T: ClassTag](maxSize: Int): Aggregator[T, Slice[T]] =
    Aggregator.fromBuilder[T, Slice[T]](newBuilder[T](maxSize))
}
