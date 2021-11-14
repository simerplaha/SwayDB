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

package swaydb.data.slice

import swaydb.Aggregator
import swaydb.data.MaxKey
import swaydb.data.utils.ByteOps
import swaydb.utils.ByteSizeOf

import java.lang
import java.nio.ByteBuffer
import java.nio.charset.{Charset, StandardCharsets}
import scala.collection.{Iterable, Iterator, Seq}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.ClassTag

/**
 * Companion implementation for [[Slice]].
 *
 * This is a trait because the [[Slice]] class itself is getting too
 * long even though inheritance such as like this is discouraged.
 */
trait SliceCompanion extends SliceBuildFrom {

  val emptyBytes: Slice[Byte] =
    of[Byte](0)

  val emptyJavaBytes: Slice[lang.Byte] =
    of[java.lang.Byte](0)

  @inline final def empty[T: ClassTag]: Slice[T] =
    of[T](0)

  final def range(from: Int, to: Int): Slice[Int] = {
    val slice = of[Int](to - from + 1).asMut()
    (from to to) foreach slice.add
    slice
  }

  final def range(from: Char, to: Char): Slice[Char] = {
    val slice = of[Char](26).asMut()
    (from to to) foreach slice.add
    slice.close()
  }

  final def range(from: Byte, to: Byte): Slice[Byte] = {
    val slice = of[Byte](to - from + 1).asMut()

    (from to to) foreach {
      i =>
        slice add i.toByte
    }

    slice.close()
  }

  def fill[T: ClassTag](length: Int)(elem: => T): Slice[T] =
    new SliceMut[T](
      array = Array.fill(length)(elem),
      fromOffset = 0,
      toOffset = if (length == 0) -1 else length - 1,
      _written = length
    )

  def ofBytesScala(length: Int): SliceMut[Byte] =
    of[Byte](length)

  def ofBytesJava(length: Int): SliceMut[java.lang.Byte] =
    of[java.lang.Byte](length)

  @inline final def of[T: ClassTag](length: Int, isFull: Boolean = false): SliceMut[T] =
    new SliceMut(
      array = new Array[T](length),
      fromOffset = 0,
      toOffset = if (length == 0) -1 else length - 1,
      _written = if (isFull) length else 0
    )

  def ofJava(array: Array[java.lang.Byte]): Slice[lang.Byte] =
    apply(array)

  @throws[ClassCastException]
  def ofJava(array: Array[Byte]): SliceMut[java.lang.Byte] =
    apply(array).asInstanceOf[SliceMut[java.lang.Byte]]

  def ofScala(array: Array[Byte]): Slice[Byte] =
    apply(array)

  def apply[T: ClassTag](data: Array[T]): Slice[T] =
    if (data.length == 0)
      of[T](0)
    else
      new SliceMut[T](
        array = data,
        fromOffset = 0,
        toOffset = data.length - 1,
        _written = data.length
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
    new SliceMut[java.lang.Byte](
      array = byteBuffer.array().asInstanceOf[Array[java.lang.Byte]],
      fromOffset = byteBuffer.arrayOffset(),
      toOffset = byteBuffer.position() - 1,
      _written = byteBuffer.position()
    )

  def ofJava(byteBuffer: ByteBuffer, from: Int, to: Int): Slice[java.lang.Byte] =
    new SliceMut[java.lang.Byte](
      array = byteBuffer.array().asInstanceOf[Array[java.lang.Byte]],
      fromOffset = from,
      toOffset = to,
      _written = to - from + 1
    )

  def ofScala(byteBuffer: ByteBuffer): Slice[Byte] =
    new SliceMut[Byte](
      array = byteBuffer.array(),
      fromOffset = byteBuffer.arrayOffset(),
      toOffset = byteBuffer.position() - 1,
      _written = byteBuffer.position()
    )

  def ofScala(byteBuffer: ByteBuffer, from: Int, to: Int): Slice[Byte] =
    new SliceMut[Byte](
      array = byteBuffer.array(),
      fromOffset = from,
      toOffset = to,
      _written = to - from + 1
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
    intersects(
      range1 = (range1._1, range1._2, true),
      range2 = (range2._1, range2._2, true)
    )

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

    @inline final def cut(): Option[Slice[Byte]] =
      slice flatMap {
        slice =>
          if (slice.isEmpty)
            None
          else
            Some(slice.cut())
      }
  }

  implicit class SeqByteSliceImplicits(slice: Seq[Slice[Byte]]) {

    @inline final def cut(): Seq[Slice[Byte]] =
      if (slice.isEmpty)
        slice
      else
        slice.map(_.cut())
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

  @inline private[swaydb] final def newAggregator[T: ClassTag](maxSize: Int): Aggregator[T, Slice[T]] =
    new SliceBuilder[T](maxSize)
}
