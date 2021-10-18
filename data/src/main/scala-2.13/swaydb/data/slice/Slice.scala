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

import swaydb.data.slice.Slice.SliceFactory
import swaydb.utils.SomeOrNoneCovariant

import scala.annotation.tailrec
import scala.annotation.unchecked.uncheckedVariance
import scala.collection._
import scala.collection.compat.IterableOnce
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

object Slice extends SliceCompanionBase {

  final case object Null extends SliceOption[Nothing] {
    override val isNoneC: Boolean = true
    override def getC: Slice[Nothing] = throw new Exception("Slice is of type Null")
    override def isUnslicedOption: Boolean = true
    override def asSliceOption(): SliceOption[Nothing] = this
  }

  class SliceBuilder[A: ClassTag](maxSize: Int) extends mutable.Builder[A, Slice[A]] {
    //max is used to in-case sizeHit == 0 which is possible for cases where (None ++ Some(Slice[T](...)))
    protected var slice: Slice[A] = Slice.of[A](maxSize max 16)

    @inline def extendSlice(by: Int) = {
      val extendedSlice = Slice.of[A](slice.size * by)
      extendedSlice addAll slice
      slice = extendedSlice
    }

    @tailrec
    final override def addOne(x: A): this.type =
      if (!slice.isFull) {
        slice add x
        this
      } else {
        extendSlice(by = 2)
        addOne(x)
      }

    override def addAll(xs: IterableOnce[A]): SliceBuilder.this.type = {
      this.slice = slice.addAllOrNew(items = xs, expandBy = 2)
      this
    }

    def clear() =
      slice = Slice.of[A](slice.size)

    def result(): Slice[A] =
      slice.close()
  }

  class SliceFactory(maxSize: Int) extends ClassTagIterableFactory[Slice] {

    override def from[A](source: IterableOnce[A])(implicit evidence: ClassTag[A]): Slice[A] =
      (newBuilder[A] ++= source).result()

    def empty[A](implicit evidence: ClassTag[A]): Slice[A] =
      Slice.of[A](maxSize)

    def newBuilder[A](implicit evidence: ClassTag[A]): mutable.Builder[A, Slice[A]] =
      new SliceBuilder[A](maxSize)
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
                                                                                                              with EvidenceIterableFactoryDefaults[T, Slice, ClassTag] {
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
