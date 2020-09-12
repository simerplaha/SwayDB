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

import swaydb.data.slice.Slice.Slice
import swaydb.data.util.SomeOrNoneCovariant

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

object Slice extends SliceCompanionBase {

  final case object Null extends SliceOption[Nothing] {
    override val isNoneC: Boolean = true
    override def getC: Slice[Nothing] = throw new Exception("Slice is of type Null")
    override def isUnslicedOption: Boolean = true
    override def asSliceOption(): SliceOption[Nothing] = this
  }

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
}
