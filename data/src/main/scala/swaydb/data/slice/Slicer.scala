/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.collection.{IterableLike, mutable}

object Slicer {

  implicit class SlicerImplicitClassTag[T](left: Slicer[T]) {
    def join(right: Slicer[T]): Slicer[T] =
      Slices(left, right)
  }

  class SliceBuilder[T](sizeHint: Int) extends mutable.Builder[T, Slice[T]] {
    //max is used to in-case sizeHit == 0 which is possible for cases where (None ++ Some(Slice[T](...)))
    protected var slice: Slice[T] = Slice.create[Nothing](((sizeHint max 16) * 2.5).toInt).asInstanceOf[Slice[T]]

    def extendSlice(by: Int) = {
      val extendedSlice = Slice.create[Nothing](slice.size * by).asInstanceOf[Slice[T]]
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
      slice = Slice.empty.asInstanceOf[Slice[T]]

    def result: Slice[T] =
      slice.close()
  }

  implicit def canBuildFrom[T]: CanBuildFrom[Slicer[_], T, Slicer[T]] =
    new CanBuildFrom[Slicer[_], T, Slicer[T]] {
      def apply(from: Slicer[_]) =
        new SliceBuilder[T](from.size max 16) //max is used in-case from.size == 0

      def apply() = new SliceBuilder[T](100)
    }
}

trait Slicer[+T] extends Iterable[T] with IterableLike[T, Slicer[T]] {

  override protected[this] def newBuilder: scala.collection.mutable.Builder[T, Slicer[T]] =
    new Slicer.SliceBuilder[T](100)

  def unslice(): Slicer[T]

  def isFull: Boolean
}
