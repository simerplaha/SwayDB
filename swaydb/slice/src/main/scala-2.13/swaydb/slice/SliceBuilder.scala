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

import swaydb.utils.Aggregator

import scala.annotation.tailrec
import scala.collection._
import scala.collection.compat.IterableOnce
import scala.reflect.ClassTag

class SliceBuilder[A: ClassTag](maxSize: Int) extends mutable.Builder[A, Slice[A]] with Aggregator[A, Slice[A]] {
  //max is used to in-case sizeHit == 0 which is possible for cases where (None ++ Some(Slice[T](...)))
  protected var slice: SliceMut[A] = Slice.allocate[A](maxSize max 16)

  @inline def extendSlice(by: Int) = {
    val extendedSlice = Slice.allocate[A](slice.size * by)
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

  def clear(): Unit =
    slice = Slice.allocate[A](slice.size)

  def result(): Slice[A] =
    slice.close()

}
