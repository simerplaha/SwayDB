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

import scala.collection.compat.IterableOnce
import scala.collection.{ClassTagIterableFactory, mutable}
import scala.reflect.ClassTag

class SliceFactory(maxSize: Int) extends ClassTagIterableFactory[Slice] {

  override def from[A](source: IterableOnce[A])(implicit evidence: ClassTag[A]): Slice[A] =
    (newBuilder[A] ++= source).result()

  def empty[A](implicit evidence: ClassTag[A]): Slice[A] =
    Slice.of[A](maxSize)

  def newBuilder[A](implicit evidence: ClassTag[A]): mutable.Builder[A, Slice[A]] =
    new SliceBuilder[A](maxSize)
}
