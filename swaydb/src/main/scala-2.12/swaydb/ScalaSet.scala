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

package swaydb

import scala.collection.compat.IterableOnce
import scala.collection.mutable

/**
 * Scala collections are blocking and requires an IO Set from SwayDB to build a Set.
 */
private[swaydb] object ScalaSet {

  def apply[A, F](db: Set[A, F, Glass]): mutable.Set[A] =
    new ScalaSetBase[A, F](db) {

      override def +=(elem: A): this.type = {
        db.add(elem)
        this
      }

      override def -=(elem: A): this.type = {
        db.remove(elem)
        this
      }

      override def --=(xs: IterableOnce[A]): this.type = {
        db.remove(xs.toIterable)
        this
      }

      override def ++=(xs: IterableOnce[A]): this.type = {
        db.add(xs.toIterable)
        this
      }
    }

  def apply[A, V](db: SetMapT[A, V, Glass], nullValue: V): mutable.Set[A] =
    new ScalaSetBaseFromMap[A](db) {
      override def +=(elem: A): this.type = {
        db.put(elem, nullValue)
        this
      }

      override def -=(elem: A): this.type = {
        db.remove(elem)
        this
      }

      override def --=(xs: IterableOnce[A]): this.type = {
        db.remove(xs.toIterable)
        this
      }

      override def ++=(xs: IterableOnce[A]): this.type = {
        db.put(xs.toIterable.map((_, nullValue)))
        this
      }
    }
}
