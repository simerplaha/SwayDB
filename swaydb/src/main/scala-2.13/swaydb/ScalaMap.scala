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
 * Scala collections are blocking and requires an IO Map from SwayDB to build a Map.
 */
private[swaydb] object ScalaMap {

  def apply[K, V](db: SetMapT[K, V, Glass]): mutable.Map[K, V] =
    new ScalaMapBase[K, V](db) {

      override def addOne(kv: (K, V)): this.type = {
        db.put(kv._1, kv._2)
        this
      }

      override def subtractOne(key: K): this.type = {
        db.remove(key)
        this
      }

      override def subtractAll(xs: IterableOnce[K]): this.type = {
        db.remove(xs.iterator)
        this
      }

      override def addAll(xs: IterableOnce[(K, V)]): this.type = {
        db.put(xs.iterator)
        this
      }
    }
}
