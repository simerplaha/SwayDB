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

import scala.collection.mutable

protected abstract class ScalaMapBase[K, V](db: SetMapT[K, V, Glass]) extends mutable.Map[K, V] {

  override def get(key: K): Option[V] =
    db.get(key)

  override def iterator: Iterator[(K, V)] =
    db.iterator(Bag.glass)

  override def isEmpty: Boolean =
    db.isEmpty

  override def headOption: Option[(K, V)] =
    db.head

  override def lastOption: Option[(K, V)] =
    db.last

  override def keySet: mutable.Set[K] =
    db.keySet

  override def contains(key: K): Boolean =
    db.contains(key)

  override def last: (K, V) =
    db.last.get

  override def head: (K, V) =
    db.head.get

  override def clear(): Unit =
    db.clearKeyValues()

}
