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

protected abstract class ScalaSetBase[A, F](db: Set[A, F, Glass]) extends mutable.Set[A] {

  override def contains(elem: A): Boolean =
    db.contains(elem)

  override def iterator: Iterator[A] =
    db.iterator(Bag.glass)

  override def isEmpty: Boolean =
    db.isEmpty

  override def headOption: Option[A] =
    db.head

  override def lastOption: Option[A] =
    db.last

  override def last: A =
    db.last.get

  override def head: A =
    db.head.get

  override def clear(): Unit =
    db.clear()

}

protected abstract class ScalaSetBaseFromMap[A](db: SetMapT[A, _, Glass]) extends mutable.Set[A] {

  override def contains(elem: A): Boolean =
    db.contains(elem)

  override def iterator: Iterator[A] =
    db.iterator(Bag.glass).map(_._1)

  override def isEmpty: Boolean =
    db.isEmpty

  override def headOption: Option[A] =
    db.head.map(_._1)

  override def lastOption: Option[A] =
    db.last.map(_._1)

  override def last: A =
    lastOption.get

  override def head: A =
    headOption.get

  override def clear(): Unit =
    db.clearKeyValues()

}
