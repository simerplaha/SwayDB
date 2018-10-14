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

package swaydb.iterator

import swaydb.data.map.MapKey
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom

case class SubMapKeysIterator[K](mapKey: K,
                                 private val keysIterator: DBKeysIterator[MapKey[K]],
                                 private val till: K => Boolean = (_: K) => true)(implicit keySerializer: Serializer[K],
                                                                                  tableKeySerializer: Serializer[MapKey[K]],
                                                                                  ordering: Ordering[Slice[Byte]]) extends Iterable[K] {

  import ordering._

  private val endKey = MapKey.End(mapKey)

  val thisMapKeyBytes = keySerializer.write(mapKey)

  def from(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.from(MapKey.Entry(mapKey, key)))

  def before(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.before(MapKey.Entry(mapKey, key)))

  def fromOrBefore(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.fromOrBefore(MapKey.Entry(mapKey, key)))

  def after(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.after(MapKey.Entry(mapKey, key)))

  def fromOrAfter(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.fromOrAfter(MapKey.Entry(mapKey, key)))

  private def from(key: MapKey[K], reverse: Boolean): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.copy(reverse = reverse).from(key))

  def till(condition: K => Boolean) =
    copy(till = condition)

  override def iterator: Iterator[K] = {
    val iter = keysIterator.iterator

    var nextKeyValue: Option[K] = None

    new Iterator[K] {

      @tailrec
      override def hasNext: Boolean =
        if (iter.hasNext) {
          val tableId = iter.next()
          val mapKeyBytes = keySerializer.write(tableId.mapKey)
          if (!(mapKeyBytes equiv thisMapKeyBytes)) //it's moved onto another table
            false
          else {
            tableId match {
              //if it's at the head of the table fetch the next key-value
              case MapKey.Start(_) =>
                hasNext

              case MapKey.Entry(_, dataKey) =>
                if (till(dataKey)) {
                  nextKeyValue = Some(dataKey)
                  true
                } else {
                  false
                }

              //if it's at the end of the table fetch the next key-value
              case MapKey.End(_) =>
                hasNext
            }
          }
        } else {
          false
        }

      override def next(): K =
        nextKeyValue.get

      override def toString(): String =
        classOf[SubMapKeysIterator[_]].getClass.getSimpleName
    }
  }

  override def head: K =
    headOption.get

  override def last: K =
    lastOption.get

  override def headOption: Option[K] =
    take(1).headOption

  override def lastOption: Option[K] =
    from(key = endKey, reverse = true).headOption

  def foreachRight[U](f: K => U): Unit =
    from(key = endKey, reverse = true) foreach f

  def mapRight[B, T](f: K => B)(implicit bf: CanBuildFrom[Iterable[K], B, T]): T =
    from(key = endKey, reverse = true) map f

  override def foldRight[B](b: B)(op: (K, B) => B): B =
    from(key = endKey, reverse = true).foldLeft(b) {
      case (prev, key) =>
        op(key, prev)
    }

  override def takeRight(n: Int): Iterable[K] =
    from(key = endKey, reverse = true)

  override def dropRight(n: Int): Iterable[K] =
    from(key = endKey, reverse = true).drop(n)

  override def reduceRight[B >: K](op: (K, B) => B): B =
    from(key = endKey, reverse = true).reduce[B] {
      case (key: K, prev: B) =>
        op(key, prev)
    }

  override def reduceRightOption[B >: K](op: (K, B) => B): Option[B] =
    from(key = endKey, reverse = true).reduceOption[B] {
      case (key: K, prev: B) =>
        op(key, prev)
    }

  override def scanRight[B, That](z: B)(op: (K, B) => B)(implicit bf: CanBuildFrom[Iterable[K], B, That]): That =
    from(key = endKey, reverse = true).scanLeft[B, That](z) {
      case (z: B, k: K) =>
        op(k, z)
    }

  override def toString(): String =
    classOf[SubMapKeysIterator[_]].getClass.getSimpleName
}
