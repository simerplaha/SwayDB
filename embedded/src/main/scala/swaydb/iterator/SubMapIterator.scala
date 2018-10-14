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

case class SubMapIterator[K, V](mapKey: K,
                                private val dbIterator: DBIterator[MapKey[K], V],
                                private val till: (K, V) => Boolean = (_: K, _: V) => true)(implicit keySerializer: Serializer[K],
                                                                                            tableKeySerializer: Serializer[MapKey[K]],
                                                                                            ordering: Ordering[Slice[Byte]],
                                                                                            valueSerializer: Serializer[V]) extends Iterable[(K, V)] {

  import ordering._

  private val endKey = MapKey.End(mapKey)

  val thisMapKeyBytes = keySerializer.write(mapKey)

  def from(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.from(MapKey.Entry(mapKey, key)))

  def before(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.before(MapKey.Entry(mapKey, key)))

  def fromOrBefore(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.fromOrBefore(MapKey.Entry(mapKey, key)))

  def after(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.after(MapKey.Entry(mapKey, key)))

  def fromOrAfter(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.fromOrAfter(MapKey.Entry(mapKey, key)))

  private def from(key: MapKey[K], reverse: Boolean): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.copy(reverse = reverse).from(key))

  def till(condition: (K, V) => Boolean) =
    copy(till = condition)

  def tillKey(condition: K => Boolean) =
    copy(
      till =
        (key: K, _: V) =>
          condition(key)
    )

  def tillValue(condition: V => Boolean) =
    copy(
      till =
        (_: K, value: V) =>
          condition(value)
    )

  override def iterator: Iterator[(K, V)] = {
    val iter = dbIterator.iterator

    var nextKeyValue: (K, V) = null

    new Iterator[(K, V)] {

      @tailrec
      override def hasNext: Boolean =
        if (iter.hasNext) {
          val (tableId, value) = iter.next()
          val mapKeyBytes = keySerializer.write(tableId.mapKey)
          if (!(mapKeyBytes equiv thisMapKeyBytes)) //it's moved onto another table
            false
          else {
            tableId match {
              //if it's at the head of the table fetch the next key-value
              case MapKey.Start(_) =>
                hasNext

              case MapKey.Entry(_, dataKey) =>
                if (till(dataKey, value)) {
                  nextKeyValue = (dataKey, value)
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

      override def next(): (K, V) =
        nextKeyValue

      override def toString(): String =
        classOf[SubMapIterator[_, _]].getClass.getSimpleName
    }
  }

  override def size: Int =
    SubMapKeysIterator(mapKey, DBKeysIterator(dbIterator.db, dbIterator.from)).size

  //
  //  override def isEmpty: Boolean =
  //    dbIterator.isEmpty
  //
  //  override def nonEmpty: Boolean =
  //    !isEmpty

  override def head: (K, V) =
    headOption.get

  override def last: (K, V) =
    lastOption.get

  override def headOption: Option[(K, V)] =
    take(1).headOption

  override def lastOption: Option[(K, V)] =
    from(key = endKey, reverse = true).headOption

  def foreachRight[U](f: (K, V) => U): Unit =
    from(key = endKey, reverse = true) foreach {
      case (key, value) =>
        f(key, value)
    }

  def mapRight[B, T](f: (K, V) => B)(implicit bf: CanBuildFrom[Iterable[(K, V)], B, T]): T =
    from(key = endKey, reverse = true) map {
      case (key, value) =>
        f(key, value)
    }

  override def foldRight[B](b: B)(op: ((K, V), B) => B): B =
    from(key = endKey, reverse = true).foldLeft(b) {
      case (prev, (key, value)) =>
        op((key, value), prev)
    }

  override def takeRight(n: Int): Iterable[(K, V)] =
    from(key = endKey, reverse = true) map {
      case (key, value) =>
        (key, value)
    }

  override def dropRight(n: Int): Iterable[(K, V)] =
    from(key = endKey, reverse = true).drop(n) map {
      case (key, value) =>
        (key, value)
    }

  override def reduceRight[B >: (K, V)](op: ((K, V), B) => B): B =
    from(key = endKey, reverse = true).reduce[B] {
      case ((key: K, value: V), prev) =>
        op((key, value), prev)
    }

  override def reduceRightOption[B >: (K, V)](op: ((K, V), B) => B): Option[B] =
    from(key = endKey, reverse = true).reduceOption[B] {
      case ((key: K, value: V), prev: B) =>
        op((key, value), prev)
    }

  override def scanRight[B, That](z: B)(op: ((K, V), B) => B)(implicit bf: CanBuildFrom[Iterable[(K, V)], B, That]): That =
    from(key = endKey, reverse = true).scanLeft(z) {
      case (k, prev) =>
        op(prev, k)
    }

  override def toString(): String =
    classOf[SubMapIterator[_, _]].getClass.getSimpleName
}
