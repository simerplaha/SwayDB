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

import swaydb.data.slice.Slice
import swaydb.extension.Key
import swaydb.serializers.Serializer

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom

case class SubMapIterator[K, V](mapKey: Seq[K],
                                private val includeSubMapsBoolean: Boolean = false,
                                private val subMapsOnlyBoolean: Boolean = false,
                                private val dbIterator: DBIterator[Key[K], V],
                                private val till: (K, V) => Boolean = (_: K, _: V) => true)(implicit keySerializer: Serializer[K],
                                                                                            tableKeySerializer: Serializer[Key[K]],
                                                                                            ordering: Ordering[Slice[Byte]],
                                                                                            valueSerializer: Serializer[V]) extends Iterable[(K, V)] {

  import ordering._

  private val endEntriesKey = Key.EntriesEnd(mapKey)
  private val endSubMapsKey = Key.SubMapsEnd(mapKey)

  private val thisMapKeyBytes = Key.writeKeys(mapKey, keySerializer)

  def includeSubMaps(): SubMapIterator[K, V] =
    copy(includeSubMapsBoolean = true)

  def subMapsOnly(): SubMapIterator[K, V] =
    copy(subMapsOnlyBoolean = true)

  def from(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.from(Key.Entry(mapKey, key)))

  def before(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.before(Key.Entry(mapKey, key)))

  def fromOrBefore(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.fromOrBefore(Key.Entry(mapKey, key)))

  def after(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.after(Key.Entry(mapKey, key)))

  def fromOrAfter(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.fromOrAfter(Key.Entry(mapKey, key)))

  def fromSubMap(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.from(Key.SubMap(mapKey, key)), includeSubMapsBoolean = true)

  def beforeSubMap(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.before(Key.SubMap(mapKey, key)), includeSubMapsBoolean = true)

  def fromOrBeforeSubMap(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.fromOrBefore(Key.SubMap(mapKey, key)), includeSubMapsBoolean = true)

  def afterSubMap(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.after(Key.SubMap(mapKey, key)), includeSubMapsBoolean = true)

  def fromOrAfterSubMap(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.fromOrAfter(Key.SubMap(mapKey, key)), includeSubMapsBoolean = true)

  private def before(key: Key[K], reverse: Boolean): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.before(key).copy(reverse = reverse))

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

    /**
      * Sample order
      *
      * MapKey.Start(1),
      *   MapKey.EntriesStart(1)
      *     MapKey.Entry(1, 1)
      *   MapKey.EntriesEnd(1)
      *   MapKey.SubMapsStart(1)
      *     MapKey.SubMap(1, 1000)
      *   MapKey.SubMapsEnd(1)
      * MapKey.End(1)
      */

    new Iterator[(K, V)] {

      @tailrec
      override def hasNext: Boolean =
        if (iter.hasNext) {
          val (mapKey, value) = iter.next()
          val mapKeyBytes = Key.writeKeys(mapKey.parentMapKeys, keySerializer)
          if (!(mapKeyBytes equiv thisMapKeyBytes)) //Exit because it's moved onto another map
            false
          else {
            mapKey match {
              case Key.Start(_) =>
                if (dbIterator.reverse) //exit iteration if it's going backward since Start is the head key
                  false
                else
                  hasNext

              case Key.EntriesStart(_) =>
                if (dbIterator.reverse) //exit iteration if it's going backward as previous entry is pointer entry Start
                  false
                else
                  hasNext

              case Key.Entry(_, dataKey) =>
                if (subMapsOnlyBoolean)
                  if (dbIterator.reverse) //Exit if it's fetching subMaps only it's already reached an entry means it's has already crossed subMaps block
                    false
                  else
                    hasNext
                else if (till(dataKey, value)) {
                  nextKeyValue = (dataKey, value)
                  true
                } else {
                  false
                }

              case Key.EntriesEnd(_) =>
                //if it's not going backwards and it's trying to fetch subMaps only then move forward
                if (!dbIterator.reverse && !includeSubMapsBoolean && !subMapsOnlyBoolean)
                  false
                else
                  hasNext

              case Key.SubMapsStart(_) =>
                //if it's not going backward and it's trying to fetch subMaps only then move forward
                if (!dbIterator.reverse && !includeSubMapsBoolean && !subMapsOnlyBoolean)
                  false
                else
                  hasNext

              case Key.SubMap(_, dataKey) =>
                //Exit if it's going forward and does not have subMaps included in the iteration.
                if (!dbIterator.reverse && !subMapsOnlyBoolean && !includeSubMapsBoolean)
                  false
                else if (till(dataKey, value)) {
                  nextKeyValue = (dataKey, value)
                  true
                } else {
                  false
                }

              case Key.SubMapsEnd(_) =>
                //Exit if it's not going forward.
                if (!dbIterator.reverse)
                  false
                else
                  hasNext

              case Key.End(_) =>
                //Exit if it's not going in reverse.
                if (!dbIterator.reverse)
                  false
                else
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

  override def head: (K, V) =
    headOption.get

  override def last: (K, V) =
    lastOption.get

  override def headOption: Option[(K, V)] =
    take(1).headOption

  /**
    * Returns the start key when doing reverse iteration.
    *
    * If subMaps are included then it will return the starting point to be [[Key.SubMapsEnd]]
    * which will iterate backward until [[Key.EntriesStart]]
    * else returns the starting point to be [[Key.EntriesEnd]] to fetch entries only.
    */
  private def reverseIterationStartingKey(): Key[K] =
    if (includeSubMapsBoolean || subMapsOnlyBoolean)
      endSubMapsKey
    else
      endEntriesKey

  override def lastOption: Option[(K, V)] =
    before(key = reverseIterationStartingKey(), reverse = true).headOption

  def foreachRight[U](f: (K, V) => U): Unit =
    before(key = reverseIterationStartingKey(), reverse = true) foreach {
      case (key, value) =>
        f(key, value)
    }

  def mapRight[B, T](f: (K, V) => B)(implicit bf: CanBuildFrom[Iterable[(K, V)], B, T]): T =
    before(key = reverseIterationStartingKey(), reverse = true) map {
      case (key, value) =>
        f(key, value)
    }

  override def foldRight[B](b: B)(op: ((K, V), B) => B): B =
    before(key = reverseIterationStartingKey(), reverse = true).foldLeft(b) {
      case (prev, (key, value)) =>
        op((key, value), prev)
    }

  override def takeRight(n: Int): Iterable[(K, V)] =
    before(key = reverseIterationStartingKey(), reverse = true).take(n)

  override def dropRight(n: Int): Iterable[(K, V)] =
    before(key = reverseIterationStartingKey(), reverse = true).drop(n)

  override def reduceRight[B >: (K, V)](op: ((K, V), B) => B): B =
    before(key = reverseIterationStartingKey(), reverse = true).reduce[B] {
      case (left: B, right: (K, V)) =>
        op(right, left)
    }

  override def reduceRightOption[B >: (K, V)](op: ((K, V), B) => B): Option[B] =
    before(key = reverseIterationStartingKey(), reverse = true).reduceOption[B] {
      case (left: B, right: (K, V)) =>
        op(right, left)
    }

  override def scanRight[B, That](z: B)(op: ((K, V), B) => B)(implicit bf: CanBuildFrom[Iterable[(K, V)], B, That]): That =
    before(key = reverseIterationStartingKey(), reverse = true).scan(z) {
      case (left: B, right: (K, V)) =>
        op(right, left)
    }.asInstanceOf[That]

  override def toString(): String =
    classOf[SubMapIterator[_, _]].getClass.getSimpleName
}
