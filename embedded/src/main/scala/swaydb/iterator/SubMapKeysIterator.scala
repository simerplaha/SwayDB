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
import swaydb.order.KeyOrder
import swaydb.serializers.Serializer

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom

/**
  * TODO - [[SubMapIterator]] and [[SubMapKeysIterator]] are similar and need a higher type.
  */
case class SubMapKeysIterator[K](mapKey: Seq[K],
                                 private val includeMapsBoolean: Boolean = false,
                                 private val mapsOnlyBoolean: Boolean = false,
                                 private val keysIterator: DBKeysIterator[Key[K]],
                                 private val till: K => Boolean = (_: K) => true)(implicit keySerializer: Serializer[K],
                                                                                  tableKeySerializer: Serializer[Key[K]],
                                                                                  ordering: Ordering[Slice[Byte]]) extends Iterable[K] {

  private val endEntriesKey = Key.EntriesEnd(mapKey)
  private val endSubMapsKey = Key.SubMapsEnd(mapKey)

  private val thisMapKeyBytes = Key.writeKeys(mapKey, keySerializer)

  def includeMaps(): SubMapKeysIterator[K] =
    copy(includeMapsBoolean = true)

  def mapsOnly(): SubMapKeysIterator[K] =
    copy(mapsOnlyBoolean = true)

  def from(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.from(Key.Entry(mapKey, key)))

  def before(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.before(Key.Entry(mapKey, key)))

  def fromOrBefore(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.fromOrBefore(Key.Entry(mapKey, key)))

  def after(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.after(Key.Entry(mapKey, key)))

  def fromOrAfter(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.fromOrAfter(Key.Entry(mapKey, key)))

  def fromMap(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.from(Key.SubMap(mapKey, key)), includeMapsBoolean = true)

  def beforeMap(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.before(Key.SubMap(mapKey, key)), includeMapsBoolean = true)

  def fromOrBeforeMap(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.fromOrBefore(Key.SubMap(mapKey, key)), includeMapsBoolean = true)

  def afterMap(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.after(Key.SubMap(mapKey, key)), includeMapsBoolean = true)

  def fromOrAfterMap(key: K): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.fromOrAfter(Key.SubMap(mapKey, key)), includeMapsBoolean = true)

  private def before(key: Key[K], reverse: Boolean): SubMapKeysIterator[K] =
    copy(keysIterator = keysIterator.before(key).copy(reverse = reverse))

  def till(condition: K => Boolean) =
    copy(till = condition)

  override def iterator: Iterator[K] = {
    val iter = keysIterator.iterator

    var nextKeyValue: Option[K] = None

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

    new Iterator[K] {

      @tailrec
      override def hasNext: Boolean =
        if (iter.hasNext) {
          val mapKey = iter.next()
          val mapKeyBytes = Key.writeKeys(mapKey.parentMapKeys, keySerializer)
          if (KeyOrder.default.compare(mapKeyBytes, thisMapKeyBytes) != 0) //Exit if it's moved onto another map
            false
          else {
            mapKey match {
              case Key.Start(_) =>
                if (keysIterator.reverse) //exit iteration if it's going backward since Start is the head key
                  false
                else
                  hasNext

              case Key.EntriesStart(_) =>
                if (keysIterator.reverse) //exit iteration if it's going backward as previous entry is pointer entry Start
                  false
                else
                  hasNext

              case Key.Entry(_, dataKey) =>
                if (mapsOnlyBoolean) {
                  if (keysIterator.reverse) //Exit if it's fetching subMaps only it's already reached an entry means it's has already crossed subMaps block
                    false
                  else
                    hasNext
                } else {
                  if (till(dataKey)) {
                    nextKeyValue = Some(dataKey)
                    true
                  } else {
                    false
                  }
                }

              case Key.EntriesEnd(_) =>
                //if it's not going backwards and it's trying to fetch subMaps only then move forward
                if (!keysIterator.reverse && !includeMapsBoolean && !mapsOnlyBoolean)
                  false
                else
                  hasNext

              case Key.SubMapsStart(_) =>
                //if it's not going backward and it's trying to fetch subMaps only then move forward
                if (!keysIterator.reverse && !includeMapsBoolean && !mapsOnlyBoolean)
                  false
                else
                  hasNext

              case Key.SubMap(_, dataKey) =>
                //Exit if it's going forward and does not have subMaps included in the iteration.
                if (!keysIterator.reverse && !mapsOnlyBoolean && !includeMapsBoolean)
                  false
                else if (till(dataKey)) {
                  nextKeyValue = Some(dataKey)
                  true
                } else {
                  false
                }

              case Key.SubMapsEnd(_) =>
                //Exit if it's not going forward.
                if (!keysIterator.reverse)
                  false
                else
                  hasNext

              case Key.End(_) =>
                //Exit if it's not going in reverse.
                if (!keysIterator.reverse)
                  false
                else
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

  /**
    * Returns the start key when doing reverse iteration.
    *
    * If subMaps are included then it will return the starting point to be [[Key.SubMapsEnd]]
    * which will iterate backward until [[Key.EntriesStart]]
    * else returns the starting point to be [[Key.EntriesEnd]] to fetch entries only.
    */
  private def reverseIterationStartingKey(): Key[K] =
    if (includeMapsBoolean || mapsOnlyBoolean)
      endSubMapsKey
    else
      endEntriesKey

  override def lastOption: Option[K] =
    before(key = reverseIterationStartingKey(), reverse = true).headOption

  def foreachRight[U](f: K => U): Unit =
    before(key = reverseIterationStartingKey(), reverse = true) foreach f

  def mapRight[B, T](f: K => B)(implicit bf: CanBuildFrom[Iterable[K], B, T]): T =
    before(key = reverseIterationStartingKey(), reverse = true) map f

  override def foldRight[B](b: B)(op: (K, B) => B): B =
    before(key = reverseIterationStartingKey(), reverse = true).foldLeft(b) {
      case (prev, key) =>
        op(key, prev)
    }

  override def takeRight(n: Int): Iterable[K] =
    before(key = reverseIterationStartingKey(), reverse = true).take(n)

  override def dropRight(n: Int): Iterable[K] =
    before(key = reverseIterationStartingKey(), reverse = true).drop(n)

  override def reduceRight[B >: K](op: (K, B) => B): B =
    before(key = reverseIterationStartingKey(), reverse = true).reduce[B] {
      case (left, right: K) =>
        op(right, left)
    }

  override def reduceRightOption[B >: K](op: (K, B) => B): Option[B] =
    before(key = reverseIterationStartingKey(), reverse = true).reduceOption[B] {
      case (left, right: K) =>
        op(right, left)
    }

  override def scanRight[B, That](z: B)(op: (K, B) => B)(implicit bf: CanBuildFrom[Iterable[K], B, That]): That =
    before(key = reverseIterationStartingKey(), reverse = true).scan(z) {
      case (left: B, right: K) =>
        op(right, left)
    }.asInstanceOf[That]

  override def toString(): String =
    classOf[SubMapKeysIterator[_]].getClass.getSimpleName
}
