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

package swaydb.extension.iterator

import swaydb.extension.Key
import swaydb.iterator.DBKeysIterator
import swaydb.data.order.KeyOrder
import swaydb.serializers.Serializer

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom

/**
  * TODO - [[MapIterator]] and [[MapKeysIterator]] are similar and need a higher type.
  *
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
  **/
case class MapKeysIterator[K](mapKey: Seq[K],
                              mapsOnly: Boolean = false,
                              userDefinedFrom: Boolean = false,
                              keysIterator: DBKeysIterator[Key[K]],
                              till: K => Boolean = (_: K) => true)(implicit keySerializer: Serializer[K],
                                                                   mapKeySerializer: Serializer[Key[K]]) extends Iterable[K] {

  private val endEntriesKey = Key.MapEntriesEnd(mapKey)
  private val endSubMapsKey = Key.SubMapsEnd(mapKey)

  private val thisMapKeyBytes = Key.writeKeys(mapKey, keySerializer)

  def from(key: K): MapKeysIterator[K] =
    if (mapsOnly)
      copy(keysIterator = keysIterator.from(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(keysIterator = keysIterator.from(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def before(key: K): MapKeysIterator[K] =
    if (mapsOnly)
      copy(keysIterator = keysIterator.before(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(keysIterator = keysIterator.before(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def fromOrBefore(key: K): MapKeysIterator[K] =
    if (mapsOnly)
      copy(keysIterator = keysIterator.fromOrBefore(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(keysIterator = keysIterator.fromOrBefore(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def after(key: K): MapKeysIterator[K] =
    if (mapsOnly)
      copy(keysIterator = keysIterator.after(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(keysIterator = keysIterator.after(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def fromOrAfter(key: K): MapKeysIterator[K] =
    if (mapsOnly)
      copy(keysIterator = keysIterator.fromOrAfter(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(keysIterator = keysIterator.fromOrAfter(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  private def before(key: Key[K], reverse: Boolean): MapKeysIterator[K] =
    copy(keysIterator = keysIterator.before(key).copy(reverse = reverse))

  private def reverse(reverse: Boolean): MapKeysIterator[K] =
    copy(keysIterator = keysIterator.copy(reverse = reverse))

  def till(condition: K => Boolean) =
    copy(till = condition)

  override def iterator: Iterator[K] = {
    new Iterator[K] {

      val iter = keysIterator.iterator

      var nextKeyValue: Option[K] = null

      @tailrec
      override def hasNext: Boolean =
        if (iter.hasNext) {
          val mapKey = iter.next()
          val mapKeyBytes = Key.writeKeys(mapKey.parentMapKeys, keySerializer)
          if (KeyOrder.default.compare(mapKeyBytes, thisMapKeyBytes) != 0) //Exit if it's moved onto another map
            false
          else {
            mapKey match {
              case Key.MapStart(_) =>
                if (keysIterator.reverse) //exit iteration if it's going backward since Start is the head key
                  false
                else
                  hasNext

              case Key.MapEntriesStart(_) =>
                if (keysIterator.reverse) //exit iteration if it's going backward as previous entry is pointer entry Start
                  false
                else
                  hasNext

              case Key.MapEntry(_, dataKey) =>
                if (mapsOnly) {
                  if (keysIterator.reverse) //Exit if it's fetching subMaps only it's already reached an entry means it's has already crossed subMaps block
                    false
                  else
                    hasNext
                } else if (till(dataKey)) {
                  nextKeyValue = Some(dataKey)
                  true
                } else {
                  false
                }

              case Key.MapEntriesEnd(_) =>
                //if it's not going backwards and it's trying to fetch subMaps only then move forward
                if (!keysIterator.reverse && !mapsOnly)
                  false
                else
                  hasNext

              case Key.SubMapsStart(_) =>
                //if it's not going backward and it's trying to fetch subMaps only then move forward
                if (!keysIterator.reverse && !mapsOnly)
                  false
                else
                  hasNext

              case Key.SubMap(_, dataKey) =>
                if (!mapsOnly) //if subMaps are excluded
                  if (keysIterator.reverse) //if subMaps are excluded & it's going in reverse continue iteration.
                    hasNext
                  else //if it's going forward with subMaps excluded then end iteration as it's already iterated all key-value entries.
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

              case Key.MapEnd(_) =>
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
        classOf[MapKeysIterator[_]].getClass.getSimpleName
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
    * which will iterate backward until [[Key.MapEntriesStart]]
    * else returns the starting point to be [[Key.MapEntriesEnd]] to fetch entries only.
    */
  private def reverseIterator(): MapKeysIterator[K] =
    if (userDefinedFrom) //if user has defined from then do not override it and just set reverse to true.
      reverse(reverse = true)
    else if (mapsOnly) //if from is not already set & map are included in the iteration then start from subMap's last key
      before(key = endSubMapsKey, reverse = true)
    else //if subMaps are excluded, start from key's last key.
      before(key = endEntriesKey, reverse = true)

  /**
    * lastOption should always force formKey to be the [[endSubMapsKey]]
    * because from is always set in [[swaydb.extension.Maps]] and regardless from where the iteration starts the
    * most efficient way to fetch the last is from the key [[endSubMapsKey]].
    */
  override def lastOption: Option[K] =
    reverseIterator().headOption

  def foreachRight[U](f: K => U): Unit =
    reverseIterator() foreach f

  def mapRight[B, T](f: K => B)(implicit bf: CanBuildFrom[Iterable[K], B, T]): T =
    reverseIterator() map f

  override def foldRight[B](b: B)(op: (K, B) => B): B =
    reverseIterator().foldLeft(b) {
      case (prev, key) =>
        op(key, prev)
    }

  override def takeRight(n: Int): Iterable[K] =
    reverseIterator().take(n)

  override def dropRight(n: Int): Iterable[K] =
    reverseIterator().drop(n)

  override def reduceRight[B >: K](op: (K, B) => B): B =
    reverseIterator().reduce[B] {
      case (left, right: K) =>
        op(right, left)
    }

  override def reduceRightOption[B >: K](op: (K, B) => B): Option[B] =
    reverseIterator().reduceOption[B] {
      case (left, right: K) =>
        op(right, left)
    }

  override def scanRight[B, That](z: B)(op: (K, B) => B)(implicit bf: CanBuildFrom[Iterable[K], B, That]): That =
    reverseIterator().scan(z) {
      case (left: B, right: K) =>
        op(right, left)
    }.asInstanceOf[That]

  override def toString(): String =
    classOf[MapKeysIterator[_]].getClass.getSimpleName
}
