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

import swaydb.data.slice.Slice
import swaydb.extension.Key
import swaydb.iterator.DBIterator
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

case class MapIterator[K, V](mapKey: Seq[K],
                             mapsOnly: Boolean = false,
                             userDefinedFrom: Boolean = false,
                             dbIterator: DBIterator[Key[K], Option[V]],
                             till: (K, V) => Boolean = (_: K, _: V) => true)(implicit keySerializer: Serializer[K],
                                                                                         mapKeySerializer: Serializer[Key[K]],
                                                                                         optionValueSerializer: Serializer[Option[V]]) extends Iterable[(K, V)] {

  private val endEntriesKey = Key.MapEntriesEnd(mapKey)
  private val endSubMapsKey = Key.SubMapsEnd(mapKey)

  private val thisMapKeyBytes = Key.writeKeys(mapKey, keySerializer)

  def from(key: K): MapIterator[K, V] =
    if (mapsOnly)
      copy(dbIterator = dbIterator.from(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(dbIterator = dbIterator.from(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def before(key: K): MapIterator[K, V] =
    if (mapsOnly)
      copy(dbIterator = dbIterator.before(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(dbIterator = dbIterator.before(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def fromOrBefore(key: K): MapIterator[K, V] =
    if (mapsOnly)
      copy(dbIterator = dbIterator.fromOrBefore(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(dbIterator = dbIterator.fromOrBefore(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def after(key: K): MapIterator[K, V] =
    if (mapsOnly)
      copy(dbIterator = dbIterator.after(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(dbIterator = dbIterator.after(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  def fromOrAfter(key: K): MapIterator[K, V] =
    if (mapsOnly)
      copy(dbIterator = dbIterator.fromOrAfter(Key.SubMap(mapKey, key)), mapsOnly = true, userDefinedFrom = true)
    else
      copy(dbIterator = dbIterator.fromOrAfter(Key.MapEntry(mapKey, key)), userDefinedFrom = true)

  private def before(key: Key[K], reverse: Boolean): MapIterator[K, V] =
    copy(dbIterator = dbIterator.before(key).copy(reverse = reverse))

  private def reverse(reverse: Boolean): MapIterator[K, V] =
    copy(dbIterator = dbIterator.copy(reverse = reverse))

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

    new Iterator[(K, V)] {

      val iter = dbIterator.iterator

      var nextKeyValue: (K, V) = null

      @tailrec
      override def hasNext: Boolean =
        if (iter.hasNext) {
          val (mapKey, valueOption) = iter.next()
          //type casting here because a value set by the User will/should always have a serializer. If the inner value if
          //Option[V] then the full value would be Option[Option[V]] and the serializer is expected to handle serialization for
          //the inner Option[V] which would be of the User's type V.
          val value = valueOption.getOrElse(optionValueSerializer.read(Slice.emptyBytes).asInstanceOf[V])
          val mapKeyBytes = Key.writeKeys(mapKey.parentMapKeys, keySerializer)
          if (KeyOrder.default.compare(mapKeyBytes, thisMapKeyBytes) != 0) //Exit if it's moved onto another map
            false
          else {
            mapKey match {
              case Key.MapStart(_) =>
                if (dbIterator.reverse) //exit iteration if it's going backward since Start is the head key
                  false
                else
                  hasNext

              case Key.MapEntriesStart(_) =>
                if (dbIterator.reverse) //exit iteration if it's going backward as previous entry is pointer entry Start
                  false
                else
                  hasNext

              case Key.MapEntry(_, dataKey) =>
                if (mapsOnly) {
                  if (dbIterator.reverse) //Exit if it's fetching subMaps only it's already reached an entry means it's has already crossed subMaps block
                    false
                  else
                    hasNext
                } else {
                  if (till(dataKey, value)) {
                    nextKeyValue = (dataKey, value)
                    true
                  } else {
                    false
                  }
                }

              case Key.MapEntriesEnd(_) =>
                //if it's not going backwards and it's trying to fetch subMaps only then move forward
                if (!dbIterator.reverse && !mapsOnly)
                  false
                else
                  hasNext

              case Key.SubMapsStart(_) =>
                //if it's not going backward and it's trying to fetch subMaps only then move forward
                if (!dbIterator.reverse && !mapsOnly)
                  false
                else
                  hasNext

              case Key.SubMap(_, dataKey) =>
                if (!mapsOnly) //if subMaps are excluded
                  if (dbIterator.reverse) //if subMaps are excluded & it's going in reverse continue iteration.
                    hasNext
                  else //if it's going forward with subMaps excluded then end iteration as it's already iterated all key-value entries.
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

              case Key.MapEnd(_) =>
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
        classOf[MapIterator[_, _]].getClass.getSimpleName
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
    * which will iterate backward until [[Key.MapEntriesStart]]
    * else returns the starting point to be [[Key.MapEntriesEnd]] to fetch entries only.
    */
  private def reverseIterator(): MapIterator[K, V] =
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
  override def lastOption: Option[(K, V)] =
    reverseIterator().headOption

  def foreachRight[U](f: (K, V) => U): Unit =
    reverseIterator() foreach {
      case (key, value) =>
        f(key, value)
    }

  def mapRight[B, T](f: (K, V) => B)(implicit bf: CanBuildFrom[Iterable[(K, V)], B, T]): T =
    reverseIterator() map {
      case (key, value) =>
        f(key, value)
    }

  override def foldRight[B](b: B)(op: ((K, V), B) => B): B =
    reverseIterator().foldLeft(b) {
      case (prev, (key, value)) =>
        op((key, value), prev)
    }

  override def takeRight(n: Int): Iterable[(K, V)] =
    reverseIterator().take(n)

  override def dropRight(n: Int): Iterable[(K, V)] =
    reverseIterator().drop(n)

  override def reduceRight[B >: (K, V)](op: ((K, V), B) => B): B =
    reverseIterator().reduce[B] {
      case (left, right: (K, V)) =>
        op(right, left)
    }

  override def reduceRightOption[B >: (K, V)](op: ((K, V), B) => B): Option[B] =
    reverseIterator().reduceOption[B] {
      case (left, right: (K, V)) =>
        op(right, left)
    }

  override def scanRight[B, That](z: B)(op: ((K, V), B) => B)(implicit bf: CanBuildFrom[Iterable[(K, V)], B, That]): That =
    reverseIterator().scan(z) {
      case (left: B, right: (K, V)) =>
        op(right, left)
    }.asInstanceOf[That]

  override def toString(): String =
    classOf[MapIterator[_, _]].getClass.getSimpleName
}
