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

import swaydb.SwayDB
import swaydb.data.slice.Slice
import swaydb.data.map.MapKey
import swaydb.serializers.Serializer

import scala.annotation.tailrec

case class SubMapIterator[K, V](db: SwayDB,
                                tableKey: K,
                                private val dbIterator: DBIterator[MapKey[K], V],
                                private val till: (K, V) => Boolean = (_: K, _: V) => true)(implicit keySerializer: Serializer[K],
                                                                                            tableKeySerializer: Serializer[MapKey[K]],
                                                                                            ordering: Ordering[Slice[Byte]],
                                                                                            valueSerializer: Serializer[V]) extends Iterable[(K, V)] {

  import ordering._

  val tableKeyBytes = keySerializer.write(tableKey)

  def from(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.from(MapKey.Row(tableKey, key)))

  def before(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.before(MapKey.Row(tableKey, key)))

  def fromOrBefore(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.fromOrBefore(MapKey.Row(tableKey, key)))

  def after(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.after(MapKey.Row(tableKey, key)))

  def fromOrAfter(key: K): SubMapIterator[K, V] =
    copy(dbIterator = dbIterator.fromOrAfter(MapKey.Row(tableKey, key)))

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
          val thisTableKeyBytes = keySerializer.write(tableId.mapKey)
          if (!(thisTableKeyBytes equiv tableKeyBytes)) //it's moved onto another table
            false
          else {
            tableId match {
              //if it's at the head of the table fetch the next key-value
              case MapKey.Start(_) =>
                hasNext

              case MapKey.Row(_, dataKey) =>
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
    }
  }
}
