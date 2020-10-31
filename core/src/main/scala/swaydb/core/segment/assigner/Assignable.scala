/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.assigner

import swaydb.Aggregator
import swaydb.core.data.{KeyValue, Memory}
import swaydb.core.level.zero.LevelZeroMapCache
import swaydb.core.map.Map
import swaydb.data.MaxKey
import swaydb.data.slice.Slice

import scala.collection.mutable.ListBuffer

/**
 * Something that can be assigned to a Segment for merge.
 *
 * Current types
 * - [[swaydb.core.segment.Segment]]
 * - [[Map[Slice[Byte], Memory, LevelZeroMapCache]]
 * - [[swaydb.core.data.KeyValue]]
 */
trait Assignable {
  def key: Slice[Byte]
}

object Assignable {

  val emptyIterable = Iterable.empty[Assignable]
  val emptyIterator: Iterator[Assignable] = Iterator.empty

  implicit val nothingCreator: Aggregator.Creator[Assignable, Nothing] =
    Aggregator.Creator.nothing[Assignable]()

  implicit def listBufferAssignableCreator: Aggregator.Creator[Assignable, ListBuffer[Assignable]] =
    Aggregator.Creator.listBuffer()

  /**
   * A [[Collection]] is a collection of key-values like [[swaydb.core.segment.Segment]]
   * and [[Map[Slice[Byte], Memory, LevelZeroMapCache]].
   *
   * [[Map]] can be created using [[Collection.fromMap]].
   *
   * This type is needed for cases where we can assign a group of key-values to a
   * [[swaydb.core.segment.Segment]] without need to assign each key-value saving
   * CPU times and IO for cases where key-values are persistent - [[swaydb.core.segment.PersistentSegment]].
   */
  trait Collection extends Assignable {
    def maxKey: MaxKey[Slice[Byte]]
    def iterator(): Iterator[KeyValue]
    def getKeyValueCount(): Int
  }

  object Collection {
    def fromMap(map: Map[Slice[Byte], Memory, LevelZeroMapCache]): Assignable.Collection =
      new Collection {
        override def maxKey: MaxKey[Slice[Byte]] =
          map.cache.skipList.last().getS match {
            case fixed: Memory.Fixed =>
              MaxKey.Fixed(fixed.key)

            case Memory.Range(fromKey, toKey, _, _) =>
              MaxKey.Range(fromKey, toKey)
          }

        override def iterator(): Iterator[KeyValue] =
          map.cache.skipList.valuesIterator

        override def getKeyValueCount(): Int =
          map.cache.skipList.size

        override def key: Slice[Byte] =
          map.cache.skipList.headKey.getC
      }
  }
}
