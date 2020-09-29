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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core.level.zero

import swaydb.core.data.{Memory, MemoryOption}
import swaydb.core.level.zero.LeveledSkipList.SkipListState
import swaydb.core.util.queue.{VolatileQueue, Walker}
import swaydb.core.util.skiplist.SkipList
import swaydb.data.slice.{Slice, SliceOption}

import scala.beans.BeanProperty

private[core] object LeveledSkipList {

  class SkipListState private[zero](val skipList: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                                    @BeanProperty @volatile var hasRange: Boolean)


  @inline def apply(skipList: SkipListState): LeveledSkipList =
    new LeveledSkipList(
      queue = VolatileQueue[SkipListState](skipList),
      currentState = skipList
    )
}

private[core] class LeveledSkipList private[zero](queue: VolatileQueue[SkipListState],
                                                  @volatile private var currentState: SkipListState) {

  @inline def current = currentState

  def addFirst(skipList: SkipListState): Unit = {
    queue.addHead(skipList)
    currentState = skipList
  }

  def isEmpty: Boolean =
    queue.isEmpty || queue.iterator.forall(_.skipList.isEmpty)

  @inline def size: Int =
    queue.iterator.foldLeft(0)(_ + _.skipList.size)

  @inline def queueSize: Int =
    queue.size

  @inline def iterator =
    queue.iterator

  @inline def hasRange =
    iterator.exists(_.hasRange)

  def walker: Walker[SkipListState] =
    queue

}
