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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.level.zero

import swaydb.core.data.{Memory, MemoryOption}
import swaydb.core.function.FunctionStore
import swaydb.core.level.memory.LeveledSkipList
import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.core.merge.FixedMerger
import swaydb.core.segment.merge.{MergeStats, SegmentMerger}
import swaydb.core.util.skiplist.{SkipList, SkipListConcurrent, SkipListSeries}
import swaydb.data.OptimiseWrites
import swaydb.data.cache.Cache
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}

import scala.annotation.tailrec
import scala.collection.mutable.ListBuffer

private[core] object LevelZeroMapCache {

  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]],
                       timeOrder: TimeOrder[Slice[Byte]],
                       functionStore: FunctionStore,
                       optimiseWrites: OptimiseWrites): MapCacheBuilder[LevelZeroMapCache] =
    () => {
      val state = LeveledSkipList.newLevel()

      val leveledSkipList = LeveledSkipList(level = state)

      new LevelZeroMapCache(leveledSkipList)
    }

}

/**
 * Ensures atomic and guarantee all or none writes to in-memory SkipList.
 *
 * Creates multi-layered SkipList.
 */
private[core] class LevelZeroMapCache private(val levels: LeveledSkipList)(implicit val keyOrder: KeyOrder[Slice[Byte]],
                                                                           timeOrder: TimeOrder[Slice[Byte]],
                                                                           functionStore: FunctionStore,
                                                                           optimiseWrites: OptimiseWrites) extends MapCache[Slice[Byte], Memory] {

  @inline private def write(entry: MapEntry[Slice[Byte], Memory], atomic: Boolean): Unit = {
    val entries = entry.entries

    if (entry.entriesCount > 1 || levels.zero.hasRange || entry.hasUpdate || entry.hasRange || entry.hasRemoveDeadline)
      LeveledSkipList.doWrite(
        head = entries.head,
        tail = entries.tail,
        skipList = levels.zero,
        atomic = atomic,
        startedNewTransaction = false
      ) foreach {
        newSkipList =>
          levels.addHead(newSkipList)
      }
    else
      entries.head applyPoint levels.zero.skipList
  }

  override def writeAtomic(entry: MapEntry[Slice[Byte], Memory]): Unit =
    write(entry = entry, atomic = true)

  override def writeNonAtomic(entry: MapEntry[Slice[Byte], Memory]): Unit =
    write(entry = entry, atomic = false)

  override def isEmpty: Boolean =
    levels.isEmpty

  override def maxKeyValueCount: Int =
    levels.size

  override def iterator: Iterator[(Slice[Byte], Memory)] =
    levels
      .iterator
      .flatMap(_.skipList.iterator)
}
