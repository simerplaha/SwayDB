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

package swaydb.core.level

import swaydb.core.map.MapEntry.Batch
import swaydb.core.map.{MapCache, MapCacheBuilder, MapEntry}
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.util.skiplist.SkipListConcurrent
import swaydb.data.order.KeyOrder
import swaydb.data.slice.{Slice, SliceOption}

/**
 * Default [[SkipListMerger]] implementation for Level's Appendix. Currently appendix does not implement
 * Range APIs so merger should never be used.
 */

object AppendixMapCache {

  implicit def builder(implicit keyOrder: KeyOrder[Slice[Byte]]) =
    new MapCacheBuilder[AppendixMapCache] {
      override def create(): AppendixMapCache =
        new AppendixMapCache(SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment](Slice.Null, Segment.Null))
    }
}

class AppendixMapCache(val skipList: SkipListConcurrent[SliceOption[Byte], SegmentOption, Slice[Byte], Segment]) extends MapCache[Slice[Byte], Segment] {

  override def writeAtomic(entry: MapEntry[Slice[Byte], Segment]): Unit =
    entry applyBatch skipList

  override def writeNonAtomic(entry: MapEntry[Slice[Byte], Segment]): Unit =
    entry match {
      case MapEntry.Put(key, value) =>
        skipList.put(key, value)

      case MapEntry.Remove(key) =>
        skipList.remove(key)

      case batch: Batch[Slice[Byte], Segment] =>
        batch.entries.foreach(writeNonAtomic)
    }

  override def asScala: Iterable[(Slice[Byte], Segment)] =
    skipList.asScala

  override def isEmpty: Boolean =
    skipList.isEmpty

  override def maxKeyValueCount: Int =
    skipList.size


}
