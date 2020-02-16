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
 */

package swaydb.core.level

import swaydb.core.function.FunctionStore
import swaydb.core.map.{MapEntry, SkipListMerger}
import swaydb.core.segment.{Segment, SegmentOption}
import swaydb.core.util.skiplist.ConcurrentSkipList
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.{Slice, SliceOption}

/**
 * Default [[SkipListMerger]] implementation for Level's Appendix. Currently appendix does not implement
 * Range APIs so merger should never be used.
 */
object AppendixSkipListMerger extends SkipListMerger[SliceOption[Byte], SegmentOption, Slice[Byte], Segment] {
  override def insert(insertKey: Slice[Byte],
                      insertValue: Segment,
                      skipList: ConcurrentSkipList[SliceOption[Byte], SegmentOption, Slice[Byte], Segment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                            timeOrder: TimeOrder[Slice[Byte]],
                                                                                                            functionStore: FunctionStore): Unit =
    throw new IllegalAccessException("Appendix does not require merger.")

  //Appendixes do not use Range so there will be no conflicts. Need a type-safe way of handling this.
  override def insert(entry: MapEntry[Slice[Byte], Segment],
                      skipList: ConcurrentSkipList[SliceOption[Byte], SegmentOption, Slice[Byte], Segment])(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                                                                            timeOrder: TimeOrder[Slice[Byte]],
                                                                                                            functionStore: FunctionStore): Unit =
    throw new IllegalAccessException("Appendix does not require merger.")
}
