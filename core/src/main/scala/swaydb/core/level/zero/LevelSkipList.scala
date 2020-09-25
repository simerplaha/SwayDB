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
import swaydb.core.util.skiplist.{SkipList, SkipListSeries}
import swaydb.data.slice.{Slice, SliceOption}

import scala.beans.BeanProperty

private[core] object LevelSkipList {
  def apply(skipList: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory], hasRange: Boolean): LevelSkipList =
    new LevelSkipList(skipList, hasRange)
}

private[core] class LevelSkipList private[zero](val skipList: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory],
                                                @BeanProperty @volatile var hasRange: Boolean) {
  def toSliceRemoveNullFromSeries: Slice[Memory] =
    skipList match {
      case series: SkipListSeries[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] =>
        val slice = Slice.of[Memory](series.size)

        //foreach to clear all removed key-values (null values). SkipListSeries is write optimised backed
        //by an Array so it cannot physically remove the key-value which would require reordering of the
        //Arrays instead it nulls the value indicating remove.
        series foreach {
          case (_, value) =>
            slice add value
        }

        slice

      case skipList: SkipList[SliceOption[Byte], MemoryOption, Slice[Byte], Memory] =>
        Slice.from(skipList.values(), skipList.size)
    }
}
