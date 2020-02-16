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

package swaydb.core.util.skiplist

import java.util.concurrent.ConcurrentSkipListMap

private[core] class ConcurrentSkipList[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](private var skipper: ConcurrentSkipListMap[Key, Value],
                                                                                                       val nullKey: OptionKey,
                                                                                                       val nullValue: OptionValue) extends SkipListBase[OptionKey, OptionValue, Key, Value, ConcurrentSkipListMap[Key, Value]](skipper, true) {
  /**
   * FIXME - [[SkipListBase]] mutates [[skipList]] when batches are submitted. This [[skipper]] is not require after
   * the class is instantiated and should be nulled to save memory. But instead of null there needs to be a better way to of delegating skipList logic
   * to [[SkipListBase]] without storing a reference of the original skipList in this instance.
   */
  skipper = null

  override def cloneInstance(skipList: ConcurrentSkipListMap[Key, Value]): ConcurrentSkipList[OptionKey, OptionValue, Key, Value] =
    new ConcurrentSkipList(
      skipper = skipList.clone(),
      nullKey = nullKey,
      nullValue = nullValue
    )
}