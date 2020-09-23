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

package swaydb.core.util.skiplist

import java.util

protected abstract class SkipListBatchableImpl[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue, SL <: util.NavigableMap[Key, Value]](@volatile var skipList: SL) extends SkipListBatchable[OptionKey, OptionValue, Key, Value] {

  def cloneInstance(skipList: SL): SkipListBatchableImpl[OptionKey, OptionValue, Key, Value, SL]

  /**
   * Does not support concurrent batch writes since it's only being used by [[swaydb.core.level.Level]] which
   * write to appendix concurrently.
   */
  def batch(batches: Iterable[SkipList.Batch[Key, Value]]): Unit = {
    var cloned = false
    val targetSkipList =
      if (batches.size > 1) {
        cloned = true
        this.cloneInstance(skipList)
      } else {
        this
      }

    batches foreach {
      batch =>
        batch apply targetSkipList
    }

    if (cloned)
      this.skipList = targetSkipList.skipList
  }

  def put(keyValues: Iterable[(Key, Value)]): Unit = {
    var cloned = false
    val targetSkipList =
      if (keyValues.size > 1) {
        cloned = true
        this.cloneInstance(skipList).skipList
      } else {
        skipList
      }

    keyValues foreach {
      case (key, value) =>
        targetSkipList.put(key, value)
    }

    if (cloned)
      this.skipList = targetSkipList
  }
}
