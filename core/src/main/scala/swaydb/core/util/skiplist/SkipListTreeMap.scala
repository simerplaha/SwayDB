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

private[core] class SkipListTreeMap[OptionKey, OptionValue, Key <: OptionKey, Value <: OptionValue](private var skipper: util.TreeMap[Key, Value],
                                                                                                    val nullKey: OptionKey,
                                                                                                    val nullValue: OptionValue) extends SkipListBatchableImpl[OptionKey, OptionValue, Key, Value, util.TreeMap[Key, Value]](skipper) with SkipListNavigable[OptionKey, OptionValue, Key, Value, util.TreeMap[Key, Value]] {
  /**
   * FIXME - [[SkipListBatchableImpl]] mutates [[skipList]] when batches are submitted. This [[skipper]] is not require after
   * the class is instantiated and should be nulled to save memory. But instead of null there needs to be a better way to of delegating skipList logic
   * to [[SkipListBatchableImpl]] without storing a reference of the original skipList in this instance.
   */
  skipper = null

  override def remove(key: Key): Unit =
    throw new IllegalAccessException("Operation not allowed - TreeMap SkipList")

  override def batch(batches: Iterable[SkipList.Batch[Key, Value]]): Unit =
    throw new IllegalAccessException("Operation not allowed - TreeMap SkipList")

  // only single put is allowed. Used during the creation of this skipList.
  // override def put(key: Key, value: Value): Unit

  override def putIfAbsent(key: Key, value: Value): Boolean =
    throw new IllegalAccessException("Operation not allowed - TreeMap SkipList")

  override def cloneInstance(skipList: util.TreeMap[Key, Value]): SkipListTreeMap[OptionKey, OptionValue, Key, Value] =
    throw new IllegalAccessException("Operation not allowed - TreeMap SkipList")
}
