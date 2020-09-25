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

import java.util.concurrent.ConcurrentSkipListMap

import swaydb.data.order.KeyOrder

object SkipListConcurrent {
  def apply[OK, OV, K <: OK, V <: OV](nullKey: OK,
                                      nullValue: OV)(implicit ordering: KeyOrder[K]): SkipListConcurrent[OK, OV, K, V] =
    new SkipListConcurrent[OK, OV, K, V](
      skipList = new ConcurrentSkipListMap[K, V](ordering),
      nullKey = nullKey,
      nullValue = nullValue
    )
}

private[core] class SkipListConcurrent[OK, OV, K <: OK, V <: OV] private(@volatile var skipList: ConcurrentSkipListMap[K, V],
                                                                         val nullKey: OK,
                                                                         val nullValue: OV) extends SkipListNavigable[OK, OV, K, V, ConcurrentSkipListMap[K, V]](skipList.size()) with SkipListBatchable[OK, OV, K, V] {

  /**
   * Does not support concurrent batch writes since it's only being used by [[swaydb.core.level.Level]] which
   * write to appendix concurrently.
   */
  def batch(batches: Iterable[SkipList.Batch[K, V]]): Unit = {
    var cloned = false
    val targetSkipList =
      if (batches.size > 1) {
        cloned = true

        new SkipListConcurrent(
          skipList = skipList.clone(),
          nullKey = nullKey,
          nullValue = nullValue
        )
      } else {
        this
      }

    batches foreach {
      batch =>
        batch apply targetSkipList
    }

    if (cloned) {
      this.skipList = targetSkipList.skipList
      sizer.set(this.skipList.size())
    }
  }

}
