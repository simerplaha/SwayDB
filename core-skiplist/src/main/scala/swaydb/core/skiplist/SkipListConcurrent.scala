/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.skiplist

import swaydb.slice.order.KeyOrder

import java.util.concurrent.ConcurrentSkipListMap

object SkipListConcurrent {

  def apply[OK, OV, K <: OK, V <: OV](nullKey: OK,
                                      nullValue: OV)(implicit ordering: KeyOrder[K]): SkipListConcurrent[OK, OV, K, V] =
    new SkipListConcurrent[OK, OV, K, V](
      skipList = new ConcurrentSkipListMap[K, V](ordering),
      nullKey = nullKey,
      nullValue = nullValue
    )
}

private[swaydb] class SkipListConcurrent[OK, OV, K <: OK, V <: OV] private(@volatile protected var skipList: ConcurrentSkipListMap[K, V],
                                                                         val nullKey: OK,
                                                                         val nullValue: OV)(implicit val keyOrder: KeyOrder[K]) extends SkipListNavigable[OK, OV, K, V](skipList.size()) with SkipListBatchable[OK, OV, K, V] {

  /**
   * Does not support concurrent batch writes since it's only being used by [[swaydb.core.level.Level]] which
   * write to appendix sequentially.
   */
  def batch(transaction: SkipListConcurrent[OK, OV, K, V] => Unit): Unit = {
    val newSkipList =
      new SkipListConcurrent(
        skipList = skipList.clone(),
        nullKey = nullKey,
        nullValue = nullValue
      )

    transaction(newSkipList)

    this.skipList = newSkipList.skipList
    sizer.set(this.skipList.size())
  }
}
