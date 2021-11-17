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

import java.util

object SkipListTreeMap {

  def apply[OK, OV, K <: OK, V <: OV](nullKey: OK,
                                      nullValue: OV)(implicit ordering: KeyOrder[K]): SkipListTreeMap[OK, OV, K, V] =
    new SkipListTreeMap[OK, OV, K, V](
      skipList = new util.TreeMap[K, V](ordering),
      nullKey = nullKey,
      nullValue = nullValue
    )

}

private[swaydb] class SkipListTreeMap[OK, OV, K <: OK, V <: OV] private(protected val skipList: util.TreeMap[K, V],
                                                                      val nullKey: OK,
                                                                      val nullValue: OV)(implicit val keyOrder: KeyOrder[K]) extends SkipListNavigable[OK, OV, K, V](skipList.size()) {

  override def remove(key: K): Unit =
    throw new IllegalAccessException("Operation not allowed - TreeMap SkipList")

  // only single put is allowed. Used during the creation of this skipList.
  override def putIfAbsent(key: K, value: V): Boolean =
    throw new IllegalAccessException("Operation not allowed - TreeMap SkipList")

}
