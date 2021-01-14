/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.util.skiplist

import java.util

import swaydb.data.order.KeyOrder

object SkipListTreeMap {

  def apply[OK, OV, K <: OK, V <: OV](nullKey: OK,
                                      nullValue: OV)(implicit ordering: KeyOrder[K]): SkipListTreeMap[OK, OV, K, V] =
    new SkipListTreeMap[OK, OV, K, V](
      skipList = new util.TreeMap[K, V](ordering),
      nullKey = nullKey,
      nullValue = nullValue
    )

}

private[core] class SkipListTreeMap[OK, OV, K <: OK, V <: OV] private(protected val skipList: util.TreeMap[K, V],
                                                                      val nullKey: OK,
                                                                      val nullValue: OV)(implicit val keyOrder: KeyOrder[K]) extends SkipListNavigable[OK, OV, K, V](skipList.size()) {

  override def remove(key: K): Unit =
    throw new IllegalAccessException("Operation not allowed - TreeMap SkipList")

  // only single put is allowed. Used during the creation of this skipList.
  override def putIfAbsent(key: K, value: V): Boolean =
    throw new IllegalAccessException("Operation not allowed - TreeMap SkipList")

}
