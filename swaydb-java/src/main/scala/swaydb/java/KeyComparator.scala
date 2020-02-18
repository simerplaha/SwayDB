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

package swaydb.java

import java.util.Comparator

trait KeyComparator[K] extends Comparator[K] {
  /**
   * You don't have to implement this if you searches do not use partial key.
   *
   * The key is used to create secondary indexes like HashIndex and BloomFilters.
   *
   * Used for partially ordered keys.
   *
   * For example if your key is
   * {{{
   *   class MyData(id: Int, name: Optional[String]))
   * }}}
   *
   * Suppose your key is MyData(1, "John") and your [[KeyComparator]] if on MyData.id
   * then this [[comparableKey]] should return MyData(1, None) so you can search for
   * keys like MyData(1, None) and get result MyData(1, "John").
   *
   * This is called partial ordering and full reads on partial keys so you
   * can store extra data with key without having to read the value.
   */
  def comparableKey(key: K): K = key
}
