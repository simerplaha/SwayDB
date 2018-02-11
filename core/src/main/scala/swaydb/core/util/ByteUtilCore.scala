/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core.util

import swaydb.data.slice.Slice

private[core] object ByteUtilCore {

  def commonPrefixBytes(a: Slice[Byte], b: Slice[Byte]): Int = {
    val min = Math.min(a.size, b.size)
    var i = 0
    while (i < min && a(i) == b(i))
      i += 1
    i
  }

  def sizeUnsignedInt(v: Int): Int = {
    var size = 0
    var x = v
    while ((x & 0xFFFFF80) != 0L) {
      size += 1
      x >>>= 7
    }
    size += 1
    size
  }

}
