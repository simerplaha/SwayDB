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

package swaydb.order

import swaydb.data.slice.Slice

object KeyOrder {

  implicit val default = new Ordering[Slice[Byte]] {
    def compare(a: Slice[Byte], b: Slice[Byte]): Int = {
      val minimum = java.lang.Math.min(a.written, b.written)
      var i = 0
      while (i < minimum) {
        val aB = a(i) & 0xFF
        val bB = b(i) & 0xFF
        if (aB != bB) return aB - bB
        i += 1
      }
      a.written - b.written
    }
  }
}