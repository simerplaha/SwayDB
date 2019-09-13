/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.data.order

import swaydb.data.slice.Slice
import swaydb.data.util.ByteUtil

private[swaydb] object TimeOrder {
  val long = new TimeOrder[Slice[Byte]] {
    override def compare(left: Slice[Byte], right: Slice[Byte]): Int =
      if (left.isEmpty || right.isEmpty)
        1 //if either of them are empty then favour left to be the largest.
      else
        ByteUtil.readUnsignedLongRightAlignedUnsafe(left) compare ByteUtil.readUnsignedLongRightAlignedUnsafe(right)
  }
}

private[swaydb] trait TimeOrder[T] extends Ordering[T]
