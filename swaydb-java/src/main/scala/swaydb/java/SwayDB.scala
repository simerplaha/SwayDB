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

import swaydb.data.order.KeyOrder.default
import swaydb.java.data.slice.ByteSlice

object SwayDB {

  def defaultComparator: KeyComparator[ByteSlice] =
    new KeyComparator[ByteSlice] {
      override def compare(t: ByteSlice, t1: ByteSlice): Int =
        default.compare(t.asScala.asInstanceOf[swaydb.data.slice.Slice[Byte]], t1.asScala.asInstanceOf[swaydb.data.slice.Slice[Byte]])

      override def comparableKey(data: ByteSlice): ByteSlice =
        data
    }
}
