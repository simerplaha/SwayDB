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

package swaydb.java

import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice._
import swaydb.java.data.slice.{ByteSlice, ByteSliceBuilder}
import swaydb.serializers.Serializer

protected object KeyOrderConverter {

  def toScalaKeyOrder[K](comparatorEither: Either[KeyComparator[ByteSlice], KeyComparator[K]],
                         keySerializer: Serializer[K]) =
    comparatorEither match {
      case Right(comparator) =>
        new KeyOrder[Slice[Byte]] {
          override def compare(left: Slice[Byte], right: Slice[Byte]): Int = {
            val leftKey = keySerializer.read(left)
            val rightKey = keySerializer.read(right)
            comparator.compare(leftKey, rightKey)
          }
        }

      case Left(comparator) =>
        new KeyOrder[Slice[Byte]] {
          override def compare(x: Slice[Byte], y: Slice[Byte]): Int =
            comparator.compare(ByteSliceBuilder(x), ByteSliceBuilder(y))
        }
    }
}
