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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.repairAppendix

import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice

sealed trait MaxKey[T] {
  val maxKey: T
  val inclusive: Boolean
}

object MaxKey {

  implicit class MaxKeyImplicits(maxKey: MaxKey[Slice[Byte]]) {
    def unslice() =
      maxKey match {
        case Fixed(maxKey) =>
          Fixed(maxKey.unslice())

        case Range(fromKey, maxKey) =>
          Range(fromKey.unslice(), maxKey.unslice())
      }

    def lessThan(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
      import keyOrder._
      (maxKey.inclusive && maxKey.maxKey < key) || (!maxKey.inclusive && maxKey.maxKey <= key)
    }
  }

  case class Fixed[T](maxKey: T) extends MaxKey[T] {
    override val inclusive: Boolean = true
  }

  case class Range[T](fromKey: T, maxKey: T) extends MaxKey[T] {
    override val inclusive: Boolean = false
  }
}