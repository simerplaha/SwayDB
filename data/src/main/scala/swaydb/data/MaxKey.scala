/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.data

import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.SomeOrNoneCovariant

sealed trait MaxKeyOrNull[+T] extends SomeOrNoneCovariant[MaxKeyOrNull[T], MaxKey[T]] {
  override def noneC: MaxKeyOrNull[Nothing] = MaxKey.Null
}

sealed trait MaxKey[+T] extends MaxKeyOrNull[T] {
  def maxKey: T
  def inclusive: Boolean
  override def isNoneC: Boolean = false
  override def getC: MaxKey[T] = this
}

object MaxKey {

  final case object Null extends MaxKeyOrNull[Nothing] {
    override def isNoneC: Boolean = true
    override def getC: MaxKey[Nothing] = throw new Exception("MaxKey is of type Null")
  }

  implicit class MaxKeyImplicits(maxKey: MaxKey[Slice[Byte]]) {
    @inline final def unslice() =
      maxKey match {
        case Fixed(maxKey) =>
          Fixed(maxKey.unslice())

        case Range(fromKey, maxKey) =>
          Range(fromKey.unslice(), maxKey.unslice())
      }

    @inline final def lessThan(key: Slice[Byte])(implicit keyOrder: KeyOrder[Slice[Byte]]): Boolean = {
      import keyOrder._
      (maxKey.inclusive && maxKey.maxKey < key) || (!maxKey.inclusive && maxKey.maxKey <= key)
    }
  }

  case class Fixed[+T](maxKey: T) extends MaxKey[T] {
    override def inclusive: Boolean = true
  }

  case class Range[+T](fromKey: T, maxKey: T) extends MaxKey[T] {
    override def inclusive: Boolean = false
  }
}
