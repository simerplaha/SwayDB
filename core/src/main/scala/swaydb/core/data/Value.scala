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

package swaydb.core.data

import swaydb.data.slice.Slice

private[swaydb] sealed trait Value {
  def id: Int

  def isRemove: Boolean

  def notRemove: Boolean = !isRemove
}

private[swaydb] object Value {

  implicit class UnSliceValue(value: Value) {

    /**
      * @return An Value key-value with it's byte arrays sliced.
      *         If the sliced byte array is empty, it set the value to None.
      */
    def unslice: Value =
      value match {
        case Value.Put(value) =>
          val unslicedValue = value.map(_.unslice())
          if (unslicedValue.exists(_.isEmpty))
            Value.Put(None)
          else
            Value.Put(unslicedValue)

        case Value.Remove =>
          value
      }
  }

  sealed trait Remove extends Value
  case object Remove extends Remove {
    override def id: Int = 0

    override def isRemove: Boolean = true
  }

  object Put {
    def apply(value: Slice[Byte]): Put =
      new Put(Some(value))
  }

  case class Put(value: Option[Slice[Byte]]) extends Value {
    override def id: Int = 1

    override def isRemove: Boolean = false
  }
}
