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

private[swaydb] sealed trait ValueType {
  def id: Int

  def isDelete: Boolean

  def notDelete: Boolean = !isDelete
}

private[swaydb] object ValueType {

  def apply(id: Int): ValueType =
    if (id == Add.id)
      Add
    else
      Remove

  object Add extends ValueType {
    override def id: Int = 0x00

    override def isDelete: Boolean = false
  }
  object Remove extends ValueType {
    override def id: Int = 0x01

    override def isDelete: Boolean = true
  }
}
