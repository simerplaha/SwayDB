/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data

import swaydb.macros.Sealed

sealed trait DataType {
  def id: Byte
  def name: String
}

object DataType {

  def map(): DataType = Map //for java
  case object Map extends DataType {
    override def id: Byte = 1
    def name = productPrefix
  }

  def set(): DataType = Set //for java
  case object Set extends DataType {
    override def id: Byte = 2
    def name = productPrefix
  }

  case object SetMap extends DataType {
    override def id: Byte = 3
    def name = productPrefix
  }

  case object Queue extends DataType {
    override def id: Byte = 4
    def name = productPrefix
  }

  case object MultiMap extends DataType {
    override def id: Byte = 5
    def name = productPrefix
  }

  case object Custom extends DataType {
    override def id: Byte = 6
    def name = productPrefix
  }

  def all =
    Sealed.array[DataType]

  def apply(id: Byte): Option[DataType] =
    all.find(_.id == id)
}
