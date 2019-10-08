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

package swaydb.serializers

import swaydb.data.slice.Slice

trait Serializer[A] {
  def write(data: A): Slice[Byte]

  def read(data: Slice[Byte]): A
}

trait JavaSerializer[A] extends Serializer[A] {
  override final def read(data: Slice[Byte]): A =
    readData(data.asInstanceOf[Slice[java.lang.Byte]])

  override final def write(data: A): Slice[Byte] =
    writeData(data).asInstanceOf[Slice[Byte]]

  def writeData(data: A): Slice[java.lang.Byte]

  def readData(data: Slice[java.lang.Byte]): A
}