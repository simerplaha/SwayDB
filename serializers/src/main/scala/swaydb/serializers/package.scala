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

package swaydb

import swaydb.data.slice.Slice

package object serializers {

  implicit def toSlice[T](data: T)(implicit code: Serializer[T]): Slice[Byte] =
    code.write(data)

  implicit def toSlice[T](data: Option[T])(implicit code: Serializer[T]): Option[Slice[Byte]] =
    data.map(value => code.write(value))

  implicit class Decode(slice: Slice[Byte]) {
    def read[T](implicit code: Serializer[T]): T =
      code.read(slice)
  }

  implicit class DecodeOption(slice: Option[Slice[Byte]]) {
    def read[T](implicit code: Serializer[T]): T =
      slice.map(slice => code.read(slice)) getOrElse code.read(Slice.emptyBytes)
  }
}