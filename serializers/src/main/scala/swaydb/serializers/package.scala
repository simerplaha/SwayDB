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

package swaydb

import swaydb.data.slice.{Slice, SliceOptional}
import swaydb.data.util.TupleOptional

package object serializers {

  @inline implicit def toSlice[T](data: T)(implicit serializer: Serializer[T]): Slice[Byte] =
    serializer.write(data)

  @inline implicit def toSlice[T](data: Option[T])(implicit serializer: Serializer[T]): Option[Slice[Byte]] =
    data.map(serializer.write)

  implicit class Decode(slice: Slice[Byte]) {
    @inline def read[T](implicit serializer: Serializer[T]): T =
      serializer.read(slice)
  }

  def read[K, V](tupleOptional: TupleOptional[Slice[Byte], SliceOptional[Byte]])(implicit keySerialiser: Serializer[K],
                                                                                 valueSerialiser: Serializer[V]): Option[(K, V)] =
    tupleOptional match {
      case TupleOptional.None =>
        None

      case TupleOptional.Some(left, right) =>
        Some(keySerialiser.read(left), valueSerialiser.read(right.getOrElseC(Slice.emptyBytes)))
    }

  implicit class DecodeOption(slice: Option[Slice[Byte]]) {
    @inline def read[T](implicit serializer: Serializer[T]): T =
      slice match {
        case Some(slice) =>
          serializer.read(slice)

        case None =>
          serializer.read(Slice.emptyBytes)
      }
  }

  implicit class DecodeOptionSliceOptional(slice: SliceOptional[Byte]) {
    @inline def read[T](implicit serializer: Serializer[T]): T =

      slice.valueOrElseC(serializer.read, serializer.read(Slice.emptyBytes))
  }
}