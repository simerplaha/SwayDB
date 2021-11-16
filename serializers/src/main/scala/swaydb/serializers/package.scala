/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb

import swaydb.slice.{Slice, SliceOption}
import swaydb.utils.TupleOrNone

package object serializers {

  @inline implicit def toByteSlice[T](data: T)(implicit serializer: Serializer[T]): Slice[Byte] =
    serializer.write(data)

  @inline implicit def toByteSlice[T](data: Option[T])(implicit serializer: Serializer[T]): Option[Slice[Byte]] =
    data.map(serializer.write)

  @inline implicit def toByteSliceOption[T](data: Option[T])(implicit serializer: Serializer[T]): SliceOption[Byte] =
    data match {
      case Some(value) =>
        value: Slice[Byte]

      case None =>
        Slice.Null
    }

  implicit class Decode(slice: Slice[Byte]) {
    @inline final def read[T](implicit serializer: Serializer[T]): T =
      serializer.read(slice)
  }

  implicit class SerialiseImplicits[T](data: T) {
    @inline def serialise(implicit serializer: Serializer[T]): Slice[Byte] =
      toByteSlice(data)
  }

  def read[K, V](tupleOptional: TupleOrNone[Slice[Byte], SliceOption[Byte]])(implicit keySerialiser: Serializer[K],
                                                                             valueSerialiser: Serializer[V]): Option[(K, V)] =
    tupleOptional match {
      case TupleOrNone.None =>
        None

      case TupleOrNone.Some(left, right) =>
        Some((keySerialiser.read(left), valueSerialiser.read(right.getOrElseC(Slice.emptyBytes))))
    }

  implicit class DecodeOption(slice: Option[Slice[Byte]]) {
    @inline final def read[T](implicit serializer: Serializer[T]): T =
      slice match {
        case Some(slice) =>
          serializer.read(slice)

        case None =>
          serializer.read(Slice.emptyBytes)
      }
  }

  implicit class DecodeOptionSliceOption(slice: SliceOption[Byte]) {
    @inline final def read[T](implicit serializer: Serializer[T]): T =
      slice match {
        case slice: Slice[Byte] =>
          serializer.read(slice)

        case Slice.Null =>
          serializer.read(Slice.emptyBytes)
      }
  }
}
