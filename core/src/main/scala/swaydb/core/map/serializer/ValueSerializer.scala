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

package swaydb.core.map.serializer

import swaydb.core.data.Value
import swaydb.core.io.reader.Reader
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.implicitNotFound
import scala.util.{Success, Try}

@implicitNotFound("Type class implementation not found for ValueSerializer of type ${T}")
sealed trait ValueSerializer[T <: Value] {
  def write(value: T, bytes: Slice[Byte]): Unit

  def read(bytes: Slice[Byte]): Try[T]

  def read(reader: Reader): Try[T]

  def bytesRequired(value: T): Int
}

object ValueSerializers {

  implicit object AddSerializer extends ValueSerializer[Value.Put] {

    //TODO - don't need Value length in Add. It's already being set in index entry.
    override def write(value: Value.Put, bytes: Slice[Byte]): Unit =
      bytes
        .addInt(value.value.map(_.size).getOrElse(0))
        .addAll(value.value.getOrElse(Slice.emptyByteSlice))

    override def read(bytes: Slice[Byte]): Try[Value.Put] =
      read(Reader(bytes))

    override def bytesRequired(value: Value.Put): Int =
      ByteSizeOf.int +
        value.value.map(_.size).getOrElse(0)

    override def read(reader: Reader): Try[Value.Put] =
      reader.readInt() flatMap {
        valueLength =>
          if (valueLength == 0)
            Success(Value.Put(None))
          else
            reader.read(valueLength) map {
              value =>
                Value.Put(Some(value))
            }
      }
  }
}

object ValueSerializer {

  import ValueSerializers._

  def write[T <: Value](value: T)(bytes: Slice[Byte])(implicit serializer: ValueSerializer[T]): Unit =
    serializer.write(value, bytes)

  def read[T <: Value](value: Slice[Byte])(implicit serializer: ValueSerializer[T]): Try[T] =
    serializer.read(value)

  def read[T <: Value](reader: Reader)(implicit serializer: ValueSerializer[T]): Try[T] =
    serializer.read(reader)

  def bytesRequired[T <: Value](value: T)(implicit serializer: ValueSerializer[T]): Int =
    serializer.bytesRequired(value)

  def byteSize(value: Value): Int =
    value match {
      case _: Value.Remove => 0
      case value @ Value.Put(_) => bytesRequired(value)
    }
}