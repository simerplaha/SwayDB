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
import swaydb.core.data.Value.{Put, Remove}
import swaydb.core.io.reader.Reader
import swaydb.core.util.ByteUtilCore
import swaydb.data.slice.{Reader, Slice}
import swaydb.data.util.ByteSizeOf

import scala.annotation.implicitNotFound
import scala.util.{Success, Try}

@implicitNotFound("Type class implementation not found for ValueSerializer of type ${T}")
trait ValueSerializer[T] {

  def write(value: T, bytes: Slice[Byte]): Unit

  def read(reader: Reader): Try[T]

  def read(bytes: Slice[Byte]): Try[T] =
    read(Reader(bytes))

  def bytesRequired(value: T): Int
}

object ValueSerializers {

  object PutSerializerWithoutSize extends ValueSerializer[Value.Put] {

    override def write(value: Value.Put, bytes: Slice[Byte]): Unit =
      bytes
        .addAll(value.value.getOrElse(Slice.emptyByteSlice))

    override def bytesRequired(value: Value.Put): Int =
      value.value.map(_.size).getOrElse(0)

    override def read(reader: Reader): Try[Value.Put] =
      reader.remaining flatMap {
        remaining =>
          if (remaining == 0)
            Success(Value.Put(None))
          else
            reader.read(remaining.toInt) map {
              value =>
                Value.Put(Some(value))
            }
      }
  }

  object PutSerializerWithSize extends ValueSerializer[Value.Put] {

    override def write(value: Value.Put, bytes: Slice[Byte]): Unit =
      bytes
        .addInt(value.value.map(_.size).getOrElse(0))
        .addAll(value.value.getOrElse(Slice.emptyByteSlice))

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

  object PutSerializerWithUnsignedSize extends ValueSerializer[Value.Put] {

    override def write(value: Value.Put, bytes: Slice[Byte]): Unit =
      bytes
        .addIntUnsigned(value.value.map(_.size).getOrElse(0))
        .addAll(value.value.getOrElse(Slice.emptyByteSlice))

    override def bytesRequired(value: Value.Put): Int =
      ByteUtilCore.sizeUnsignedInt(value.value.map(_.size).getOrElse(0)) +
        value.value.map(_.size).getOrElse(0)

    override def read(reader: Reader): Try[Value.Put] =
      reader.readIntUnsigned() flatMap {
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

  def write[T](value: T)(bytes: Slice[Byte])(implicit serializer: ValueSerializer[T]): Unit =
    serializer.write(value, bytes)

  def read[T](value: Slice[Byte])(implicit serializer: ValueSerializer[T]): Try[T] =
    serializer.read(value)

  def read[T](reader: Reader)(implicit serializer: ValueSerializer[T]): Try[T] =
    serializer.read(reader)

  def bytesRequired[T](value: T)(implicit serializer: ValueSerializer[T]): Int =
    serializer.bytesRequired(value)
}