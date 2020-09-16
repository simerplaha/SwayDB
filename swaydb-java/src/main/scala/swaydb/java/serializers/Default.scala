/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.java.serializers

import java.nio.charset.StandardCharsets
import java.util.Optional

import swaydb.data.slice.Slice
import swaydb.serializers.Default._

object Default {

  def intSerializer(): Serializer[java.lang.Integer] = JavaIntSerializer

  object JavaIntSerializer extends Serializer[java.lang.Integer] {
    override def write(data: java.lang.Integer): Slice[java.lang.Byte] =
      Slice.writeInt[java.lang.Byte](data)

    override def read(slice: Slice[java.lang.Byte]): java.lang.Integer =
      slice.readInt()
  }

  def longSerializer(): Serializer[java.lang.Long] = JavaLongSerializer

  object JavaLongSerializer extends Serializer[java.lang.Long] {
    override def write(data: java.lang.Long): Slice[java.lang.Byte] =
      Slice.writeLong[java.lang.Byte](data)

    override def read(slice: Slice[java.lang.Byte]): java.lang.Long =
      slice.readLong()
  }

  def charSerializer(): Serializer[java.lang.Character] = JavaCharSerializer

  object JavaCharSerializer extends Serializer[java.lang.Character] {
    override def write(data: java.lang.Character): Slice[java.lang.Byte] =
      CharSerializer.write(data).cast

    override def read(slice: Slice[java.lang.Byte]): java.lang.Character =
      CharSerializer.read(slice.cast)
  }

  def doubleSerializer(): Serializer[java.lang.Double] = JavaDoubleSerializer

  object JavaDoubleSerializer extends Serializer[java.lang.Double] {
    override def write(data: java.lang.Double): Slice[java.lang.Byte] =
      DoubleSerializer.write(data).cast

    override def read(slice: Slice[java.lang.Byte]): java.lang.Double =
      slice.toByteBufferWrap.getDouble
  }

  def floatSerializer(): Serializer[java.lang.Float] = JavaFloatSerializer

  object JavaFloatSerializer extends Serializer[java.lang.Float] {
    override def write(data: java.lang.Float): Slice[java.lang.Byte] =
      FloatSerializer.write(data).cast

    override def read(slice: Slice[java.lang.Byte]): java.lang.Float =
      FloatSerializer.read(slice.cast)
  }

  def shortSerializer(): Serializer[java.lang.Short] = JavaShortSerializer

  object JavaShortSerializer extends Serializer[java.lang.Short] {
    override def write(data: java.lang.Short): Slice[java.lang.Byte] =
      ShortSerializer.write(data).cast

    override def read(slice: Slice[java.lang.Byte]): java.lang.Short =
      ShortSerializer.read(slice.cast)
  }

  def stringSerializer(): Serializer[java.lang.String] = StringSerializer

  object StringSerializer extends Serializer[java.lang.String] {
    override def write(data: java.lang.String): Slice[java.lang.Byte] =
      Slice.writeString[java.lang.Byte](data, StandardCharsets.UTF_8)

    override def read(slice: Slice[java.lang.Byte]): java.lang.String =
      slice.readString(StandardCharsets.UTF_8)
  }

  def optionalStringSerializer(): Serializer[Optional[java.lang.String]] =
    JavaOptionalStringSerializer

  object JavaOptionalStringSerializer extends Serializer[Optional[java.lang.String]] {
    override def write(data: Optional[java.lang.String]): Slice[java.lang.Byte] =
      if (data.isPresent)
        Slice.writeString[java.lang.Byte](data.get(), StandardCharsets.UTF_8)
      else
        Slice.emptyJavaBytes

    override def read(slice: Slice[java.lang.Byte]): Optional[java.lang.String] =
      if (slice.isEmpty)
        Optional.empty()
      else
        Optional.of(slice.readString(StandardCharsets.UTF_8))
  }

  def byteSliceSerializer(): Serializer[Slice[java.lang.Byte]] = JavaByteSliceSerializer

  object JavaByteSliceSerializer extends Serializer[Slice[java.lang.Byte]] {
    override def write(data: Slice[java.lang.Byte]): Slice[java.lang.Byte] =
      data

    override def read(slice: Slice[java.lang.Byte]): Slice[java.lang.Byte] =
      slice
  }

  def javaByteSliceOptionalSerializer(): Serializer[Optional[Slice[java.lang.Byte]]] = JavaByteSliceOptionalSerializer

  object JavaByteSliceOptionalSerializer extends Serializer[Optional[Slice[java.lang.Byte]]] {
    override def write(data: Optional[Slice[java.lang.Byte]]): Slice[java.lang.Byte] =
      if (data.isPresent)
        data.get()
      else
        Slice.emptyJavaBytes

    override def read(slice: Slice[java.lang.Byte]): Optional[Slice[java.lang.Byte]] =
      if (slice.isEmpty)
        Optional.empty()
      else
        Optional.of(slice)
  }

  def javaByteArraySerializer(): Serializer[Array[java.lang.Byte]] = JavaByteArraySerializer

  object JavaByteArraySerializer extends Serializer[Array[java.lang.Byte]] {
    override def write(data: Array[java.lang.Byte]): Slice[java.lang.Byte] =
      Slice(data)

    override def read(slice: Slice[java.lang.Byte]): Array[java.lang.Byte] =
      slice.toArray
  }
}
