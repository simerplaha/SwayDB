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
import swaydb.data.slice.Slice.Sliced
import swaydb.data.util.ByteOps._
import swaydb.data.util.JavaByteOps
import swaydb.serializers.Default._

object Default {

  implicit val byteOps = JavaByteOps

  def intSerializer(): Serializer[java.lang.Integer] = JavaIntSerializer

  object JavaIntSerializer extends Serializer[java.lang.Integer] {
    override def write(data: java.lang.Integer): Sliced[java.lang.Byte] =
      Slice.writeInt(data)

    override def read(data: Sliced[java.lang.Byte]): java.lang.Integer =
      data.cast.readInt()
  }

  def longSerializer(): Serializer[java.lang.Long] = JavaLongSerializer

  implicit object JavaLongSerializer extends Serializer[java.lang.Long] {
    override def write(data: java.lang.Long): Sliced[java.lang.Byte] =
      Slice.writeLong(data)

    override def read(data: Sliced[java.lang.Byte]): java.lang.Long =
      data.cast.readLong()
  }

  def charSerializer(): Serializer[java.lang.Character] = JavaCharSerializer

  implicit object JavaCharSerializer extends Serializer[java.lang.Character] {
    override def write(data: java.lang.Character): Sliced[java.lang.Byte] =
      CharSerializer.write(data).cast

    override def read(data: Sliced[java.lang.Byte]): java.lang.Character =
      CharSerializer.read(data.cast)
  }

  def doubleSerializer(): Serializer[java.lang.Double] = JavaDoubleSerializer

  implicit object JavaDoubleSerializer extends Serializer[java.lang.Double] {
    override def write(data: java.lang.Double): Sliced[java.lang.Byte] =
      DoubleSerializer.write(data).cast

    override def read(data: Sliced[java.lang.Byte]): java.lang.Double =
      data.toByteBufferWrap.getDouble
  }

  def floatSerializer(): Serializer[java.lang.Float] = JavaFloatSerializer

  implicit object JavaFloatSerializer extends Serializer[java.lang.Float] {
    override def write(data: java.lang.Float): Sliced[java.lang.Byte] =
      FloatSerializer.write(data).cast

    override def read(data: Sliced[java.lang.Byte]): java.lang.Float =
      FloatSerializer.read(data.cast)
  }

  def shortSerializer(): Serializer[java.lang.Short] = JavaShortSerializer

  implicit object JavaShortSerializer extends Serializer[java.lang.Short] {
    override def write(data: java.lang.Short): Sliced[java.lang.Byte] =
      ShortSerializer.write(data).cast

    override def read(data: Sliced[java.lang.Byte]): java.lang.Short =
      ShortSerializer.read(data.cast)
  }

  def stringSerializer(): Serializer[java.lang.String] = StringSerializer

  implicit object StringSerializer extends Serializer[java.lang.String] {
    override def write(data: java.lang.String): Sliced[java.lang.Byte] =
      Slice.writeString(data, StandardCharsets.UTF_8)

    override def read(data: Sliced[java.lang.Byte]): java.lang.String =
      data.cast.readString(StandardCharsets.UTF_8)
  }

  def optionalStringSerializer(): Serializer[Optional[java.lang.String]] =
    JavaOptionalStringSerializer

  implicit object JavaOptionalStringSerializer extends Serializer[Optional[java.lang.String]] {
    override def write(data: Optional[java.lang.String]): Sliced[java.lang.Byte] =
      data.map(data => Slice.writeString[java.lang.Byte](data, StandardCharsets.UTF_8)).orElseGet(() => Slice.emptyJavaBytes)

    override def read(data: Sliced[java.lang.Byte]): Optional[java.lang.String] =
      if (data.isEmpty)
        Optional.empty()
      else
        Optional.of(data.cast.readString(StandardCharsets.UTF_8))
  }

  def byteSliceSerializer(): Serializer[Sliced[java.lang.Byte]] = JavaByteSliceSerializer

  implicit object JavaByteSliceSerializer extends Serializer[Sliced[java.lang.Byte]] {
    override def write(data: Sliced[java.lang.Byte]): Sliced[java.lang.Byte] =
      data

    override def read(data: Sliced[java.lang.Byte]): Sliced[java.lang.Byte] =
      data
  }

  def javaByteSliceOptionalSerializer(): Serializer[Optional[Sliced[java.lang.Byte]]] = JavaByteSliceOptionalSerializer

  implicit object JavaByteSliceOptionalSerializer extends Serializer[Optional[Sliced[java.lang.Byte]]] {
    override def write(data: Optional[Sliced[java.lang.Byte]]): Sliced[java.lang.Byte] =
      data.orElseGet(() => Slice.emptyJavaBytes)

    override def read(data: Sliced[java.lang.Byte]): Optional[Sliced[java.lang.Byte]] =
      if (data.isEmpty)
        Optional.empty()
      else
        Optional.of(data)
  }

  def javaByteArraySerializer(): Serializer[Array[java.lang.Byte]] = JavaByteArraySerializer

  implicit object JavaByteArraySerializer extends Serializer[Array[java.lang.Byte]] {
    override def write(data: Array[java.lang.Byte]): Sliced[java.lang.Byte] =
      Slice(data)

    override def read(data: Sliced[java.lang.Byte]): Array[java.lang.Byte] =
      data.toArray
  }
}
