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

package swaydb.java.serializers

import swaydb.serializers.Default._
import swaydb.slice.Slice

import java.nio.charset.StandardCharsets
import java.util.Optional

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
