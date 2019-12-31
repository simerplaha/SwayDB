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

package swaydb.java.serializers

import swaydb.java.data.slice.{ByteSlice, Slice}
import swaydb.java.serializers.{Serializer => JavaSerializer}
import swaydb.serializers.{Serializer => ScalaSerializer}

object SerializerConverter {

  def toScala[T](javaSerializer: JavaSerializer[T]): ScalaSerializer[T] =
    new ScalaSerializer[T] {
      override def write(data: T): swaydb.data.slice.Slice[Byte] =
        swaydb.data.slice.Slice(javaSerializer.write(data))

      override def read(data: swaydb.data.slice.Slice[Byte]): T =
        javaSerializer.read(ByteSlice(Slice(data)))
    }

  def toJava[T](scalaSerializer: ScalaSerializer[T]): JavaSerializer[T] =
    new JavaSerializer[T] {
      override def write(data: T): Array[Byte] =
        scalaSerializer.write(data).toArray

      override def read(slice: ByteSlice): T =
        scalaSerializer.read(slice.innerSlice.asScala.asInstanceOf[swaydb.data.slice.Slice[Byte]])
    }
}
