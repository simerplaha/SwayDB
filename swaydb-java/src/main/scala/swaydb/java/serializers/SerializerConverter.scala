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

import swaydb.java.data.slice.ByteSlice
import swaydb.java.serializers.{Serializer => JavaSerializer}
import swaydb.serializers.{Serializer => ScalaSerializer}

object SerializerConverter {

  def toScala[T](javaSerializer: JavaSerializer[T]): ScalaSerializer[T] =
    new ScalaSerializer[T] {
      override def write(data: T): swaydb.data.slice.Slice[Byte] =
        javaSerializer.write(data).asScala.cast[Byte]

      override def read(data: swaydb.data.slice.Slice[Byte]): T =
        javaSerializer.read(ByteSlice(data.cast[java.lang.Byte]))
    }

  def toJava[T](scalaSerializer: ScalaSerializer[T]): JavaSerializer[T] =
    new JavaSerializer[T] {
      override def write(data: T): ByteSlice =
        new ByteSlice(scalaSerializer.write(data).cast[java.lang.Byte])

      override def read(data: ByteSlice): T =
        scalaSerializer.read(data.asScala.cast[Byte])
    }
}
