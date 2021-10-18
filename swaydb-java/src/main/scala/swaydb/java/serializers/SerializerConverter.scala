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

import swaydb.data.slice.Slice
import swaydb.java.serializers.{Serializer => JavaSerializer}
import swaydb.serializers.{Serializer => ScalaSerializer}

object SerializerConverter {

  def toScala[T](javaSerializer: JavaSerializer[T]): ScalaSerializer[T] =
    new ScalaSerializer[T] {
      override def write(data: T): Slice[Byte] =
        javaSerializer.write(data).cast

      override def read(data: Slice[Byte]): T =
        javaSerializer.read(data.cast)
    }

  def toJava[T](scalaSerializer: ScalaSerializer[T]): JavaSerializer[T] =
    new JavaSerializer[T] {
      override def write(data: T): Slice[java.lang.Byte] =
        scalaSerializer.write(data).cast

      override def read(data: Slice[java.lang.Byte]): T =
        scalaSerializer.read(data.cast)
    }
}
