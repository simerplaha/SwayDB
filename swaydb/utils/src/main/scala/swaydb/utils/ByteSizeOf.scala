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

package swaydb.utils

sealed trait ByteSizeOf[A] {
  def size: Int
}

private[swaydb] object ByteSizeOf {
  @inline val byte: Int = java.lang.Byte.BYTES
  @inline val short: Int = java.lang.Short.BYTES
  @inline val int: Int = java.lang.Integer.BYTES
  @inline val varInt: Int = int + 1 //5
  @inline val long: Int = java.lang.Long.BYTES
  @inline val varLong: Int = long + 2 //10
  @inline val boolean: Int = java.lang.Byte.BYTES
  @inline val char: Int = java.lang.Character.BYTES
  @inline val double: Int = java.lang.Double.BYTES
  @inline val float: Int = java.lang.Float.BYTES

  implicit object ByteSizeOfByte extends ByteSizeOf[Byte] {
    override def size: Int = ByteSizeOf.byte
  }

  implicit object ByteSizeOfJavaByte extends ByteSizeOf[java.lang.Byte] {
    override def size: Int = ByteSizeOf.byte
  }

  implicit object ByteSizeOfShort extends ByteSizeOf[Short] {
    override def size: Int = ByteSizeOf.short
  }

  implicit object ByteSizeOfInt extends ByteSizeOf[Int] {
    override def size: Int = ByteSizeOf.int
  }

  @inline def apply[A]()(implicit sizeOf: ByteSizeOf[A]): Int =
    sizeOf.size
}
