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

package swaydb.serializers

import swaydb.slice.Slice

object Serializer {
  private val one = Slice(1.toByte)

  /**
   * Converts the serialiser to prefix Some values with 1 byte.
   *
   * SwayDB does not stores empty bytes. But for cases where there are nested null values
   * like Some(None) a marker is required to indicate the value is Some(None) instead of None.
   */
  def toNestedOption[A](serializer: Serializer[A]) =
    new Serializer[Option[A]] {

      override def write(data: Option[A]): Slice[Byte] =
        data match {
          case Some(value) =>
            //if value is defined serialise the value with prefix 1.byte indicating value exists.
            val valueBytes = serializer.write(value)
            if (valueBytes.isEmpty) {
              one
            } else {
              val slice = Slice.allocate[Byte](valueBytes.size + 1)
              slice add 1
              slice addAll valueBytes
            }

          case None =>
            Slice.emptyBytes
        }

      override def read(slice: Slice[Byte]): Option[A] =
        if (slice.isEmpty)
          None
        else
          Some(serializer.read(slice.dropHead()))
    }
}

trait Serializer[A] {
  def write(data: A): Slice[Byte]

  def read(slice: Slice[Byte]): A
}
