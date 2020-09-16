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

package swaydb.serializers

import swaydb.data.slice.Slice

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
              val slice = Slice.of[Byte](valueBytes.size + 1)
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
