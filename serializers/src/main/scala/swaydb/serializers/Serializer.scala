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
  def toOption[A](serializer: Serializer[A]) =
    new Serializer[Option[A]] {
      override def write(data: Option[A]): Slice[Byte] =
        data match {
          case Some(value) =>
            serializer.write(value)

          case None =>
            Slice.emptyBytes
        }

      override def read(data: Slice[Byte]): Option[A] =
        if (data.isEmpty)
          None
        else
          Some(serializer.read(data))
    }
}

trait Serializer[A] {
  def write(data: A): Slice[Byte]

  def read(data: Slice[Byte]): A
}
