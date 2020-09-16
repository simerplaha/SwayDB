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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.multimap

import swaydb.core.util.Bytes
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer
import swaydb.data.slice.Slice

private[swaydb] sealed trait MultiValue[+V]
private[swaydb] object MultiValue {

  private val two = Slice(2.toByte)

  def serialiser[A](serializer: Serializer[A]): Serializer[MultiValue[A]] =
    new Serializer[MultiValue[A]] {

      override def write(data: MultiValue[A]): Slice[Byte] =
        data match {
          case our: Our =>
            our match {
              case None =>
                Slice.emptyBytes

              case mapId: MapId =>
                val slice = Slice.of[Byte](1 + Bytes.sizeOfUnsignedLong(mapId.id))
                slice add 1.toByte
                slice addUnsignedLong mapId.id
            }

          case their: Their[A] =>
            val bytes = serializer.write(their.value)
            if (bytes.isEmpty) {
              two
            } else {
              val slice = Slice.of[Byte](1 + bytes.size)
              slice add 3.toByte
              slice addAll bytes
            }
        }

      override def read(slice: Slice[Byte]): MultiValue[A] =
        if (slice.isEmpty) {
          MultiValue.None
        } else if (slice.head == 1) {
          new MapId(slice.dropHead().readUnsignedLong())
        } else if (slice.head == 2) {
          val theirValue = serializer.read(Slice.emptyBytes)
          new Their(theirValue)
        } else if (slice.head == 3) {
          val theirBytes = slice.dropHead()
          val theirValue = serializer.read(theirBytes)
          new Their(theirValue)
        } else {
          throw new Exception(s"Invalid data id :${slice.head}")
        }
    }

  sealed trait Our extends MultiValue[Nothing]
  object None extends Our {
    override def toString: String =
      "MultiValue.None"
  }

  object MapId {
    @inline def apply(id: Long): MapId =
      new MapId(id)
  }

  class MapId(val id: Long) extends Our {
    override def equals(other: Any): Boolean =
      other match {
        case other: MapId =>
          other.id == id

        case _ =>
          false
      }

    override def hashCode(): Int =
      id.hashCode()

    override def toString: String =
      s"MultiValue.MapId($id)"
  }

  object Their {
    @inline def apply[V](value: V): Their[V] =
      new Their(value)
  }

  class Their[+V](val value: V) extends MultiValue[V] {
    override def equals(other: Any): Boolean =
      other match {
        case other: Their[_] =>
          value == other.value

        case _ =>
          false
      }

    override def hashCode(): Int =
      value.hashCode()

    override def toString: String =
      s"MultiValue.Their(${value.toString})"
  }
}
