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

package swaydb.multimap

import swaydb.core.util.Bytes
import swaydb.serializers.Serializer
import swaydb.slice.Slice

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
