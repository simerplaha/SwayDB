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
 */

package swaydb.extensions

import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer
import swaydb.{IO, Prepare}

private[extensions] object Extend {

  /**
   * Wraps the input [[swaydb.Map]] instance and returns a new [[Map]] instance
   * which contains extended APIs to create nested Maps.
   */
  def apply[K, V](map: swaydb.Map[Key[K], Option[V], Nothing, IO.ApiIO])(implicit keySerializer: Serializer[K],
                                                                         optionValueSerializer: Serializer[Option[V]],
                                                                         keyOrder: KeyOrder[Slice[Byte]]): IO.ApiIO[Map[K, V]] = {
    implicit val mapKeySerializer = Key.serializer(keySerializer)

    implicit val valueSerializer = new Serializer[V] {
      override def write(data: V): Slice[Byte] =
        optionValueSerializer.write(Some(data))

      override def read(data: Slice[Byte]): V =
        optionValueSerializer.read(data) getOrElse {
          throw IO.throwable("optionValueSerializer returned None for value bytes.")
        }
    }
    val rootMapKey = Seq.empty[K]

    map.commit(
      Prepare.Put(Key.MapStart(rootMapKey), None),
      Prepare.Put(Key.MapEntriesStart(rootMapKey), None),
      Prepare.Put(Key.MapEntriesEnd(rootMapKey), None),
      Prepare.Put(Key.SubMapsStart(rootMapKey), None),
      Prepare.Put(Key.SubMapsEnd(rootMapKey), None),
      Prepare.Put(Key.MapEnd(rootMapKey), None)
    ) map {
      _ =>
        Map[K, V](
          map = map,
          mapKey = rootMapKey
        )
    }
  }
}
