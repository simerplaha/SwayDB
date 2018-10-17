/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.extensions

import swaydb.{Batch, extensions}
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

import scala.util.Try

private[swaydb] object Extend {

  def apply[K, V](map: swaydb.Map[Key[K], Option[V]])(implicit keySerializer: Serializer[K],
                                                      optionValueSerializer: Serializer[Option[V]],
                                                      ordering: Ordering[Slice[Byte]]): Try[extensions.Map[K, V]] = {
    implicit val mapKeySerializer = Key.serializer(keySerializer)

    implicit val valueSerializer = new Serializer[V] {
      override def write(data: V): Slice[Byte] =
        optionValueSerializer.write(Some(data))

      override def read(data: Slice[Byte]): V =
        optionValueSerializer.read(data) getOrElse {
          throw new Exception("optionValueSerializer returned None for valid value.")
        }
    }
    val rootMapKey = Seq.empty[K]

    map.batch(
      Batch.Put(Key.Start(rootMapKey), None),
      Batch.Put(Key.EntriesStart(rootMapKey), None),
      Batch.Put(Key.EntriesEnd(rootMapKey), None),
      Batch.Put(Key.SubMapsStart(rootMapKey), None),
      Batch.Put(Key.SubMapsEnd(rootMapKey), None),
      Batch.Put(Key.End(rootMapKey), None)
    ) map {
      _ =>
        Map[K, V](
          map = map,
          mapKey = Seq.empty
        )
    }
  }
}