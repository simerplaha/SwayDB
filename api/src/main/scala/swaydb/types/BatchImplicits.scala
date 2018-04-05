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

package swaydb.types

import swaydb.Batch
import swaydb.data.request
import swaydb.serializers.{Serializer, _}

private[swaydb] object BatchImplicits {

  implicit def batchToRequest[K, V](batch: Batch[K, V])(implicit keySerializer: Serializer[K],
                                                        valueSerializer: Serializer[V]): request.Batch =
    batch match {
      case Batch.Put(key, value) =>
        request.Batch.Put(key, Some(value))

      case Batch.Update(from, to, value) =>
        request.Batch.UpdateRange(from, to, Some(value))

      case Batch.Add(key) =>
        request.Batch.Put(key, None)

      case Batch.Remove(key, None) =>
        request.Batch.Remove(key)

      case Batch.Remove(from, Some(to)) =>
        request.Batch.RemoveRange(from, to)
    }

  implicit def batchesToRequests[K, V](batches: Iterable[Batch[K, V]])(implicit keySerializer: Serializer[K],
                                                                       valueSerializer: Serializer[V]): Iterable[request.Batch] =
    batches.map(batch => batchToRequest(batch)(keySerializer, valueSerializer))

  implicit def batchesToRequests[T](batches: Iterable[Batch[T, Nothing]])(implicit serializer: Serializer[T]): Iterable[request.Batch] =
    batches.map(batch => batchToRequest(batch)(serializer, Default.UnitSerializer))
}
