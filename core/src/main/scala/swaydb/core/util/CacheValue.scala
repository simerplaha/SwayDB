/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.core.util

import swaydb.data.IO


object CacheValue {
  def apply[T](fetch: => IO[T]): CacheValue[T] =
    new CacheValue(fetch)
}

/**
  * Caches a value on read. Used for IO operations where the output does not change.
  * For example: A file's size.
  */
class CacheValue[T](init: => IO[T]) {

  @volatile private var cacheValue: Option[IO[T]] = None

  def get: IO[T] =
    cacheValue getOrElse {
      init map {
        success =>
          cacheValue = Some(IO.Success(success))
          success
      }
    }

  def isCached: Boolean =
    cacheValue.isDefined

  def clear() =
    cacheValue = None
}
