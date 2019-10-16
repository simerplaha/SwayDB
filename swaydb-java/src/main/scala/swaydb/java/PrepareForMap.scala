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

package swaydb.java

import java.time.Duration
import java.util.Optional

import swaydb.java.Prepare.Map.{ApplyFunction, Put, Remove, Update}

object PrepareForMap {

  def put[K, V, F](key: K, value: V): Put[K, V, F] =
    Put(key, value, Optional.empty())

  def put[K, V, F](key: K, value: V, expireAfter: Duration): Put[K, V, F] =
    Put(key, value, Optional.of(expireAfter))

  def remove[K, V, F](key: K): Remove[K, V, F] =
    Remove(key, Optional.empty(), Optional.empty())

  def remove[K, V, F](fromKey: K, toKey: K): Remove[K, V, F] =
    Remove(fromKey, Optional.of(toKey), Optional.empty())

  def expire[K, V, F](key: K, after: Duration): Remove[K, V, F] =
    Remove(key, Optional.empty(), Optional.of(after))

  def expire[K, V, F](fromKey: K, toKey: K, after: Duration): Remove[K, V, F] =
    Remove(fromKey, Optional.of(toKey), Optional.of(after))

  def update[K, V, F](key: K, value: V): Update[K, V, F] =
    Update(key, Optional.empty(), value)

  def update[K, V, F](fromKey: K, toKey: K, value: V): Update[K, V, F] =
    Update(fromKey, Optional.of(toKey), value)

  def applyFunction[K, V, F](key: K, function: F): ApplyFunction[K, V, F] =
    ApplyFunction(key, Optional.empty(), function)

  def applyFunction[K, V, F](fromKey: K, toKey: K, function: F): ApplyFunction[K, V, F] =
    ApplyFunction(fromKey, Optional.of(toKey), function)

}
