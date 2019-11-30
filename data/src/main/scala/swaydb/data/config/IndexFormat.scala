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

package swaydb.data.config

sealed trait IndexFormat
object IndexFormat {
  /**
   * Stores a reference to the position of a the entire key-value entry within the sorted index.
   *
   * This configuration requires a maximum of 1 to 5 bytes and is space efficient but might be
   * slower then [[ReferenceKey]] and [[CopyKey]].
   */
  object ReferenceOffset extends IndexFormat

  /**
   * In addition to information stored by [[ReferenceOffset]] this also stores a reference to the position of a key within sorted index.
   *
   * Requires additional 2 to 10 bytes. This config might require more space but increases read performance since as it reduces the amount
   * parsed data to fetch the stored key there reduces CPU time.
   *
   * Faster than [[ReferenceOffset]] and slower than [[CopyKey]]
   */
  object ReferenceKey extends IndexFormat

  /**
   * In addition to information stored by [[ReferenceOffset]] this also stores a copy of the key within the index itself.
   *
   * In addition to space required by [[ReferenceKey]] this requires additional space to store the key.
   * This config increases read and compaction performance since as it reduces the amount
   * parsed data to fetch the stored key and also reduces CPU and IO.
   *
   * Fastest config.
   */
  object CopyKey extends IndexFormat
}
