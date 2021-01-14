/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
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
  def reference: IndexFormat.Reference = Reference
  sealed trait Reference extends IndexFormat
  object Reference extends Reference

  /**
   * In addition to information stored by [[Reference]] this also stores a copy of the key within the index itself.
   *
   * In addition to space required by [[ReferenceKey]] this requires additional space to store the key.
   * This config increases read and compaction performance since as it reduces the amount
   * parsed data to fetch the stored key and also reduces CPU and IO.
   *
   * Fastest config.
   */
  def copyKey: IndexFormat.CopyKey = CopyKey
  sealed trait CopyKey extends IndexFormat
  object CopyKey extends CopyKey
}
