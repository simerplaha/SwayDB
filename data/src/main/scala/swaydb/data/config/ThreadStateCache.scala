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

package swaydb.data.config

/**
 * Each thread is assigned a state. This config indicates if
 * that state should be limited or unlimited.
 *
 * A single thread can spawn reads of over 100s of segments.
 * For eg: performing forward and reverse iterations over millions of keys
 * could spread over multiple segments. These iterators cannot use bloomFilter since
 * bloomFilters only do exists check on a key. This states are used for skipping
 * reading a segment if it's not required.
 */
sealed trait ThreadStateCache
object ThreadStateCache {

  case class Limit(hashMapMaxSize: Int,
                   maxProbe: Int) extends ThreadStateCache

  case object NoLimit extends ThreadStateCache

  /**
   * Disabling ThreadState can be used if your database configuration
   * allows for perfect HashIndexes and if you do not use iterations.
   * Otherwise disabling [[ThreadStateCache]] can have noticable performance impact.
   */
  case object Disable extends ThreadStateCache
}
