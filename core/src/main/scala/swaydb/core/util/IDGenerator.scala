/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

import java.util.concurrent.atomic.AtomicLong

private[core] object IDGenerator {

  @inline final def apply(initial: Long = 0) = new IDGenerator(initial)

  @inline final def segmentId(id: Long): String =
    s"$id.${Extension.Seg.toString}"
}

private[core] class IDGenerator(initial: Long) {
  private val atomicID = new AtomicLong(initial)

  def nextID: Long =
    atomicID.incrementAndGet()

  def currentId: Long =
    atomicID.get()

  def nextSegmentID: String =
    s"$nextID.${Extension.Seg.toString}"
}

private[core] object BlockCacheFileIDGenerator {
  private val atomicID = new AtomicLong(0)

  def nextID: Long =
    atomicID.incrementAndGet()
}