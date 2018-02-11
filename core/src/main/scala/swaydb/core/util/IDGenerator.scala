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

package swaydb.core.util

import java.util.concurrent.atomic.AtomicLong

private[core] object IDGenerator {

  def apply(initial: Long = 0) = new IDGenerator(initial)

  def segmentId(id: Long): String =
    id + s".${Extension.Seg}"
}

private[core] class IDGenerator(initial: Long) {
  private val atomicID = new AtomicLong(initial)

  def nextID: Long =
    atomicID.incrementAndGet()

  def set(id: Long) =
    atomicID.set(id)

  def nextSegmentID: String =
    nextID + s".${Extension.Seg}"

  def nextMapID: String =
    nextID + s".${Extension.Log}"

}