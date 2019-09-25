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

package swaydb.data.slice

import java.nio.file.Paths

/**
 * http://www.swaydb.io/slice/byte-slice
 */
private[swaydb] case class SliceReader(slice: Slice[Byte],
                                       private var position: Int = 0) extends Reader {

  def path = Paths.get(this.getClass.getSimpleName)

  override val size: Long =
    slice.size.toLong

  def hasAtLeast(size: Long): Boolean =
    (slice.size - position) >= size

  def read(size: Int): Slice[Byte] = {
    if (size == 0)
      Slice.emptyBytes
    else {
      val bytes = slice.take(position, size)
      position += size
      bytes
    }
  }

  def moveTo(newPosition: Long): SliceReader = {
    position = newPosition.toInt max 0
    this
  }

  def moveTo(newPosition: Int): SliceReader = {
    position = newPosition max 0
    this
  }

  def get() = {
    val byte = slice get position
    position += 1
    byte
  }

  def hasMore =
    position < slice.size

  override def getPosition: Int =
    position

  override def copy(): SliceReader =
    SliceReader(slice)

  override def readRemaining(): Slice[Byte] =
    read(remaining)

  override def isFile: Boolean = false
}