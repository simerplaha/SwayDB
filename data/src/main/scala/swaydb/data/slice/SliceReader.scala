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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.data.slice

import scala.util.{Success, Try}

/**
  * http://www.swaydb.io/slice/byte-slice
  */
private[swaydb] case class SliceReader(slice: Slice[Byte]) extends Reader {

  private var position: Int = 0

  override val size: Try[Long] =
    Try(slice.size.toLong)

  def hasAtLeast(size: Long): Try[Boolean] =
    Try((slice.size - position) >= size)

  def read(size: Int): Try[Slice[Byte]] =
    Try {
      if (size == 0)
        Slice.emptyBytes
      else {
        val bytes = slice.slice(position, position + size - 1)
        position += size
        bytes
      }
    }

  def moveTo(newPosition: Long): Reader = {
    position = newPosition.toInt
    this
  }

  def get() =
    Try {
      val byte = slice get position
      position += 1
      byte
    }

  def hasMore =
    Success(position < slice.size)

  override def getPosition: Int =
    position

  override def copy(): Reader =
    SliceReader(slice)

  override def readRemaining(): Try[Slice[Byte]] =
    remaining flatMap read

}