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

import swaydb.{ErrorHandler, IO}

/**
 * http://www.swaydb.io/slice/byte-slice
 */
private[swaydb] case class SliceReader[E >: swaydb.Error.IO : ErrorHandler](slice: Slice[Byte]) extends Reader[E] {

  private var position: Int = 0

  def path = Paths.get(this.getClass.getSimpleName)

  override val size: IO[E, Long] =
    IO(slice.size.toLong)

  def hasAtLeast(size: Long): IO[E, Boolean] =
    IO((slice.size - position) >= size)

  def read(size: Int): IO[E, Slice[Byte]] =
    IO {
      if (size == 0)
        Slice.emptyBytes
      else {
        val bytes = slice.take(position, size)
        position += size
        bytes
      }
    }

  def moveTo(newPosition: Long): SliceReader[E] = {
    position = newPosition.toInt max 0
    this
  }

  def get() =
    IO {
      val byte = slice get position
      position += 1
      byte
    }

  def hasMore =
    IO.Success(position < slice.size)

  override def getPosition: Int =
    position

  override def copy(): SliceReader[E] =
    SliceReader(slice)

  override def readRemaining(): IO[E, Slice[Byte]] =
    remaining flatMap read

  override def isFile: Boolean = false
}