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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.reader

import com.typesafe.scalalogging.LazyLogging
import swaydb.IO
import swaydb.core.io.file.BlockCacheSource
import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.data.slice.{Reader, ReaderBase, Slice, SliceOption}

/**
 * Defers [[ReaderBase]] related operations to [[BlockReader]].
 */
private[block] trait BlockReaderBase extends ReaderBase[Byte] with LazyLogging {

  private[reader] val reader: Reader[Byte]

  def offset: BlockOffset

  def path = reader.path

  private var position: Int = 0

  override val isFile: Boolean =
    reader.isFile

  override def remaining: Long =
    offset.size - position

  def moveTo(position: Int) = {
    this.position = position
    this
  }

  override def hasMore: Boolean =
    hasAtLeast(1)

  override def hasAtLeast(atLeastSize: Long): Boolean =
    hasAtLeast(position, atLeastSize)

  def hasAtLeast(fromPosition: Long, atLeastSize: Long): Boolean =
    (offset.size - fromPosition) >= atLeastSize

  override def size: Long =
    offset.size

  override def getPosition: Int =
    position

  override def get(): Byte =
    if (hasMore) {
      val byte =
        reader
          .moveTo(offset.start + position)
          .get()

      position += 1
      byte
    } else {
      throw IO.throwable(s"Has no more bytes. Position: $position")
    }

  override def read(size: Int): Slice[Byte] = {
    val remaining = this.remaining.toInt
    if (remaining <= 0) {
      Slice.emptyBytes
    } else {
      val bytesToRead = size min remaining

      val bytes =
        reader
          .moveTo(offset.start + position)
          .read(bytesToRead)

      position += bytesToRead
      bytes
    }
  }

  def readFullBlock(): Slice[Byte] =
    reader
      .moveTo(offset.start)
      .read(offset.size)

  def readFullBlockOrNone(): SliceOption[Byte] =
    if (offset.size == 0)
      Slice.Null
    else
      readFullBlock()

  override def readRemaining(): Slice[Byte] =
    read(remaining)
}
