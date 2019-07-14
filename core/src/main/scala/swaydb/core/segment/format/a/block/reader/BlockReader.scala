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

package swaydb.core.segment.format.a.block.reader

import swaydb.core.segment.format.a.block.Block
import swaydb.data.IO
import swaydb.data.slice.{Reader, Slice}

protected abstract class BlockReader[B <: Block](reader: Reader,
                                                 val block: B) extends Reader {

  private var position: Int = 0

  override val size: IO[Long] =
    IO(block.offset.size)

  def moveTo(newPosition: Long): BlockReader[B] = {
    position = newPosition.toInt
    this
  }

  def hasMore: IO[Boolean] =
    hasAtLeast(1)

  def hasAtLeast(atLeastSize: Long): IO[Boolean] =
    hasAtLeast(position, atLeastSize)

  def hasAtLeast(fromPosition: Long, atLeastSize: Long): IO[Boolean] =
    size map {
      size =>
        (size - fromPosition) >= atLeastSize
    }

  override def getPosition: Int =
    position

  override def get(): IO[Int] =
    hasMore flatMap {
      hasMore =>
        if (hasMore)
          reader
            .moveTo(block.offset.start + position)
            .get()
            .map {
              got =>
                position += 1
                got
            }
        else
          IO.Failure(IO.Error.Fatal(s"Has no more bytes. Position: $getPosition"))
    }

  override def read(size: Int): IO[Slice[Byte]] =
    remaining flatMap {
      remaining =>
        val minimum = size min remaining.toInt
        reader
          .moveTo(block.offset.start + position)
          .read(minimum)
          .map {
            bytes =>
              position += minimum
              bytes
          }
    }

  def readAll(): IO[Slice[Byte]] =
    reader
      .moveTo(block.offset.start)
      .read(block.offset.size)

  override def readRemaining(): IO[Slice[Byte]] =
    remaining flatMap read
}
