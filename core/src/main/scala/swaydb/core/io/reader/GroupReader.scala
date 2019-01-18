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

package swaydb.core.io.reader

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.slice.{Reader, Slice}

import scala.util.{Failure, Try}

private[core] class GroupReader(decompressedValuesSize: Int,
                                startIndexOffset: Int,
                                endIndexOffset: Int,
                                valuesDecompressor: () => Try[Reader],
                                indexReader: Reader) extends Reader with LazyLogging {

  private var position: Int = 0

  override def size: Try[Long] =
    indexReader.size map (_ + decompressedValuesSize)

  def moveTo(newPosition: Long): Reader = {
    position = newPosition.toInt
    this
  }

  def hasMore: Try[Boolean] =
    size.map(position <= _)

  def hasAtLeast(atLeastSize: Long): Try[Boolean] =
    size map {
      size =>
        (size - position) >= atLeastSize
    }

  override def copy(): GroupReader =
    new GroupReader(
      decompressedValuesSize = decompressedValuesSize,
      startIndexOffset = startIndexOffset,
      endIndexOffset = endIndexOffset,
      valuesDecompressor = valuesDecompressor,
      indexReader = indexReader.copy()
    )

  override def getPosition: Int =
    position

  override def get(): Try[Int] =
    if (position >= startIndexOffset) {
      indexReader.moveTo(position - startIndexOffset).get() map {
        byte =>
          position += 1
          byte
      }
    }
    else
      valuesDecompressor() flatMap {
        reader =>
          reader.moveTo(position).get() map {
            byte =>
              position += 1
              byte
          }
      }

  override def read(size: Int) =
    if (position >= startIndexOffset)
      indexReader.moveTo(position - startIndexOffset).read(size) map {
        bytes =>
          position += size
          bytes
      }
    else
      valuesDecompressor() flatMap {
        reader =>
          reader.moveTo(position).read(size) map {
            bytes =>
              position += size
              bytes
          }
      }

  override def readRemaining(): Try[Slice[Byte]] =
    Failure(new NotImplementedError(s"Function readRemaining() on ${this.getClass.getSimpleName} is not supported!"))
}
