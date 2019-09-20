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

package swaydb.core.io.reader

import java.nio.file.Paths

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.data.slice.{ReaderBase, Slice}

private[core] class GroupReader(decompressedValuesSize: Int,
                                startIndexOffset: Int,
                                endIndexOffset: Int,
                                valuesDecompressor: () => IO[swaydb.Error.Segment, ReaderBase[swaydb.Error.Segment]],
                                indexReader: ReaderBase[swaydb.Error.Segment]) extends ReaderBase[swaydb.Error.Segment] with LazyLogging {

  private var position: Int = 0

  def path = Paths.get(this.getClass.getSimpleName)

  override def size: IO[swaydb.Error.Segment, Long] =
    indexReader
      .size
      .map(_ + decompressedValuesSize)

  def moveTo(newPosition: Long): ReaderBase[swaydb.Error.Segment] = {
    position = newPosition.toInt max 0
    this
  }

  def hasMore: IO[swaydb.Error.Segment, Boolean] =
    size.map(position < _)

  def hasAtLeast(atLeastSize: Long): IO[swaydb.Error.Segment, Boolean] =
    size
      .map {
        size =>
          (size - position) >= atLeastSize
      }

  override def copy(): ReaderBase[swaydb.Error.Segment] =
    new GroupReader(
      decompressedValuesSize = decompressedValuesSize,
      startIndexOffset = startIndexOffset,
      endIndexOffset = endIndexOffset,
      valuesDecompressor = valuesDecompressor,
      indexReader = indexReader.copy()
    )

  override def getPosition: Int =
    position

  override def get(): IO[swaydb.Error.Segment, Int] =
    if (position >= startIndexOffset) {
      indexReader
        .moveTo(position - startIndexOffset)
        .get()
        .onRightSideEffect {
          _ =>
            position += 1
        }
    }
    else
      valuesDecompressor()
        .flatMap {
          reader =>
            reader
              .moveTo(position)
              .get()
              .onRightSideEffect {
                _ =>
                  position += 1
              }
        }

  override def read(size: Int) =
    if (position >= startIndexOffset)
      indexReader
        .moveTo(position - startIndexOffset)
        .read(size)
        .onRightSideEffect {
          _ =>
            position += size
        }
    else
      valuesDecompressor()
        .flatMap {
          reader =>
            reader
              .moveTo(position)
              .read(size)
              .onRightSideEffect {
                _ =>
                  position += size
              }
        }

  override def isFile: Boolean = false

  override def readRemaining(): IO[swaydb.Error.Segment, Slice[Byte]] =
    IO.failed(new IllegalStateException(s"Function readRemaining() on ${this.getClass.getSimpleName} is not supported!"))
}
