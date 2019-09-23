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

import com.typesafe.scalalogging.LazyLogging
import swaydb.{Error, IO}
import swaydb.core.io.file.DBFile
import swaydb.data.slice.{Reader, Slice}

private[core] class FileReader(val file: DBFile) extends Reader[swaydb.Error.Segment] with LazyLogging {

  private var position: Int = 0

  def isLoaded: IO[swaydb.Error.IO, Boolean] =
    file.isLoaded

  override def size: IO[swaydb.Error.IO, Long] =
    file.fileSize

  def moveTo(newPosition: Long): FileReader = {
    position = newPosition.toInt max 0
    this
  }

  def hasMore: IO[swaydb.Error.IO, Boolean] =
    size.map(position < _)

  def hasAtLeast(size: Long): IO[swaydb.Error.IO, Boolean] =
    file.fileSize map {
      fileSize =>
        (fileSize - position) >= size
    }

  override def copy(): FileReader =
    new FileReader(file)

  override def getPosition: Int = position

  override def get() =
    (file get position) map {
      byte =>
        position += 1
        byte
    }

  override def read(size: Int) =
    if (size == 0)
      IO.emptyBytes
    else
      file.read(position, size) map {
        bytes =>
          position += size
          bytes
      }

  def path =
    file.path

  override def readRemaining(): IO[swaydb.Error.Segment, Slice[Byte]] =
    remaining flatMap read

  final override val isFile: Boolean =
    true

  override def readBlock(position: Int): Option[IO[Error.IO, Slice[Byte]]] =
    file.readBlock(position)
}