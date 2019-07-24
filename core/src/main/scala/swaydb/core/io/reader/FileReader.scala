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
import swaydb.IO
import swaydb.core.io.file.DBFile
import swaydb.data.io.Core
import swaydb.data.slice.{Reader, Slice}

private[core] class FileReader(file: DBFile) extends Reader[Core.Error.Private] with LazyLogging {

  private var position: Int = 0

  def isLoaded: IO[Core.Error.Private, Boolean] =
    file.isLoaded

  override def size: IO[Core.Error.Private, Long] =
    file.fileSize

  def moveTo(newPosition: Long): Reader[Core.Error.Private] = {
    position = newPosition.toInt max 0
    this
  }

  def hasMore: IO[Core.Error.Private, Boolean] =
    size.map(position < _)

  def hasAtLeast(size: Long): IO[Core.Error.Private, Boolean] =
    file.fileSize map {
      fileSize =>
        (fileSize - position) >= size
    }

  override def copy(): Reader[Core.Error.Private] =
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

  override def readRemaining(): IO[Core.Error.Private, Slice[Byte]] =
    remaining flatMap read

  final override val isFile: Boolean =
    true
}