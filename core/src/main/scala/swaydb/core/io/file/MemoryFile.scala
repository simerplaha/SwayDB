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

package swaydb.core.io.file

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.ErrorHandler.Throwable
import swaydb.IO
import swaydb.data.slice.Slice

private[file] object MemoryFile {
  def apply(path: Path, bytes: Slice[Byte]): DBFileType =
    new MemoryFile(
      path = path,
      bytes = bytes
    )
}

private[file] class MemoryFile(val path: Path,
                               private var bytes: Slice[Byte]) extends LazyLogging with DBFileType {

  override def close(): IO[swaydb.Error.IO, Unit] =
    IO.unit

  override def append(slice: Slice[Byte]): IO[swaydb.Error.IO, Unit] =
    IO.failed[swaydb.Error.IO, Unit](new UnsupportedOperationException("Memory files are immutable. Cannot append."))

  override def append(slice: Iterable[Slice[Byte]]): IO[swaydb.Error.IO, Unit] =
    IO.failed[swaydb.Error.IO, Unit](new UnsupportedOperationException("Memory files are immutable. Cannot append."))

  override def read(position: Int, size: Int): IO[swaydb.Error.IO, Slice[Byte]] =
    IO(bytes.slice(position, position + size - 1))

  override def get(position: Int): IO[swaydb.Error.IO, Byte] =
    IO(bytes.get(position))

  override def readAll: IO[swaydb.Error.IO, Slice[Byte]] =
    IO(bytes)

  override def fileSize: IO[swaydb.Error.IO, Long] =
    IO(bytes.size)

  override def isMemoryMapped: IO[swaydb.Error.IO, Boolean] =
    IO.`false`

  override def isLoaded: IO[swaydb.Error.IO, Boolean] =
    IO.`true`

  override def isOpen: Boolean =
    true

  override def isFull: IO[swaydb.Error.IO, Boolean] =
    IO.`true`

  override def memory: Boolean = true

  override def delete(): IO[swaydb.Error.IO, Unit] =
    close map {
      _ =>
        //null bytes for GC
        bytes = null
    }

  override def forceSave(): IO[swaydb.Error.IO, Unit] =
    IO.unit
}