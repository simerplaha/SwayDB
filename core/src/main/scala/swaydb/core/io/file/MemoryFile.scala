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

package swaydb.core.io.file

import com.typesafe.scalalogging.LazyLogging
import java.nio.file.Path
import swaydb.data.IO
import swaydb.data.slice.Slice

private[file] object MemoryFile {
  def apply(path: Path, bytes: Slice[Byte]): DBFileType =
    new MemoryFile(path, bytes)
}

private[file] class MemoryFile(val path: Path,
                               private var bytes: Slice[Byte]) extends LazyLogging with DBFileType {

  override def close(): IO[Unit] =
    IO.unit

  override def append(slice: Slice[Byte]): IO[Unit] =
    IO.Failure(new UnsupportedOperationException("Memory files are immutable. Cannot append."))

  override def read(position: Int, size: Int): IO[Slice[Byte]] =
    IO(bytes.slice(position, position + size - 1))

  override def get(position: Int): IO[Byte] =
    IO(bytes.get(position))

  override def readAll: IO[Slice[Byte]] =
    IO(bytes)

  override def fileSize: IO[Long] =
    IO(bytes.size)

  override def isMemoryMapped: IO[Boolean] =
    IO.Success(false)

  override def isLoaded: IO[Boolean] =
    IO.Success(true)

  override def isOpen: Boolean =
    true

  override def isFull: IO[Boolean] =
    IO.Success(true)

  override def memory: Boolean = true

  override def delete(): IO[Unit] =
    close map {
      _ =>
        //null bytes for GC
        bytes = null
    }

  override def forceSave(): IO[Unit] =
    IO.unit
}