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

import swaydb.IO
import swaydb.data.slice.Slice

private[file] trait DBFileType {

  def path: Path

  def delete(): IO[Unit]

  def close(): IO[Unit]

  def append(slice: Slice[Byte]): IO[Unit]

  def append(slice: Iterable[Slice[Byte]]): IO[Unit]

  def read(position: Int, size: Int): IO[Slice[Byte]]

  def get(position: Int): IO[Byte]

  def readAll: IO[Slice[Byte]]

  def fileSize: IO[Long]

  def isMemoryMapped: IO[Boolean]

  def isLoaded: IO[Boolean]

  def isOpen: Boolean

  def isFull: IO[Boolean]

  def memory: Boolean

  def persistent: Boolean =
    !memory

  def forceSave(): IO[Unit]
}
