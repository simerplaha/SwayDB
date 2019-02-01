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

import java.nio.file.Path
import scala.util.Try
import swaydb.data.slice.Slice

private[file] trait DBFileType {

  def delete(): Try[Unit]

  def close(): Try[Unit]

  def append(slice: Slice[Byte]): Try[Unit]

  def read(position: Int, size: Int): Try[Slice[Byte]]

  def get(position: Int): Try[Byte]

  def readAll: Try[Slice[Byte]]

  def fileSize: Try[Long]

  def isMemoryMapped: Try[Boolean]

  def isLoaded: Try[Boolean]

  val path: Path

  def isOpen: Boolean

  def isFull: Try[Boolean]

  def memory: Boolean

  def persistent: Boolean =
    !memory

  def forceSave(): Try[Unit]

}
