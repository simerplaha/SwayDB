/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.core.io.file

import swaydb.core.sweeper.FileSweeperItem
import swaydb.data.slice.Slice

import java.nio.channels.WritableByteChannel
import java.nio.file.Path

private[file] trait DBFileType extends FileSweeperItem {

  val path: Path

  private[file] def writeableChannel: WritableByteChannel

  def delete(): Unit

  def close(): Unit

  def isMemoryMapped: Boolean

  def isLoaded: Boolean

  def isOpen: Boolean

  def isFull: Boolean

  def forceSave(): Unit

  def readAll: Slice[Byte]

  def size: Long

  def append(slice: Slice[Byte]): Unit

  def append(slice: Iterable[Slice[Byte]]): Unit

  def transfer(position: Int, count: Int, transferTo: DBFileType): Int

  def read(position: Int, size: Int): Slice[Byte]

  def get(position: Int): Byte
}
