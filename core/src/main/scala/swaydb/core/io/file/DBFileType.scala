/*
 * Copyright (c) 2020 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

import swaydb.core.actor.FileSweeperItem
import swaydb.data.slice.Slice

private[file] trait DBFileType extends FileSweeperItem {

  val path: Path

  def blockCacheFileId: Long

  def delete(): Unit

  def close(): Unit

  def isMemoryMapped: Boolean

  def isLoaded: Boolean

  def isOpen: Boolean

  def isFull: Boolean

  def forceSave(): Unit

  def readAll: Slice[Byte]

  def fileSize: Long

  def append(slice: Slice[Byte]): Unit

  def append(slice: Iterable[Slice[Byte]]): Unit

  def read(position: Int, size: Int): Slice[Byte]

  def get(position: Int): Byte
}
