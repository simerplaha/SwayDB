/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.core.io.file

import swaydb.core.sweeper.FileSweeperItem
import swaydb.data.slice.{Slice, SliceRO}

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

  def size: Int

  def append(slice: Slice[Byte]): Unit

  def append(slice: Iterable[Slice[Byte]]): Unit

  def transfer(position: Int, count: Int, transferTo: DBFileType): Int

  def read(position: Int, size: Int): Slice[Byte]

  def read(position: Int, size: Int, blockSize: Int): SliceRO[Byte]

  def get(position: Int): Byte
}
