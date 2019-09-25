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

package swaydb.core.segment.format.a.block.reader

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.data.slice.{Reader, ReaderBase, Slice}

/**
 * Defers [[ReaderBase]] related operations to [[BlockReader]].
 */
private[block] trait BlockReaderBase extends ReaderBase with LazyLogging {

  private[reader] val reader: Reader

  override val isFile: Boolean = reader.isFile

  def offset: BlockOffset

  def path = reader.path

  def state: BlockReader.State

  override def size: Long =
    state.offset.size

  def hasMore: Boolean =
    state.hasMore

  def hasAtLeast(atLeastSize: Long): Boolean =
    state.hasAtLeast(atLeastSize)

  override def getPosition: Int =
    state.position

  override def get(): Int =
    BlockReader get state

  override def read(size: Int): Slice[Byte] =
    BlockReader.read(size, state)

  def readFullBlock(): Slice[Byte] =
    BlockReader readFullBlock state

  def readFullBlockOrNone(): Option[Slice[Byte]] =
    if (offset.size == 0)
      None
    else
      Some(readFullBlock())

  override def readRemaining(): Slice[Byte] =
    read(state.remaining)
}
