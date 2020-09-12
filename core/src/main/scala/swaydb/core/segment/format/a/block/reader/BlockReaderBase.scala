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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core.segment.format.a.block.reader

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.data.slice.Slice._
import swaydb.data.slice.{Reader, ReaderBase, Slice, SliceOption}

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

  override def get(): Byte =
    BlockReader get state

  override def read(size: Int): Slice[Byte] =
    BlockReader.read(size, state)

  def readFullBlock(): Slice[Byte] =
    BlockReader readFullBlock state

  def readFullBlockOrNone(): SliceOption[Byte] =
    if (offset.size == 0)
      Slice.Null
    else
      readFullBlock()

  override def readRemaining(): Slice[Byte] =
    read(state.remaining)
}
