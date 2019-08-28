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
import swaydb.Error.Segment.ExceptionHandler
import swaydb.IO
import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.data.slice.{Reader, ReaderBase, Slice}

/**
 * Defers [[ReaderBase]] related operations to [[BlockReader]].
 */
private[block] trait BlockReaderBase extends ReaderBase[swaydb.Error.Segment] with LazyLogging {

  private[reader] val reader: Reader[swaydb.Error.Segment]

  override val isFile: Boolean = reader.isFile

  def offset: BlockOffset

  def path = reader.path

  def state: BlockReader.State

  override def size: IO[swaydb.Error.Segment, Long] =
    IO(state.offset.size)

  def hasMore: IO[swaydb.Error.Segment, Boolean] =
    IO(state.hasMore)

  def hasAtLeast(atLeastSize: Long): IO[swaydb.Error.Segment, Boolean] =
    IO(state.hasAtLeast(atLeastSize))

  override def getPosition: Int =
    state.position

  override def get(): IO[swaydb.Error.Segment, Int] =
    BlockReader get state

  override def read(size: Int): IO[swaydb.Error.Segment, Slice[Byte]] =
    BlockReader.read(size, state)

  def readFullBlock(): IO[swaydb.Error.Segment, Slice[Byte]] =
    BlockReader readFullBlock state

  def readFullBlockOrNone(): IO[swaydb.Error.Segment, Option[Slice[Byte]]] =
    if (offset.size == 0)
      IO.none
    else
      readFullBlock().map(Some(_))

  override def readRemaining(): IO[swaydb.Error.Segment, Slice[Byte]] =
    read(state.remaining)
}
