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
import swaydb.Error.Segment.ErrorHandler
import swaydb.core.io.file.BlockCache
import swaydb.core.io.reader.FileReader
import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.data.slice.{Reader, Slice}
import swaydb.{Error, IO}

private[reader] object BlockReader extends LazyLogging {

  def apply(offset: BlockOffset,
            reader: Reader[swaydb.Error.Segment]): BlockReader.State =
    new State(
      offset = offset,
      position = 0,
      previousReadEndPosition = 0,
      isSequentialRead = true,
      reader = reader
    )

  class State(val offset: BlockOffset,
              val reader: Reader[swaydb.Error.Segment],
              var isSequentialRead: Boolean, //var that stores the guess if the previous read was sequential.
              var position: Int,
              var previousReadEndPosition: Int) {

    val isFile = reader.isFile

    val size = offset.size

    def updatePreviousEndPosition(): Unit =
      previousReadEndPosition = position - 1

    def remaining: Int =
      size - position

    def moveTo(position: Int) =
      this.position = position

    def hasMore: Boolean =
      hasAtLeast(1)

    def hasAtLeast(atLeastSize: Long): Boolean =
      hasAtLeast(position, atLeastSize)

    def hasAtLeast(fromPosition: Long, atLeastSize: Long): Boolean =
      (size - fromPosition) >= atLeastSize
  }

  def isSequentialRead(state: State): Boolean = {
    val isSeq =
      state.previousReadEndPosition == 0 || //if this is the initial read.
        state.position - state.previousReadEndPosition <= 1 //if position is continuation of previous read's end position
    state.isSequentialRead = isSeq
    isSeq
  }

  def get(state: State): IO[swaydb.Error.Segment, Int] =
    if (state.isFile)
      read(1, state).map(_.head)
    else if (state.hasMore)
      state.
        reader
        .moveTo(state.offset.start + state.position)
        .get()
        .map {
          byte =>
            state.position += 1
            byte
        }
    else
      IO.Failure(swaydb.Error.Fatal(s"Has no more bytes. Position: ${state.position}"))

  def read(size: Int, state: State): IO[swaydb.Error.Segment, Slice[Byte]] = {
    val remaining = state.remaining
    if (remaining <= 0) {
      IO.emptyBytes
    } else {
      val bytesToRead = size min remaining
      state
        .reader
        .moveTo(state.offset.start + state.position)
        .read(bytesToRead)
        .map {
          bytes =>
            state.position += bytesToRead
            state.updatePreviousEndPosition()
            bytes
        }
    }
  }

  def readFullBlock(state: State): IO[swaydb.Error.Segment, Slice[Byte]] =
    state.
      reader
      .moveTo(state.offset.start)
      .read(state.offset.size)
}
