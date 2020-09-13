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

import swaydb.IO
import swaydb.core.segment.format.a.block.BlockOffset
import swaydb.data.slice.{Reader, Slice}

private[reader] object BlockReader {

  def apply(offset: BlockOffset,
            reader: Reader[Byte]): BlockReader.State =
    new State(
      offset = offset,
      position = 0,
      reader = reader
    )

  class State(val offset: BlockOffset,
              val reader: Reader[Byte],
              var position: Int) {

    def remaining: Int =
      offset.size - position

    def moveTo(position: Int) =
      this.position = position

    def hasMore: Boolean =
      hasAtLeast(1)

    def hasAtLeast(atLeastSize: Long): Boolean =
      hasAtLeast(position, atLeastSize)

    def hasAtLeast(fromPosition: Long, atLeastSize: Long): Boolean =
      (offset.size - fromPosition) >= atLeastSize
  }

  def get(state: State): Byte =
    if (state.hasMore) {
      val byte =
        state.
          reader
          .moveTo(state.offset.start + state.position)
          .get()

      state.position += 1
      byte
    } else {
      throw IO.throwable(s"Has no more bytes. Position: ${state.position}")
    }

  def read(size: Int, state: State): Slice[Byte] = {
    val remaining = state.remaining
    if (remaining <= 0) {
      Slice.emptyBytes
    } else {
      val bytesToRead = size min remaining

      val bytes =
        state
          .reader
          .moveTo(state.offset.start + state.position)
          .read(bytesToRead)

      state.position += bytesToRead
      bytes
    }
  }

  def readFullBlock(state: State): Slice[Byte] =
    state.
      reader
      .moveTo(state.offset.start)
      .read(state.offset.size)
}
