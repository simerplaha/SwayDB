/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

import swaydb.data.slice.Slice

import scala.beans.BeanProperty

object BlockReaderCache {
  class State(@BeanProperty var fromOffset: Int,
              @BeanProperty var bytes: Slice[Byte]) {
    def toOffset = fromOffset + bytes.size - 1

    def size = bytes.size

    def isEmpty =
      size == 0
  }

  def set(position: Int, bytes: Slice[Byte], state: State): Unit = {
    state setFromOffset position
    state setBytes bytes
  }

  def init(position: Int, bytes: Slice[Byte]): State =
    new State(position, bytes)

  def read(position: Int, size: Int, state: State): Slice[Byte] =
    if (state.isEmpty)
      Slice.emptyBytes
    else if (position >= state.fromOffset && position <= state.toOffset)
      state.bytes.drop(position - state.fromOffset) take size
    else
      Slice.emptyBytes
}
