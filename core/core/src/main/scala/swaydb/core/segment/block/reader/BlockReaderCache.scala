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

package swaydb.core.segment.block.reader

import swaydb.slice.Slice

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
      state.bytes.take(fromIndex = position - state.fromOffset, count = size)
    else
      Slice.emptyBytes
}
