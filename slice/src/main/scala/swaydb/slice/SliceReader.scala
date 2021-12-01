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

package swaydb.slice

import java.nio.file.Paths

/**
 * http://www.swaydb.io/slice/byte-slice
 */
case class SliceReader(slice: Slice[Byte],
                       private var position: Int = 0) extends Reader {

  def path = Paths.get(this.productPrefix)

  override def size: Int =
    slice.size

  def hasAtLeast(size: Int): Boolean =
    (slice.size - position) >= size

  def read(size: Int): Slice[Byte] =
    if (size <= 0) {
      Slice.empty
    } else {
      val bytes = slice.take(position, size)
      position += size
      bytes
    }

  def read(size: Int, blockSize: Int): SliceRO[Byte] =
    if (size <= 0) {
      Slice.emptyBytes
    } else {
      val bytes = slice.take(position, size)
      position += size
      Slices(bytes.split(blockSize))
    }

  def moveTo(newPosition: Int): SliceReader = {
    position = newPosition max 0
    this
  }

  def get(): Byte = {
    val byte = slice get position
    position += 1
    byte
  }

  def hasMore: Boolean =
    position < slice.size

  override def getPosition: Int =
    position

  override def copy(): SliceReader =
    SliceReader(slice)

  override def readRemaining(): Slice[Byte] =
    read(remaining)

  override def isFile: Boolean = false

}
