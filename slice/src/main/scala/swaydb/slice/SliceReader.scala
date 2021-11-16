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

import swaydb.slice.utils.ByteOps

import java.nio.file.Paths
import scala.reflect.ClassTag

/**
 * http://www.swaydb.io/slice/byte-slice
 */
case class SliceReader[@specialized(Byte) B](slice: Slice[B],
                                             private var position: Int = 0)(implicit val byteOps: ByteOps[B]) extends Reader[B] {

  def path = Paths.get(this.productPrefix)

  override def size: Int =
    slice.size

  def hasAtLeast(size: Int): Boolean =
    (slice.size - position) >= size

  def read(size: Int): Slice[B] =
    if (size <= 0) {
      Slice.empty
    } else {
      val bytes = slice.take(position, size)
      position += size
      bytes
    }

  def read(size: Int, blockSize: Int): SliceRO[B] = {
    implicit val classTag: ClassTag[B] = slice.classTag

    if (size <= 0) {
      Slice.empty[B]
    } else {
      val bytes = slice.take(position, size)
      position += size
      Slices(bytes.split(blockSize))
    }
  }

  def moveTo(newPosition: Int): SliceReader[B] = {
    position = newPosition max 0
    this
  }

  def get(): B = {
    val byte = slice get position
    position += 1
    byte
  }

  def hasMore: Boolean =
    position < slice.size

  override def getPosition: Int =
    position

  override def copy(): SliceReader[B] =
    SliceReader(slice)

  override def readRemaining(): Slice[B] =
    read(remaining)

  override def isFile: Boolean = false

}
