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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.slice

import java.nio.file.Paths

import swaydb.data.util.ByteOps

/**
 * http://www.swaydb.io/slice/byte-slice
 */
case class SliceReader[B](slice: Slice[B],
                          private var position: Int = 0)(implicit val byteOps: ByteOps[B]) extends Reader[B] {

  def path = Paths.get(this.productPrefix)

  override def size: Long =
    slice.size

  def hasAtLeast(size: Long): Boolean =
    (slice.size - position) >= size

  def read(size: Int): Slice[B] =
    if (size <= 0) {
      Slice.empty
    } else {
      val bytes = slice.take(position, size)
      position += size
      bytes
    }

  def moveTo(newPosition: Long): SliceReader[B] = {
    position = newPosition.toInt max 0
    this
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

  override def readUnsignedInt(): Int =
    byteOps.readUnsignedInt(this)

  override def readUnsignedIntWithByteSize(): (Int, Int) =
    byteOps.readUnsignedIntWithByteSize(this)

  def hasMore =
    position < slice.size

  override def getPosition: Int =
    position

  override def copy(): SliceReader[B] =
    SliceReader(slice)

  override def readRemaining(): Slice[B] =
    read(remaining)

  override def isFile: Boolean = false

}
