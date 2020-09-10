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

package swaydb.java.data.slice

import java.nio.file.Paths

case class SliceReader(asScala: swaydb.data.slice.SliceReader) {

  def path = Paths.get(this.getClass.getSimpleName)

  def size: Long =
    asScala.size

  def hasAtLeast(size: Long): Boolean =
    asScala.hasAtLeast(size)

  def read(size: Int): ByteSliceBuilder =
    ByteSliceBuilder(Slice(asScala.read(size)))

  def moveTo(newPosition: Long): SliceReader = {
    asScala.moveTo(newPosition)
    this
  }

  def moveTo(newPosition: Int): SliceReader = {
    asScala.moveTo(newPosition)
    this
  }

  def get() =
    asScala.get()

  def hasMore =
    asScala.hasMore

  def getPosition: Int =
    asScala.getPosition

  def copy(): SliceReader =
    SliceReader(asScala.copy())

  def readRemaining(): ByteSliceBuilder =
    ByteSliceBuilder(Slice(asScala.readRemaining()))
}
