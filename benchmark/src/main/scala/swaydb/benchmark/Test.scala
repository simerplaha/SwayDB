/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.benchmark

import java.nio.file.Path

import swaydb._
import swaydb.data.config.MMAP
import swaydb.data.slice.Slice
import swaydb.serializers.Default.SliceSerializer
import swaydb.serializers.Default.SliceOptionSerializer

sealed trait Test {
  val db: swaydb.Map[Slice[Byte], Option[Slice[Byte]]]

  def randomWrite: Boolean

  def randomRead: Boolean

  def forwardIteration: Boolean

  def reverseIteration: Boolean

  def keyValueCount: Long

  def map: Boolean
}

case class MemoryTest(keyValueCount: Long,
                      randomWrite: Boolean,
                      randomRead: Boolean,
                      forwardIteration: Boolean,
                      reverseIteration: Boolean,
                      map: Boolean) extends Test {
  override val db = memory.Map[Slice[Byte], Option[Slice[Byte]]]().get
}

case class PersistentTest(dir: Path,
                          mmap: Boolean,
                          keyValueCount: Long,
                          randomWrite: Boolean,
                          randomRead: Boolean,
                          forwardIteration: Boolean,
                          reverseIteration: Boolean,
                          map: Boolean) extends Test {
  override val db =
    if (mmap)
      persistent.Map[Slice[Byte], Option[Slice[Byte]]](dir = dir).get
    else
      persistent.Map[Slice[Byte], Option[Slice[Byte]]](dir = dir, mmapMaps = false, mmapAppendix = false, mmapSegments = MMAP.Disabled).get
}