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

package swaydb.data.config

import swaydb.data.util.OperatingSystem

import scala.util.Random

sealed trait MMAP {
  val mmapReads: Boolean
  val mmapWrites: Boolean
  val deleteOnClean: Boolean
}

object MMAP {

  /**
   * This type is only for [[swaydb.core.map.Map]] type.
   */
  sealed trait Segment extends MMAP
  sealed trait Map extends MMAP {
    def isMMAP: Boolean
  }

  def enabled(deleteOnClean: Boolean): MMAP.Enabled =
    Enabled(deleteOnClean)

  case class Enabled(deleteOnClean: Boolean) extends MMAP.Segment with MMAP.Map {
    override val mmapReads: Boolean = true
    override val mmapWrites: Boolean = true
    override val isMMAP: Boolean = true
  }

  def readOnly(deleteOnClean: Boolean): MMAP.ReadOnly =
    ReadOnly(deleteOnClean)

  case class ReadOnly(deleteOnClean: Boolean) extends MMAP.Segment {
    override val mmapReads: Boolean = true
    override val mmapWrites: Boolean = false
  }

  def disabled(): MMAP.Disabled =
    Disabled

  sealed trait Disabled extends MMAP.Segment with MMAP.Map
  case object Disabled extends Disabled {
    override val mmapReads: Boolean = false
    override val mmapWrites: Boolean = false
    override val isMMAP: Boolean = false
    override val deleteOnClean: Boolean = false
  }

  def randomForSegment(): MMAP.Segment =
    if (Random.nextBoolean())
      MMAP.Enabled(OperatingSystem.get().isWindows)
    else if (Random.nextBoolean())
      MMAP.ReadOnly(OperatingSystem.get().isWindows)
    else
      MMAP.Disabled

  def randomForMap(): MMAP.Map =
    if (Random.nextBoolean())
      MMAP.Enabled(OperatingSystem.get().isWindows)
    else
      MMAP.Disabled
}
