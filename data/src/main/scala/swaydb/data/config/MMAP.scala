/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.data.config

/**
 * Configurations to enable or disable memory-mapping of all files.
 */
sealed trait MMAP {
  val mmapReads: Boolean
  val mmapWrites: Boolean
  val deleteAfterClean: Boolean

  def hasMMAP: Boolean =
    mmapReads || mmapWrites
}

object MMAP {

  /**
   * Configurations that can be applied to .seg files.
   */
  sealed trait Segment extends MMAP

  /**
   * Configurations that can be applied to .log files
   */
  sealed trait Map extends MMAP {
    def isMMAP: Boolean
  }

  def off(forceSave: ForceSave.ChannelFiles): MMAP.Off =
    Off(forceSave)

  def on(deleteAfterClean: Boolean, forceSave: ForceSave.MMAPFiles): MMAP.On =
    On(deleteAfterClean, forceSave)

  def readOnly(deleteAfterClean: Boolean): MMAP.ReadOnly =
    ReadOnly(deleteAfterClean)

  /**
   * Enables memory-mapped files for both reads and writes.
   *
   * @param deleteAfterClean If true deletes memory-mapped files only after they in-memory buffer is cleared.
   *                         This configurations is required for windows. Use [[swaydb.data.util.OperatingSystem.isWindows]]
   *                         to set this.
   * @param forceSave        Sets the configurations for force saving memory-mapped files.
   *                         See - https://github.com/simerplaha/SwayDB/issues/251.
   *
   */
  case class On(deleteAfterClean: Boolean,
                forceSave: ForceSave.MMAPFiles) extends MMAP.Segment with MMAP.Map {
    override val mmapReads: Boolean = true
    override val mmapWrites: Boolean = true
    override val isMMAP: Boolean = true

    def copyWithDeleteAfterClean(deleteAfterClean: Boolean): On =
      copy(deleteAfterClean = deleteAfterClean)

    def copyWithForceSave(forceSave: ForceSave.MMAPFiles): On =
      copy(forceSave = forceSave)
  }

  /**
   * Enables memory-mapped files for read only. This does not require force safe as
   *
   * @param deleteAfterClean If true deletes memory-mapped files only after they in-memory buffer is cleared.
   *                         This configurations is required for windows. Use [[swaydb.data.util.OperatingSystem.isWindows]]
   *                         to set this.
   */
  case class ReadOnly(deleteAfterClean: Boolean) extends MMAP.Segment {
    override val mmapReads: Boolean = true
    override val mmapWrites: Boolean = false
  }

  case class Off(forceSave: ForceSave.ChannelFiles) extends MMAP.Segment with MMAP.Map {
    override val mmapReads: Boolean = false
    override val mmapWrites: Boolean = false
    override val isMMAP: Boolean = false
    override val deleteAfterClean: Boolean = false
  }
}
