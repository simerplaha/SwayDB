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

package swaydb.data.config

import swaydb.utils
import swaydb.utils.OperatingSystem

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
   *                         This configurations is required for windows. Use [[OperatingSystem.isWindows]]
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
   *                         This configurations is required for windows. Use [[utils.OperatingSystem.isWindows]]
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
