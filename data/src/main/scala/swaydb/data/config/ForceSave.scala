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
 * Config to set if forceSave should be applied to [[java.nio.MappedByteBuffer]] and [[java.nio.channels.FileChannel]].
 */
sealed trait ForceSave {
  def enableForReadOnlyMode: Boolean
  def enableBeforeCopy: Boolean
  def isDisabled: Boolean
  def enabledBeforeClose: Boolean
  def logBenchmark: Boolean
}
object ForceSave {

  /**
   * [[ForceSave]] configurations that can be applied to memory-mapped ([[java.nio.MappedByteBuffer]]) files only.
   */
  sealed trait MMAPFiles extends ForceSave {
    def enabledBeforeClean: Boolean
  }

  /**
   * [[ForceSave]] configurations that can be applied to [[java.nio.channels.FileChannel]] files only.
   */
  sealed trait ChannelFiles extends ForceSave

  /**
   * Disables force save for all cases.
   */
  sealed trait Off extends MMAPFiles with ChannelFiles
  case object Off extends Off {
    override val isDisabled: Boolean = true
    override val enableForReadOnlyMode: Boolean = false
    override val enabledBeforeClose: Boolean = false
    override val enabledBeforeClean: Boolean = false
    override val enableBeforeCopy: Boolean = false
    override val logBenchmark: Boolean = false
  }

  /**
   * Enabled force save only before copying a file.
   *
   * @param enableForReadOnlyMode if true will also apply forceSave to files are they are opened
   *                              in readOnly mode. This is required for situations where a database
   *                              instance was closed without properly closing all files. Depending on
   *                              the operating system, re-opening same database instance might use
   *                              the same MappedByteBuffer which might not be flushed/force saved.
   *                              Setting this to true will cover those situations.
   * @param logBenchmark          if true logs time taken to forceSave.
   */
  case class BeforeCopy(enableForReadOnlyMode: Boolean,
                        logBenchmark: Boolean) extends MMAPFiles with ChannelFiles {
    override val isDisabled: Boolean = false
    override val enabledBeforeClose: Boolean = false
    override val enabledBeforeClean: Boolean = false
    override val enableBeforeCopy: Boolean = true
  }

  /**
   * Enables force save before the file is closed. This applies to both memory-mapped and file-channel files.
   *
   * @param enableBeforeCopy      if true enables [[BeforeCopy]]
   * @param enableForReadOnlyMode if true will also apply forceSave to files are they are opened
   *                              in readOnly mode. This is required for situations where a database
   *                              instance was closed without properly closing all files. Depending on
   *                              the operating system, re-opening same database instance might use
   *                              the same MappedByteBuffer which might not be flushed/force saved.
   *                              Setting this to true will cover those situations.
   * @param logBenchmark          if true logs time taken to forceSave.
   */
  case class BeforeClose(enableBeforeCopy: Boolean,
                         enableForReadOnlyMode: Boolean,
                         logBenchmark: Boolean) extends MMAPFiles with ChannelFiles {
    override val isDisabled: Boolean = false
    override val enabledBeforeClose: Boolean = true
    override val enabledBeforeClean: Boolean = false
  }

  /**
   * Enables force save before a memory-mapped file are cleaned.
   *
   * @param enableBeforeCopy      if true enables [[BeforeCopy]]
   * @param enableForReadOnlyMode if true will also apply forceSave to files are they are opened
   *                              in readOnly mode. This is required for situations where a database
   *                              instance was closed without properly closing all files. Depending on
   *                              the operating system, re-opening same database instance might use
   *                              the same MappedByteBuffer which might not be flushed/force saved.
   *                              Setting this to true will cover those situations.
   * @param logBenchmark          if true logs time taken to forceSave.
   */
  case class BeforeClean(enableBeforeCopy: Boolean,
                         enableForReadOnlyMode: Boolean,
                         logBenchmark: Boolean) extends MMAPFiles {
    override val isDisabled: Boolean = false
    override val enabledBeforeClose: Boolean = false
    override val enabledBeforeClean: Boolean = true
  }

  def off(): ForceSave.Off =
    ForceSave.Off

  def beforeClose(enableBeforeCopy: Boolean,
                  enableForReadOnlyMode: Boolean,
                  logBenchmark: Boolean): ForceSave.BeforeClose =
    BeforeClose(
      enableBeforeCopy = enableBeforeCopy,
      enableForReadOnlyMode = enableForReadOnlyMode,
      logBenchmark = logBenchmark
    )

  def beforeCopy(enableForReadOnlyMode: Boolean,
                 logBenchmark: Boolean): BeforeCopy =
    BeforeCopy(
      enableForReadOnlyMode = enableForReadOnlyMode,
      logBenchmark = logBenchmark
    )

  def beforeClean(enableBeforeCopy: Boolean,
                  enableForReadOnlyMode: Boolean,
                  logBenchmark: Boolean): ForceSave.BeforeClean =
    BeforeClean(
      enableBeforeCopy = enableBeforeCopy,
      enableForReadOnlyMode = enableForReadOnlyMode,
      logBenchmark = logBenchmark
    )
}
