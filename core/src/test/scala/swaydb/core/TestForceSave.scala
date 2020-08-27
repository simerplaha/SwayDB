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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core

import swaydb.data.config.ForceSave
import swaydb.data.util.OperatingSystem

import scala.util.Random

/**
 * Global setting for [[ForceSave]] to handle slow running tests on windows.
 *
 * On all other machines [[ForceSave]] is random for all tests.
 *
 * @see Issue - https://github.com/simerplaha/SwayDB/issues/251
 *      MappedByteBuffer.force is slow on windows and since test-cases are
 *      closing all files on each run slows down test run.
 */
object TestForceSave {

  //sets ForceSave setting for MMAP files on windows
  @volatile private var forceSaveMMAP: ForceSave.MMAPFiles = ForceSave.Disabled
  //sets ForceSave setting for Channel files on windows
  @volatile private var forceSaveChannel: ForceSave.ChannelFiles = ForceSave.Disabled
  //sets ForceSave setting to be random for windows
  @volatile private var double: Double = 0.0

  /**
   * Sets the [[ForceSave]] setting for windows.
   */
  def setForWindows(forceSaveMMAP: ForceSave.MMAPFiles,
                    forceSaveChannel: ForceSave.ChannelFiles): Unit = {
    this.forceSaveMMAP = forceSaveMMAP
    this.forceSaveChannel = forceSaveChannel
  }

  /**
   * Enables [[ForceSave]] to be random.
   *
   * @param 0.0 disables randomness. 0.5 would give 50% chance to be random.
   *
   */
  def setRandomForWindows(double: Double = 0.0): Unit =
    this.double = double

  /**
   * @return current set [[ForceSave]] setting for MMAP files for Windows.
   */
  def mmap(): ForceSave.MMAPFiles =
    if (OperatingSystem.isWindows && Random.nextDouble() >= double)
      forceSaveMMAP
    else if (Random.nextBoolean())
      ForceSave.BeforeClean(
        enableBeforeCopy = Random.nextBoolean(),
        enableForReadOnly = Random.nextBoolean(),
        logBenchmark = true
      )
    else if (Random.nextBoolean())
      ForceSave.BeforeClose(
        enableBeforeCopy = Random.nextBoolean(),
        enableForReadOnly = Random.nextBoolean(),
        logBenchmark = true
      )
    else
      ForceSave.Disabled

  /**
   * @return current set [[ForceSave]] setting for Channel files for Windows.
   */
  def channel(): ForceSave.ChannelFiles =
    if (OperatingSystem.isWindows && Random.nextDouble() >= double)
      forceSaveChannel
    else if (Random.nextBoolean())
      ForceSave.BeforeClose(
        enableBeforeCopy = Random.nextBoolean(),
        enableForReadOnly = Random.nextBoolean(),
        logBenchmark = true
      )
    else
      ForceSave.Disabled
}
