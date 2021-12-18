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

package swaydb.core

import swaydb.config.ForceSave
import swaydb.utils.OperatingSystem

import scala.util.Random

/**
 * Global setting for [[ForceSave]] to handle slow running tests on windows.
 *
 * On all other machines [[ForceSave]] is random for all tests.
 *
 * @see Issue - https://github.com/simerplaha/SwayDB/issues/251
 *      MappedByteBuffer.force is slow on windows and since test-cases are
 *      closing all files on each run Force save on all tests slows down test run.
 */
object TestForceSave {

  @volatile private var double: Double = 1.1 //defaults to allow random.

  /**
   * Enables [[ForceSave]] to be random.
   *
   * @param 0.0  disables randomness - [[ForceSave.Off]].
   *        0.5 would give 50% chance to be random and 50% to be [[ForceSave.Off]].
   *        1.1 will always apply randomness.
   *
   */
  def setRandomForWindows(double: Double = 1.1): Unit =
    this.double = double

  /**
   * @return current set [[ForceSave]] setting for MMAP files for Windows.
   */
  def mmap(): ForceSave.MMAPFiles =
    if (OperatingSystem.isWindows() && Random.nextDouble() >= double)
      ForceSave.Off
    else if (Random.nextBoolean())
      ForceSave.BeforeClean(
        enableBeforeCopy = Random.nextBoolean(),
        enableForReadOnlyMode = Random.nextBoolean(),
        logBenchmark = true
      )
    else if (Random.nextBoolean())
      ForceSave.BeforeClose(
        enableBeforeCopy = Random.nextBoolean(), //java heap error on true and false
        enableForReadOnlyMode = Random.nextBoolean(), //java heap error on true
        logBenchmark = true
      )
    else
      ForceSave.Off

  /**
   * @return current set [[ForceSave]] setting for Channel files for Windows.
   */
  def standard(): ForceSave.StandardFiles =
    if (OperatingSystem.isWindows() && Random.nextDouble() >= double)
      ForceSave.Off
    else if (Random.nextBoolean())
      ForceSave.BeforeClose(
        enableBeforeCopy = Random.nextBoolean(), //java heap error on true and false
        enableForReadOnlyMode = Random.nextBoolean(), //java heap error on true and false
        logBenchmark = true
      )
    else
      ForceSave.Off
}
