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

package swaydb.config

sealed trait RecoveryMode {
  def name = this.getClass.getSimpleName.replaceAll("\\$", "")

  val drop: Boolean
}

object RecoveryMode {

  /**
   * Returns failure immediately if a corruption is detected.
   */
  def reportFailure(): RecoveryMode =
    ReportFailure

  case object ReportFailure extends RecoveryMode {
    override val drop: Boolean = false
  }

  /**
   * Keeps all entries until a corrupted entry in the Log file is detected and then
   * drops all entries after the corruption.
   */

  def dropCorruptedTailEntries(): RecoveryMode =
    DropCorruptedTailEntries

  case object DropCorruptedTailEntries extends RecoveryMode {
    override val drop: Boolean = true
  }

  /**
   * Keeps all entries until a corrupted entry in the Log file is detected and then
   * drops all entries after the corruption and also ignores all the
   * subsequent Map files after the corrupted file.
   */

  def dropCorruptedTailEntriesAndLogs(): RecoveryMode =
    DropCorruptedTailEntriesAndLogs

  case object DropCorruptedTailEntriesAndLogs extends RecoveryMode {
    override val drop: Boolean = true
  }
}
