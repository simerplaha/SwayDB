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

  def dropCorruptedTailEntriesAndMaps(): RecoveryMode =
    DropCorruptedTailEntriesAndMaps

  case object DropCorruptedTailEntriesAndMaps extends RecoveryMode {
    override val drop: Boolean = true
  }
}
