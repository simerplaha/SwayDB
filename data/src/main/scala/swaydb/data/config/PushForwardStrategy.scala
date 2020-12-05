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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.data.config

/**
 * Defines strategy for pushing key-values to the lowest non-overlapping Level.
 */
sealed trait PushForwardStrategy {
  def on: Boolean
  def always: Boolean
}

case object PushForwardStrategy {

  /**
   * Disables pushing key-values to the lowest non-overlapping Level.
   */
  case object Off extends PushForwardStrategy {
    override final val on: Boolean = false
    override final val always: Boolean = false
  }

  /**
   * Enables compaction to always try to push non-overlapping key-values
   * to the lowest Level.
   *
   * This strategy is useful for data that is mostly inserted sequentially and
   * is less likely to mutate for example Time-series data.
   *
   * The benefit of pushing key-values to the lowest level directly is that the last Level always
   * checks for expired key-values are also delete keys (from disk or RAM) that were removed or expired.
   */
  case object On extends PushForwardStrategy {
    final override val on: Boolean = true
    final override val always: Boolean = true
  }

  /**
   * Enables compaction to push non-overlapping key-values to the next Level only if
   * the current Level is overflown (as defined by [[swaydb.data.compaction.Throttle]])
   * configuration.
   *
   * This strategy will not directly push key-values to the last level which would
   * result in Levels which will have pyramid style size hierarchy as configured in [[swaydb.data.compaction.Throttle]]
   * 1.gb
   * 10.gb
   * 100.gb
   * 1000.gb
   * ...
   *
   * Data that is randomly inserted should use this strategy to improve read performance.
   * Because for randomly inserted that also requires high read performance data we want to
   * avoid creating multiple-levels of the same data which would require reads to seek multiple files.
   */
  case object OnOverflow extends PushForwardStrategy {
    final override val on: Boolean = true
    final override val always: Boolean = false
  }
}
