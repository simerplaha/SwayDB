/*
 * Copyright (c) 2021 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.data.compaction

/**
 * Defines strategy for compacting non-overlapping key-values.
 */
sealed trait PushStrategy {
  def onOverflow: Boolean
  def immediately: Boolean = !onOverflow
}

case object PushStrategy {

  //for Java
  def onOverflow(): PushStrategy.OnOverflow =
    OnOverflow

  //for Java
  def immediately(): PushStrategy.Immediately =
    Immediately

  /**
   * NOTE: Recommended for mutable data.
   *
   * Disables pushing key-values directly to the lowest non-overlapping Level.
   * These key-values will only be pushed to lower levels if the Level is overflown
   * as defined by the configuration [[LevelThrottle]].
   */
  sealed trait OnOverflow extends PushStrategy
  case object OnOverflow extends OnOverflow {
    override final val onOverflow: Boolean = true
  }

  /**
   * NOTE: Recommended for immutable data.
   *
   * Will always push non-overlapping to the lowest non-overlapping Level.
   * This configuration is useful for data that is mostly immutable.
   *
   * For example: Time-series data where mutability is rarely expected
   * so we enable this to push key-values directly to the lowest Level.
   */
  sealed trait Immediately extends PushStrategy
  case object Immediately extends Immediately {
    final override val onOverflow: Boolean = false
  }
}
