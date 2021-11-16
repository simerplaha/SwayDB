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

package swaydb.config.compaction

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
