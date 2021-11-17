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

package swaydb.core.compaction.throttle

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, LevelRef}
import swaydb.utils.FiniteDurations.FiniteDurationImplicits

private[throttle] object ThrottleLevelOrdering extends LazyLogging {

  /**
   * Given the Level returns the ordering for [[LevelRef]].
   */
  val ordering =
    new Ordering[LevelRef] {
      override def compare(left: LevelRef, right: LevelRef): Int = {
        (left, right) match {
          //Level
          case (left: Level, right: Level) => order(left, right)
          case (left: Level, right: LevelZero) => order(right, left) * -1
          //LevelZero
          case (left: LevelZero, right: Level) => order(left, right)
          case (_: LevelZero, _: LevelZero) => 0
        }
      }
    }

  def order(left: LevelZero,
            right: Level): Int = {
    val leftPushDelay = left.throttle(left.levelZeroMeter).compactionDelay
    val rightPushDelay = right.throttle(right.meter).compactionDelay
    val compare = leftPushDelay compare rightPushDelay
    logger.debug(s"Levels (${left.levelNumber} -> ${right.levelNumber}) - leftPushDelay: ${leftPushDelay.asString}/${leftPushDelay.toNanos} -> rightPushDelay: ${rightPushDelay.asString}/${rightPushDelay.toNanos} = $compare ")
    compare
  }

  def order(left: Level,
            right: Level): Int = {
    val leftPushDelay = left.throttle(left.meter).compactionDelay
    val rightPushDelay = right.throttle(right.meter).compactionDelay
    val compare = leftPushDelay compare rightPushDelay
    logger.debug(s"Levels (${left.levelNumber} -> ${right.levelNumber}) - leftPushDelay: ${leftPushDelay.asString}/${leftPushDelay.toNanos} -> rightPushDelay: ${rightPushDelay.asString}/${rightPushDelay.toNanos} = $compare ")
    compare
  }
}
