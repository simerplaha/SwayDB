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

package swaydb.core.level.compaction.throttle

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
