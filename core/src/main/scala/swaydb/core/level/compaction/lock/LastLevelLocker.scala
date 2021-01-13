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

package swaydb.core.level.compaction.lock

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.level.Level
import swaydb.core.level.zero.LevelZero
import swaydb.{Actor, ActorWire}

import scala.concurrent.ExecutionContext

object LastLevelLocker {

  trait LastLevelSetResponse {
    def levelSetSuccessful(): Unit

    def levelSetFailed(): Unit
  }

  def fetchLastLevel(zero: LevelZero): Level = {
    var lastLevel: Level = null

    zero.nextLevel match {
      case Some(nextLevel) =>
        nextLevel.foreachLevel {
          case level: Level =>
            if (lastLevel == null || level.isNonEmpty())
              lastLevel = level

          case level =>
            throw new Exception(s"${level.getClass.getSimpleName} found in NextLevel.")
        }

      case None =>
        //Not sure if this should be supported in the future but currently at API
        //level we do not allow creating LevelZero without a NextLevel.
        throw new Exception(s"${LevelZero.productPrefix} with no lower level.")
    }

    if (lastLevel == null)
      throw new Exception("Last level is null.")

    lastLevel
  }

  def createActor(zero: LevelZero)(implicit ec: ExecutionContext): ActorWire[LastLevelLocker, Unit] = {
    val lastLevel = fetchLastLevel(zero)

    val state =
      new LastLevelLocker(
        lastLevel = lastLevel,
        locks = 0,
        zero = zero,
        delayedSetOrNull = null
      )

    Actor.wire[LastLevelLocker](
      name = this.getClass.getSimpleName,
      init = _ => state
    )(ec)
  }

}

/**
 * Locks the last Level i.e. the last non-empty level.
 */

class LastLevelLocker private(private var lastLevel: Level,
                              private var locks: Int,
                              zero: LevelZero,
                              private var delayedSetOrNull: () => Unit) extends LazyLogging {

  def lock(): Level = {
    locks += 1
    lastLevel
  }

  def unlock(): Unit = {
    if (locks <= 0) {
      logger.error(s"No existing locks. Failed to unlock. Locks $locks.")
    } else {
      locks -= 1
      if (locks == 0 && delayedSetOrNull != null) {
        val request = delayedSetOrNull
        this.delayedSetOrNull = null
        request.apply()
      }
    }
  }

  def set(newLast: Level, replyTo: ActorWire[LastLevelLocker.LastLevelSetResponse, Unit]): Unit =
    if (newLast.levelNumber == lastLevel.levelNumber) {
      replyTo.send(_.levelSetSuccessful())
    } else if (locks == 0) {
      this.lastLevel = newLast
      replyTo.send(_.levelSetSuccessful())
    } else if (this.delayedSetOrNull != null) {
      logger.error("Extension request already exists.")
      replyTo.send(_.levelSetFailed())
    } else {
      this.delayedSetOrNull = () => this.set(newLast, replyTo)
    }
}
