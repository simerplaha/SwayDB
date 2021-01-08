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

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future, Promise}

object LastLevelLocker {

  def fetchLastLevel(zero: LevelZero): Level = {
    var lastLevel: Level = null

    zero.nextLevel match {
      case Some(nextLevel) =>
        nextLevel.foreachLevel {
          case level: Level =>
            if (level.isNonEmpty())
              lastLevel = level

          case level =>
            throw new Exception(s"${level.getClass.getSimpleName} found in NextLevel.")
        }

      case None =>
        //Not sure if this should be support in the future but currently at API
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
        promises = mutable.Queue.empty
      )

    Actor.wire[LastLevelLocker](
      name = this.getClass.getSimpleName,
      impl = state
    )(ec)
  }

}

class LastLevelLocker private(private var lastLevel: Level,
                              private var locks: Int,
                              zero: LevelZero,
                              promises: mutable.Queue[Promise[Unit]])(implicit ec: ExecutionContext) extends LazyLogging {

  def lock(): Level = {
    locks += 1
    lastLevel
  }

  def unlock(): Unit = {
    if (locks == 0)
      throw new Exception("No existing locks. Failed to unlock.")

    locks -= 1
    if (locks == 0)
      promises.dequeueAll {
        promise =>
          promise.completeWith(Future.unit)
          true
      }
  }

  def resetLastLevel(): Level = {
    this.lastLevel = LastLevelLocker.fetchLastLevel(zero)
    this.lastLevel
  }

  def extend(newLast: Level): Promise[Unit] = {
    if (newLast.levelNumber == lastLevel.levelNumber) {
      Promise.successful(())
    } else if (locks == 0) {
      if (newLast.levelNumber < this.lastLevel.levelNumber) {
        Promise.failed(new Exception(s"New last level: ${newLast.levelNumber} is smaller than ${this.lastLevel.levelNumber}"))
      } else {
        this.lastLevel = newLast
        Promise.successful(())
      }
    } else {
      val promise = Promise[Unit]()
      promises += promise
      promise
    }
  }
}
