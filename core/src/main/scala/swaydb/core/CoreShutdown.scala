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
 * If you modify this Program, or any covered work, by linking or combining
 * it with other code, such other code is not for that reason alone subject
 * to any of the requirements of the GNU Affero GPL version 3.
 */

package swaydb.core

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.level.compaction.Compactor
import swaydb.core.level.compaction.throttle.ThrottleState
import swaydb.core.level.zero.LevelZero
import swaydb.data.util.Futures.FutureImplicits
import swaydb.{ActorWire, Bag, Scheduler}

import scala.concurrent.Future
import scala.concurrent.duration._

private[core] object CoreShutdown extends LazyLogging {

  /**
   * This does the final clean up on all Levels.
   *
   * - Releases all locks on all system folders
   * - Flushes all files and persists them to disk for persistent databases.
   */

  def close(zero: LevelZero,
            retryInterval: FiniteDuration)(implicit compactor: ActorWire[Compactor[ThrottleState], ThrottleState],
                                           scheduler: Scheduler) = {
    implicit val ec = scheduler.ec
    implicit val futureBag = Bag.future(scheduler.ec)

    logger.info("****** Shutting down ******")

    logger.info("Stopping compaction!")
    compactor
      .ask
      .flatMap {
        (impl, state, self) =>
          impl.terminate(state, self)
      }
      .recoverWith {
        case exception =>
          logger.error("Failed compaction shutdown.", exception)
          Future.failed(exception)
      }
      .and(zero.close(retryInterval))
  }
}
