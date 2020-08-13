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
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.actor.FileSweeper.FileSweeperActor
import swaydb.core.actor.{ByteBufferSweeper, FileSweeper, MemorySweeper}
import swaydb.core.io.file.BlockCache
import swaydb.core.level.compaction.Compactor
import swaydb.core.level.compaction.throttle.ThrottleState
import swaydb.core.level.zero.LevelZero
import swaydb.data.util.Futures
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

  def shutdown(zero: LevelZero,
               retryOnBusyDelay: FiniteDuration)(implicit compactor: ActorWire[Compactor[ThrottleState], ThrottleState],
                                                 fileSweeper: Option[FileSweeperActor],
                                                 blockCache: Option[BlockCache.State],
                                                 keyValueMemorySweeper: Option[MemorySweeper.KeyValue],
                                                 scheduler: Scheduler,
                                                 cleaner: ByteBufferSweeperActor): Future[Unit] = {
    implicit val ec = scheduler.ec
    implicit val futureBag = Bag.future(scheduler.ec)

    logger.info("****** Shutting down ******")

    logger.info("Stopping compaction!")
    val compactionShutdown =
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

    val levelsShutdown =
      compactionShutdown flatMap {
        _ =>
          logger.info("Closing files!")
          zero.close onLeftSideEffect {
            error =>
              logger.error("Failed to close files.", error.exception)
          } toFuture
      }

    val blockCacheShutdown =
      levelsShutdown flatMap {
        _ =>
          logger.info("Clearing blockCache.")
          blockCache match {
            case Some(value) =>
              Future.successful(value.clear())

            case None =>
              Futures.unit
          }
      }

    val keyValueSweeperShutdown =
      blockCacheShutdown flatMap {
        _ =>
          keyValueMemorySweeper.foreach {
            sweeper =>
              logger.info("Clearing cached key-values")
              sweeper.terminateAndClear()
          }
          Futures.unit
      }

    val fileSweeperShutdown =
      keyValueSweeperShutdown flatMap {
        _ =>
          fileSweeper match {
            case Some(fileSweeper) =>
              logger.info(s"Terminating FileSweeperActor.")
              fileSweeper.terminateAndRecover(retryOnBusyDelay)

            case None =>
              Futures.unit
          }
      }

    val bufferCleanerResult =
      fileSweeperShutdown flatMap {
        _ =>
          logger.info(s"Terminating ByteBufferCleanerActor.")
          cleaner.get() match {
            case Some(actor) =>
              actor.terminateAndRecover(retryOnBusyDelay) flatMap {
                _ =>
                  logger.info(s"Terminated ByteBufferCleanerActor. Awaiting shutdown response.")
                  actor ask ByteBufferSweeper.Command.IsTerminatedAndCleaned[Unit]
              } flatMap {
                isShut =>
                  if (isShut)
                    Futures.`true`
                  else
                    Futures.`false`
              }

            case None =>
              Futures.unit
          }
      }

    val releaseLocks =
      bufferCleanerResult flatMap {
        _ =>
          logger.info("Releasing locks.")
          zero.releaseLocks onLeftSideEffect {
            error =>
              logger.error("Failed to release locks.", error.exception)
          } toFuture
      }

    releaseLocks flatMap {
      _ =>
        logger.info("Terminating Scheduler.")
        scheduler.terminate()
        Futures.unit
    }
  }
}
