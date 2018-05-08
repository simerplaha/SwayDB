/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.core

import java.nio.file.Paths

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.data.Persistent
import swaydb.core.io.file.DBFile
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, LevelRef, TrashLevel}
import swaydb.core.segment.Segment
import swaydb.core.util.FileUtil._
import swaydb.data.config._
import swaydb.data.slice.Slice
import swaydb.data.storage.{AppendixStorage, LevelStorage}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.FiniteDuration
import scala.util.{Success, Try}
import scala.concurrent.duration._

private[core] object DBInitializer extends LazyLogging {

  val graceTimeout = 10.seconds

  def apply(config: SwayDBConfig,
            maxSegmentsOpen: Int,
            cacheSize: Long,
            keyValueQueueDelay: FiniteDuration,
            segmentCloserDelay: FiniteDuration)(implicit ec: ExecutionContext,
                                                ordering: Ordering[Slice[Byte]]): Try[CoreAPI] = {

    implicit val fileOpenLimiter: DBFile => Unit =
      if (config.persistent)
        LimitQueues.segmentOpenLimiter(maxSegmentsOpen, segmentCloserDelay)
      else
        _ => throw new IllegalAccessError("fileOpenLimiter is not required for in-memory databases.")

    implicit val keyValueLimiter: (Persistent, Segment) => Unit =
      if (config.persistent)
        LimitQueues.keyValueLimiter(cacheSize, keyValueQueueDelay)
      else
        (_, _) => throw new IllegalAccessError("keyValueLimiter is not required for in-memory databases.")

    def createLevel(id: Long,
                    nextLevel: Option[LevelRef],
                    config: LevelConfig): Try[LevelRef] =
      config match {
        case MemoryLevelConfig(segmentSize, pushForward, bloomFilterFalsePositiveRate, throttle) =>
          Level(
            levelStorage = LevelStorage.Memory(dir = Paths.get("MEMORY_LEVEL").resolve(id.toString)),
            segmentSize = segmentSize,
            nextLevel = nextLevel,
            pushForward = pushForward,
            appendixStorage = AppendixStorage.Memory,
            bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
            throttle = throttle,
            graceTimeout = graceTimeout
          )

        case PersistentLevelConfig(dir, otherDirs, segmentSize, mmapSegment, mmapAppendix, appendixFlushCheckpointSize, pushForward, bloomFilterFalsePositiveRate, throttle) =>
          Level(
            levelStorage =
              LevelStorage.Persistent(
                mmapSegmentsOnWrite = mmapSegment.mmapWrite,
                mmapSegmentsOnRead = mmapSegment.mmapRead,
                dir = dir.resolve(id.toString),
                otherDirs = otherDirs.map(dir => dir.copy(path = dir.path.resolve(id.toString)))
              ),
            segmentSize = segmentSize,
            nextLevel = nextLevel,
            pushForward = pushForward,
            appendixStorage = AppendixStorage.Persistent(mmapAppendix, appendixFlushCheckpointSize),
            bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
            throttle = throttle,
            graceTimeout = graceTimeout
          )

        case TrashLevelConfig =>
          Success(TrashLevel)
      }

    /**
      * Closes all the open files and releases the locks on database folders.
      */
    def addShutdownHook(zero: LevelZero): Unit =
      sys.addShutdownHook {
        logger.info("Closing files.")
        zero.close.failed foreach {
          exception =>
            logger.error("Failed to close Levels.", exception)
        }

        logger.info("Releasing database locks.")
        zero.releaseLocks.failed foreach {
          exception =>
            logger.error("Failed to release locks.", exception)
        }
      }

    def createLevels(levelConfigs: List[LevelConfig],
                     previousLowerLevel: Option[LevelRef]): Try[CoreAPI] =
      levelConfigs match {
        case Nil =>
          createLevel(1, previousLowerLevel, config.level1) flatMap {
            level1 =>
              LevelZero(config.level0.mapSize, config.level0.storage, level1, config.level0.acceleration, 10000, graceTimeout) map {
                zero =>
                  addShutdownHook(zero)
                  zero
              }
          }

        case lowestLevelConfig :: upperLevelConfigs =>
          val levelNumber: Long = previousLowerLevel.flatMap(_.paths.headOption.map(_.path.folderId - 1)).getOrElse(levelConfigs.size + 1)
          createLevel(levelNumber, previousLowerLevel, lowestLevelConfig) flatMap {
            newLowerLevel =>
              createLevels(upperLevelConfigs, Some(newLowerLevel))
          }
      }

    createLevels(config.otherLevels.reverse, None)
  }

}
