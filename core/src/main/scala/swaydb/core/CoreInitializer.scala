/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 */

package swaydb.core

import java.nio.file.Paths

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Level.ExceptionHandler
import swaydb.{IO, Scheduler, WiredActor}
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.function.FunctionStore
import swaydb.core.group.compression.GroupByInternal
import swaydb.core.io.file.IOEffect._
import swaydb.core.io.file.{BlockCache, BufferCleaner}
import swaydb.core.level.compaction._
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, NextLevel, TrashLevel}
import swaydb.core.segment.format.a.block
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.{AppendixStorage, LevelStorage}

import scala.concurrent.ExecutionContext

private[core] object CoreInitializer extends LazyLogging {

  implicit val compaction: Compaction = Compaction.default

  /**
   * Closes all the open files and releases the locks on database folders.
   */
  private def addShutdownHook(zero: LevelZero,
                              compactor: Option[WiredActor[CompactionStrategy[CompactorState], CompactorState]])(implicit compactionStrategy: CompactionStrategy[CompactorState]): Unit =
    sys.addShutdownHook {
      logger.info("Shutting down compaction.")
      IO(compactor foreach compactionStrategy.terminate) onLeftSideEffect {
        error =>
          logger.error("Failed compaction shutdown.", error.exception)
      }

      logger.info("Closing files.")
      zero.close onLeftSideEffect {
        error =>
          logger.error("Failed to close Levels.", error.exception)
      }

      logger.info("Releasing database locks.")
      zero.releaseLocks onLeftSideEffect {
        error =>
          logger.error("Failed to release locks.", error.exception)
      }
    }

  def apply(config: LevelZeroPersistentConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                               timeOrder: TimeOrder[Slice[Byte]],
                                               functionStore: FunctionStore,
                                               bufferCleanerEC: Option[ExecutionContext] = None): IO[swaydb.Error.Boot, Core[IO.ApiIO]] = {

    implicit val compactionStrategy: CompactionStrategy[CompactorState] = Compactor
    if (config.storage.isMMAP && bufferCleanerEC.isEmpty)
      IO.failed[swaydb.Error.Boot, Core[IO.ApiIO]]("ExecutionContext for ByteBuffer is required for memory-mapped configured databases.") //FIXME - create a LevelZeroPersistentMMAPConfig type to remove this error check.
    else
      LevelZero(
        mapSize = config.mapSize,
        storage = config.storage,
        nextLevel = None,
        throttle = config.throttle,
        acceleration = config.acceleration
      ) match {
        case IO.Right(zero) =>
          bufferCleanerEC foreach (ec => BufferCleaner.initialiseCleaner(Scheduler()(ec)))
          addShutdownHook(zero, None)
          IO[swaydb.Error.Boot, Core[IO.ApiIO]](new Core(zero, () => IO.unit))

        case IO.Left(error) =>
          IO.failed[swaydb.Error.Boot, Core[IO.ApiIO]](error.exception)
      }
  }

  def apply(config: LevelZeroMemoryConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                           timeOrder: TimeOrder[Slice[Byte]],
                                           functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[IO.ApiIO]] = {

    implicit val compactionStrategy: CompactionStrategy[CompactorState] = Compactor

    LevelZero(
      mapSize = config.mapSize,
      storage = config.storage,
      nextLevel = None,
      throttle = config.throttle,
      acceleration = config.acceleration
    ) match {
      case IO.Right(zero) =>
        addShutdownHook(zero, None)
        IO[swaydb.Error.Boot, Core[IO.ApiIO]](new Core(zero, () => IO.unit))

      case IO.Left(error) =>
        IO.failed[swaydb.Error.Boot, Core[IO.ApiIO]](error.exception)
    }
  }

  def executionContext(levelConfig: LevelConfig): Option[CompactionExecutionContext] =
    levelConfig match {
      case TrashLevelConfig =>
        None

      case config: MemoryLevelConfig =>
        Some(config.compactionExecutionContext)

      case config: PersistentLevelConfig =>
        Some(config.compactionExecutionContext)
    }

  def executionContexts(config: SwayDBConfig): List[CompactionExecutionContext] =
    List(config.level0.compactionExecutionContext) ++
      executionContext(config.level1).toList ++
      config.otherLevels.flatMap(executionContext)

  def startCompaction(zero: LevelZero,
                      executionContexts: List[CompactionExecutionContext],
                      copyForwardAllOnStart: Boolean)(implicit compactionStrategy: CompactionStrategy[CompactorState],
                                                      compactionOrdering: CompactionOrdering): IO[swaydb.Error.Level, Option[WiredActor[CompactionStrategy[CompactorState], CompactorState]]] =
    compactionStrategy.createAndListen(
      zero = zero,
      executionContexts = executionContexts,
      copyForwardAllOnStart = copyForwardAllOnStart
    ) map (Some(_))

  def sendInitialWakeUp(compactor: WiredActor[CompactionStrategy[CompactorState], CompactorState]): Unit =
    compactor send {
      (impl, state, self) =>
        impl.wakeUp(
          state = state,
          forwardCopyOnAllLevels = true,
          self = self
        )
    }

  def apply(config: SwayDBConfig,
            fileCache: FileCache.Enable,
            memoryCache: MemoryCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                      timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[IO.ApiIO]] = {

    implicit val fileSweeper: FileSweeper.Enabled =
      FileSweeper(fileCache)

    //TODO - do not initialise for in-memory no grouping databases.
    implicit val memorySweeper: Option[MemorySweeper.Enabled] =
      MemorySweeper(memoryCache)

    implicit val blockCache: Option[BlockCache.State] =
      memorySweeper flatMap BlockCache.init

    implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] =
      memorySweeper flatMap {
        enabled: MemorySweeper.Enabled =>
          enabled match {
            case both: MemorySweeper.Both =>
              Some(both)

            case value: MemorySweeper.KeyValueSweeper =>
              Some(value)

            case _: MemorySweeper.BlockSweeper =>
              None
          }
      }

    implicit val compactionStrategy: CompactionStrategy[CompactorState] =
      Compactor

    implicit val compactionOrdering: CompactionOrdering =
      DefaultCompactionOrdering

    if (config.hasMMAP)
      BufferCleaner.initialiseCleaner(Scheduler()(fileSweeper.ec))

    def createLevel(id: Long,
                    nextLevel: Option[NextLevel],
                    config: LevelConfig): IO[swaydb.Error.Level, NextLevel] =
      config match {
        case config: MemoryLevelConfig =>
          implicit val compression: Option[GroupByInternal.KeyValues] = config.groupBy map GroupByInternal.apply
          Level(
            segmentSize = config.segmentSize,
            bloomFilterConfig = block.BloomFilterBlock.Config.disabled,
            hashIndexConfig = block.HashIndexBlock.Config.disabled,
            binarySearchIndexConfig = block.BinarySearchIndexBlock.Config.disabled,
            sortedIndexConfig = block.SortedIndexBlock.Config.disabled,
            valuesConfig = block.ValuesBlock.Config.disabled,
            segmentConfig = block.SegmentBlock.Config.default,
            levelStorage = LevelStorage.Memory(dir = Paths.get("MEMORY_LEVEL").resolve(id.toString)),
            appendixStorage = AppendixStorage.Memory,
            nextLevel = nextLevel,
            pushForward = config.copyForward,
            throttle = config.throttle,
            deleteSegmentsEventually = config.deleteSegmentsEventually
          )

        case config: PersistentLevelConfig =>
          implicit val compression: Option[GroupByInternal.KeyValues] = config.groupBy map GroupByInternal.apply
          Level(
            segmentSize = config.segmentSize,
            bloomFilterConfig = block.BloomFilterBlock.Config(config = config.mightContainKey),
            hashIndexConfig = block.HashIndexBlock.Config(config = config.hashIndex),
            binarySearchIndexConfig = block.BinarySearchIndexBlock.Config(config = config.binarySearchIndex),
            sortedIndexConfig = block.SortedIndexBlock.Config(config.sortedIndex),
            valuesConfig = block.ValuesBlock.Config(config.values),
            segmentConfig = block.SegmentBlock.Config(config.segmentIO, config.segmentCompressions),
            levelStorage =
              LevelStorage.Persistent(
                mmapSegmentsOnWrite = config.mmapSegment.mmapWrite,
                mmapSegmentsOnRead = config.mmapSegment.mmapRead,
                dir = config.dir.resolve(id.toString),
                otherDirs = config.otherDirs.map(dir => dir.copy(path = dir.path.resolve(id.toString)))
              ),
            appendixStorage = AppendixStorage.Persistent(config.mmapAppendix, config.appendixFlushCheckpointSize),
            nextLevel = nextLevel,
            pushForward = config.copyForward,
            throttle = config.throttle,
            deleteSegmentsEventually = config.deleteSegmentsEventually
          )

        case TrashLevelConfig =>
          IO.Right(TrashLevel)
      }

    def createLevels(levelConfigs: List[LevelConfig],
                     previousLowerLevel: Option[NextLevel]): IO[swaydb.Error.Level, Core[IO.ApiIO]] =
      levelConfigs match {
        case Nil =>
          createLevel(
            id = 1,
            nextLevel = previousLowerLevel,
            config = config.level1
          ) flatMap {
            level1 =>
              LevelZero(
                mapSize = config.level0.mapSize,
                storage = config.level0.storage,
                nextLevel = Some(level1),
                throttle = config.level0.throttle,
                acceleration = config.level0.acceleration
              ) flatMap {
                zero =>
                  startCompaction(
                    zero = zero,
                    executionContexts = executionContexts(config),
                    copyForwardAllOnStart = true
                  ) map {
                    compactor =>
                      addShutdownHook(
                        zero = zero,
                        compactor = compactor
                      )

                      //trigger initial wakeUp.
                      compactor foreach sendInitialWakeUp

                      new Core(
                        zero = zero,
                        onClose = () => IO[swaydb.Error.Close, Unit](compactor foreach compactionStrategy.terminate)
                      )
                  }
              }
          }

        case lowestLevelConfig :: upperLevelConfigs =>

          val levelNumber: Long =
            previousLowerLevel
              .flatMap(_.paths.headOption.map(_.path.folderId - 1))
              .getOrElse(levelConfigs.size + 1)

          createLevel(levelNumber, previousLowerLevel, lowestLevelConfig) flatMap {
            newLowerLevel =>
              createLevels(upperLevelConfigs, Some(newLowerLevel))
          }
      }

    logger.info(s"Starting ${config.otherLevels.size} configured Levels.")

    /**
     * Convert [[swaydb.Error.Level]] to [[swaydb.Error]]
     */
    createLevels(config.otherLevels.reverse, None) match {
      case IO.Right(core) =>
        IO[swaydb.Error.Boot, Core[IO.ApiIO]](core)

      case IO.Left(error) =>
        IO.failed[swaydb.Error.Boot, Core[IO.ApiIO]](error.exception)
    }
  }
}
