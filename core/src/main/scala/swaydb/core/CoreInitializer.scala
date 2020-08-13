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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.core

import java.nio.file.Paths

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Level.ExceptionHandler
import swaydb.core.CoreShutdown.shutdown
import swaydb.core.actor.{ByteBufferSweeper, FileSweeper, MemorySweeper}
import swaydb.core.function.FunctionStore
import swaydb.core.actor.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.io.file.Effect._
import swaydb.core.io.file.BlockCache
import swaydb.core.level.compaction._
import swaydb.core.level.compaction.throttle.{ThrottleCompactor, ThrottleState}
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, NextLevel, TrashLevel}
import swaydb.core.segment.format.a.block
import swaydb.core.segment.format.a.block.bloomfilter.BloomFilterBlock
import swaydb.core.segment.format.a.block.segment.SegmentBlock
import swaydb.core.segment.format.a.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.format.a.block.values.ValuesBlock
import swaydb.data.compaction.CompactionExecutionContext
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.storage.{AppendixStorage, LevelStorage}
import swaydb.{ActorRef, ActorWire, Bag, Error, IO, Scheduler}

import scala.concurrent.Await
import scala.concurrent.duration._

/**
 * Creates all configured Levels via [[ConfigWizard]] instances and starts compaction.
 */
private[core] object CoreInitializer extends LazyLogging {

  /**
   * Based on the configuration returns execution context for the Level.
   */
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

  /**
   * Boots up compaction Actor and start listening to changes in levels.
   */
  def initialiseCompaction(zero: LevelZero,
                           executionContexts: List[CompactionExecutionContext])(implicit compactionStrategy: Compactor[ThrottleState]): IO[Error.Level, ActorWire[Compactor[ThrottleState], ThrottleState]] =
    compactionStrategy.createAndListen(
      zero = zero,
      executionContexts = executionContexts
    )

  def sendInitialWakeUp(compactor: ActorWire[Compactor[ThrottleState], ThrottleState]): Unit =
    compactor send {
      (impl, state, self) =>
        impl.wakeUp(
          state = state,
          forwardCopyOnAllLevels = true,
          self = self
        )
    }

  /**
   * Initialises Core/Levels. To see full documentation for each input parameter see the website - http://swaydb.io/configurations/.
   *
   * @param config           configuration used for initialisations which is created via [[ConfigWizard]]
   * @param enableTimer      if true initialises the timer folder. This is only required if the database has functions enabled.
   * @param cacheKeyValueIds if true, will cache the 3000+ key-values in-memory instead of performing binary search for each search key-value id.
   *                         Set this to true to boost performance and reduce IOps.
   * @param fileCache        Controls when files are closed & deleted. See the configuration documentation - http://swaydb.io/configurations/fileCache/
   * @param threadStateCache Each thread is assigned a small cache to boost read performance. This can be optionally enabled. See
   *                         http://swaydb.io/configurations/threadStateCache/
   * @param memoryCache      Configures how in-memory caches should process read bytes and parsed key-values. See - http://swaydb.io/configurations/memoryCache/
   * @param keyOrder         Defines the sort order for keys. See documentation on website.
   * @param timeOrder        Defines the order in which a single key-value's updates are applied.
   * @param functionStore    Stores all registered functions.
   * @return
   */
  def apply(config: SwayDBConfig,
            enableTimer: Boolean,
            cacheKeyValueIds: Boolean,
            fileCache: FileCache.Enable,
            threadStateCache: ThreadStateCache,
            memoryCache: MemoryCache,
            shutdownTimeout: FiniteDuration)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                             timeOrder: TimeOrder[Slice[Byte]],
                                             functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[Bag.Less]] = {

    implicit val fileSweeper: ActorRef[FileSweeper.Command, Unit] =
      FileSweeper(fileCache)

    val memorySweeper: Option[MemorySweeper.Enabled] =
      MemorySweeper(memoryCache)

    implicit val blockCache: Option[BlockCache.State] =
      memorySweeper flatMap BlockCache.init

    implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] =
      memorySweeper flatMap {
        enabled: MemorySweeper.Enabled =>
          enabled match {
            case sweeper: MemorySweeper.All =>
              Some(sweeper)

            case sweeper: MemorySweeper.KeyValueSweeper =>
              Some(sweeper)

            case _: MemorySweeper.BlockSweeper =>
              None
          }
      }

    implicit val compactionStrategy: Compactor[ThrottleState] =
      ThrottleCompactor

    implicit val bufferCleaner: ByteBufferSweeperActor =
      ByteBufferSweeper()(Scheduler()(fileSweeper.executionContext))

    def createLevel(id: Long,
                    nextLevel: Option[NextLevel],
                    config: LevelConfig): IO[swaydb.Error.Level, NextLevel] =
      config match {
        case config: MemoryLevelConfig =>
          Level(
            bloomFilterConfig = BloomFilterBlock.Config.disabled,
            hashIndexConfig = block.hashindex.HashIndexBlock.Config.disabled,
            binarySearchIndexConfig = block.binarysearch.BinarySearchIndexBlock.Config.disabled,
            sortedIndexConfig = SortedIndexBlock.Config.disabled,
            valuesConfig = ValuesBlock.Config.disabled,
            segmentConfig =
              SegmentBlock.Config(
                ioStrategy = _ => IOStrategy.ConcurrentIO(false),
                cacheBlocksOnCreate = false,
                minSize = config.minSegmentSize,
                maxCount = config.maxKeyValuesPerSegment,
                pushForward = config.copyForward,
                mmap = MMAP.Disabled,
                deleteEventually = config.deleteSegmentsEventually,
                compressions = _ => Seq.empty
              ),
            levelStorage = LevelStorage.Memory(dir = Paths.get("MEMORY_LEVEL").resolve(id.toString)),
            appendixStorage = AppendixStorage.Memory,
            nextLevel = nextLevel,
            throttle = config.throttle
          )

        case config: PersistentLevelConfig =>
          Level(
            bloomFilterConfig = BloomFilterBlock.Config(config = config.mightContainKeyIndex),
            hashIndexConfig = block.hashindex.HashIndexBlock.Config(config = config.randomKeyIndex),
            binarySearchIndexConfig = block.binarysearch.BinarySearchIndexBlock.Config(config = config.binarySearchIndex),
            sortedIndexConfig = SortedIndexBlock.Config(config.sortedKeyIndex),
            valuesConfig = ValuesBlock.Config(config.valuesConfig),
            segmentConfig = SegmentBlock.Config(config.segmentConfig),
            levelStorage =
              LevelStorage.Persistent(
                dir = config.dir.resolve(id.toString),
                otherDirs = config.otherDirs.map(dir => dir.copy(path = dir.path.resolve(id.toString)))
              ),
            appendixStorage = AppendixStorage.Persistent(config.mmapAppendix, config.appendixFlushCheckpointSize),
            nextLevel = nextLevel,
            throttle = config.throttle
          )

        case TrashLevelConfig =>
          IO.Right(TrashLevel)
      }

    def createLevels(levelConfigs: List[LevelConfig],
                     previousLowerLevel: Option[NextLevel]): IO[swaydb.Error.Level, Core[Bag.Less]] =
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
                enableTimer = enableTimer,
                cacheKeyValueIds = cacheKeyValueIds,
                nextLevel = Some(level1),
                throttle = config.level0.throttle,
                acceleration = config.level0.acceleration
              ) flatMap {
                zero =>
                  val contexts = executionContexts(config)
                  initialiseCompaction(
                    zero = zero,
                    executionContexts = contexts
                  ) map {
                    implicit compactor =>
                      implicit val shutdownEC =
                        contexts collectFirst {
                          case CompactionExecutionContext.Create(executionContext) =>
                            executionContext
                        } getOrElse scala.concurrent.ExecutionContext.Implicits.global

                      implicit val optionalFileSweeper = Some(fileSweeper)

                      sys.addShutdownHook {
                        implicit val scheduler = Scheduler()
                        Await.result(shutdown(zero, shutdownTimeout), shutdownTimeout)
                      }

                      //trigger initial wakeUp.
                      sendInitialWakeUp(compactor)

                      def onClose =
                        IO.fromFuture[swaydb.Error.Close, Unit] {
                          implicit val scheduler = Scheduler()
                          CoreShutdown.shutdown(zero, shutdownTimeout)
                        }

                      new Core[Bag.Less](
                        zero = zero,
                        threadStateCache = threadStateCache,
                        onClose = onClose
                      )
                  }
              }
          }

        case lowestLevelConfig :: upperLevelConfigs =>

          val levelNumber: Long =
            previousLowerLevel
              .flatMap(_.pathDistributor.headOption.map(_.path.folderId - 1))
              .getOrElse(levelConfigs.size + 1)

          createLevel(levelNumber, previousLowerLevel, lowestLevelConfig) flatMap {
            newLowerLevel =>
              createLevels(upperLevelConfigs, Some(newLowerLevel))
          }
      }

    logger.info(s"Booting ${config.otherLevels.size + 2} Levels.")

    /**
     * Convert [[swaydb.Error.Level]] to [[swaydb.Error]]
     */
    createLevels(config.otherLevels.reverse, None) match {
      case IO.Right(core) =>
        IO[swaydb.Error.Boot, Core[Bag.Less]](core)

      case IO.Left(error) =>
        IO.failed[swaydb.Error.Boot, Core[Bag.Less]](error.exception)
    }
  }
}
