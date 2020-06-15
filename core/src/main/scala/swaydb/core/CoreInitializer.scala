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
import swaydb.core.actor.{FileSweeper, MemorySweeper}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.Effect._
import swaydb.core.io.file.{BlockCache, BufferCleaner}
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
import swaydb.{ActorWire, Bag, Error, IO, Scheduler}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Creates all configured Levels via [[ConfigWizard]] instances and starts compaction.
 */
private[core] object CoreInitializer extends LazyLogging {

  /**
   * This does the final clean up on all Levels.
   *
   * - Releases all locks on all system folders
   * - Flushes all files and persists them to disk for persistent databases.
   */
  private def closeLevels(zero: LevelZero) = {
    logger.info("Closing files.")
    zero.close onLeftSideEffect {
      error =>
        logger.error("Failed to close files.", error.exception)
    } onRightSideEffect {
      _ =>
        logger.info("Files closed!")
    }

    logger.info("Releasing locks.")
    zero.releaseLocks onLeftSideEffect {
      error =>
        logger.error("Failed to release locks.", error.exception)
    } onRightSideEffect {
      _ =>
        logger.info("Locks released!")
    }
  }

  /**
   * Closes all the open files and releases the locks on database folders.
   */
  private def addShutdownHook(zero: LevelZero,
                              compactor: ActorWire[Compactor[ThrottleState], ThrottleState])(implicit executionContext: ExecutionContext): Unit =
    sys.addShutdownHook {
      logger.info("Shutting down compaction.")

      def compactionShutdown: Future[Unit] =
        compactor
          .ask
          .flatMap {
            (impl, state, self) =>
              impl.terminate(state, self)
          }

      IO {
        Await.result(compactionShutdown, 30.seconds)
      } onLeftSideEffect {
        error =>
          logger.error("Failed compaction shutdown.", error.exception)
      } onRightSideEffect {
        _ =>
          logger.info("Compaction stopped!")
      }

      closeLevels(zero)
    }

  /**
   * Closes all the open files and releases the locks on database folders.
   */
  private def addShutdownHookNoCompaction(zero: LevelZero): Unit =
    sys.addShutdownHook {
      closeLevels(zero)
    }

  def apply(config: PersistentLevelZeroConfig,
            enableTimer: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore,
                                  bufferCleanerEC: Option[ExecutionContext] = None): IO[swaydb.Error.Boot, Core[Bag.Less]] =
    if (config.storage.isMMAP && bufferCleanerEC.isEmpty)
      IO.failed[swaydb.Error.Boot, Core[Bag.Less]]("ExecutionContext for ByteBuffer is required for memory-mapped configured databases.") //FIXME - create a LevelZeroPersistentMMAPConfig type to remove this error check.
    else
      LevelZero(
        mapSize = config.mapSize,
        storage = config.storage,
        enableTimer = enableTimer,
        cacheKeyValueIds = false,
        nextLevel = None,
        acceleration = config.acceleration,
        throttle = config.throttle
      ) match {
        case IO.Right(zero) =>
          bufferCleanerEC foreach (ec => BufferCleaner.initialiseCleaner(Scheduler()(ec)))
          addShutdownHookNoCompaction(zero)
          IO[swaydb.Error.Boot, Core[Bag.Less]](new Core(zero, ThreadStateCache.NoLimit, IO.Defer.unit))

        case IO.Left(error) =>
          IO.failed[swaydb.Error.Boot, Core[Bag.Less]](error.exception)
      }

  def apply(config: MemoryLevelZeroConfig,
            enableTimer: Boolean)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                  timeOrder: TimeOrder[Slice[Byte]],
                                  functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[Bag.Less]] =
    LevelZero(
      mapSize = config.mapSize,
      storage = config.storage,
      nextLevel = None,
      enableTimer = enableTimer,
      cacheKeyValueIds = false,
      throttle = config.throttle,
      acceleration = config.acceleration
    ) match {
      case IO.Right(zero) =>
        addShutdownHookNoCompaction(zero)
        IO[swaydb.Error.Boot, Core[Bag.Less]](new Core(zero, ThreadStateCache.NoLimit, IO.Defer.unit))

      case IO.Left(error) =>
        IO.failed[swaydb.Error.Boot, Core[Bag.Less]](error.exception)
    }

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
            memoryCache: MemoryCache)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                      timeOrder: TimeOrder[Slice[Byte]],
                                      functionStore: FunctionStore): IO[swaydb.Error.Boot, Core[Bag.Less]] = {

    implicit val fileSweeper: FileSweeper.Enabled =
      FileSweeper(fileCache)

    //TODO - do not initialise for in-memory no grouping databases.
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

    if (config.hasMMAP)
      BufferCleaner.initialiseCleaner(Scheduler()(fileSweeper.ec))

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
                mmapWrites = false,
                mmapReads = false,
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
                    compactor =>
                      implicit val shutdownEC =
                        contexts collectFirst {
                          case CompactionExecutionContext.Create(executionContext) =>
                            executionContext
                        } getOrElse scala.concurrent.ExecutionContext.Implicits.global

                      addShutdownHook(
                        zero = zero,
                        compactor = compactor
                      )

                      //trigger initial wakeUp.
                      sendInitialWakeUp(compactor)

                      def onClose =
                        IO.fromFuture[swaydb.Error.Close, Unit] {
                          compactor
                            .ask
                            .flatMap {
                              (impl, state, actor) =>
                                impl.terminate(state, actor)
                            }
                        } flatMapIO {
                          _ =>
                            closeLevels(zero)
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
