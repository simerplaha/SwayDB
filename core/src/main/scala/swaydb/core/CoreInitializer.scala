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

package swaydb.core

import com.typesafe.scalalogging.LazyLogging
import swaydb.Error.Level.ExceptionHandler
import swaydb.core.build.{Build, BuildValidator}
import swaydb.core.function.FunctionStore
import swaydb.core.io.file.ForceSaveApplier
import swaydb.core.level.compaction._
import swaydb.core.level.compaction.throttle.ThrottleCompactorCreator
import swaydb.core.level.zero.LevelZero
import swaydb.core.level.{Level, LevelCloser, NextLevel}
import swaydb.core.segment.block
import swaydb.core.segment.block.binarysearch.BinarySearchIndexConfig
import swaydb.core.segment.block.bloomfilter.{BloomFilterBlock, BloomFilterConfig}
import swaydb.core.segment.block.segment.SegmentBlock
import swaydb.core.segment.block.sortedindex.SortedIndexBlock
import swaydb.core.segment.block.values.ValuesBlock
import swaydb.core.segment.ref.search.ThreadReadState
import swaydb.core.sweeper.ByteBufferSweeper.ByteBufferSweeperActor
import swaydb.core.sweeper.{ByteBufferSweeper, FileSweeper, MemorySweeper}
import swaydb.data.compaction.CompactionConfig
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.sequencer.Sequencer
import swaydb.data.slice.Slice
import swaydb.data.storage.{Level0Storage, LevelStorage}
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.effect.Effect._
import swaydb.effect.IOStrategy
import swaydb.utils.StorageUnits._
import swaydb.{Bag, DefActor, Error, Glass, IO}

import java.util.function.Supplier
import scala.sys.ShutdownHookThread

/**
 * Creates all configured Levels via [[ConfigWizard]] instances and starts compaction.
 */
private[core] object CoreInitializer extends LazyLogging {

  /**
   * Boots up compaction Actor and start listening to changes in levels.
   */
  def initialiseCompaction(zero: LevelZero,
                           compactionConfig: CompactionConfig)(implicit compactorCreator: CompactorCreator,
                                                               fileSweeper: FileSweeper.On): IO[Error.Level, DefActor[Compactor]] =
    compactorCreator.createAndListen(
      zero = zero,
      compactionConfig = compactionConfig
    )

  def sendInitialWakeUp(compactor: DefActor[Compactor]): Unit =
    compactor.send(_.wakeUp())

  def addShutdownHook[BAG[_]](core: Core[BAG]): ShutdownHookThread =
    sys.addShutdownHook {
      if (core.state != CoreState.Closed)
        core.closeWithBag[Glass]()
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
            fileCache: FileCache.On,
            threadStateCache: ThreadStateCache,
            memoryCache: MemoryCache,
            compactionConfig: CompactionConfig)(implicit keyOrder: KeyOrder[Slice[Byte]],
                                                timeOrder: TimeOrder[Slice[Byte]],
                                                functionStore: FunctionStore,
                                                buildValidator: BuildValidator): IO[swaydb.Error.Boot, Core[Glass]] =
    if (config.level0.logSize > 1.gb) {
      val exception = new Exception(s"logSize ${config.level0.logSize / 1000000}.MB is too large. Maximum limit is 1.GB.")
      logger.error(exception.getMessage, exception)
      IO.failed[swaydb.Error.Boot, Core[Glass]](exception)
    } else {
      val validationResult =
        config.level0.storage match {
          case Level0Storage.Memory =>
            IO.unit

          case Level0Storage.Persistent(_, dir, _) =>
            Build.validateOrCreate(dir)
        }

      validationResult match {
        case IO.Right(_) =>

          implicit val fileSweeper: FileSweeper.On =
            FileSweeper(fileCache)

          val memorySweeper: Option[MemorySweeper.On] =
            MemorySweeper(memoryCache)

          implicit val blockCacheSweeper: Option[MemorySweeper.Block] =
            memorySweeper flatMap {
              case block: MemorySweeper.Block =>
                Some(block)

              case _: MemorySweeper.KeyValue =>
                None
            }

          implicit val keyValueMemorySweeper: Option[MemorySweeper.KeyValue] =
            memorySweeper flatMap {
              enabled: MemorySweeper.On =>
                enabled match {
                  case sweeper: MemorySweeper.All =>
                    Some(sweeper)

                  case sweeper: MemorySweeper.KeyValueSweeper =>
                    Some(sweeper)

                  case _: MemorySweeper.BlockSweeper =>
                    None
                }
            }

          implicit val compactorCreator: CompactorCreator =
            ThrottleCompactorCreator

          implicit val bufferSweeper: ByteBufferSweeperActor =
            ByteBufferSweeper()(fileSweeper.executionContext)

          lazy val memoryLevelPath = MemoryPathGenerator.next()

          def createLevel(id: Long,
                          nextLevel: Option[NextLevel],
                          config: LevelConfig): IO[swaydb.Error.Level, NextLevel] =
            config match {
              case config: MemoryLevelConfig =>
                implicit val forceSaveApplier: ForceSaveApplier = ForceSaveApplier.Off

                if (config.minSegmentSize > 1.gb) {
                  val exception = new Exception(s"minSegmentSize ${config.minSegmentSize / 1000000}.MB is too large. Maximum limit is 1.GB.")
                  logger.error(exception.getMessage, exception)
                  IO.failed[swaydb.Error.Level, NextLevel](exception)
                } else {
                  Level(
                    bloomFilterConfig = BloomFilterConfig.disabled(),
                    hashIndexConfig = block.hashindex.HashIndexBlock.Config.disabled,
                    binarySearchIndexConfig = BinarySearchIndexConfig.disabled(),
                    sortedIndexConfig = SortedIndexBlock.Config.disabled,
                    valuesConfig = ValuesBlock.Config.disabled,
                    segmentConfig =
                      SegmentBlock.Config(
                        fileOpenIOStrategy = IOStrategy.AsyncIO(false),
                        blockIOStrategy = _ => IOStrategy.ConcurrentIO(false),
                        cacheBlocksOnCreate = false,
                        minSize = config.minSegmentSize,
                        maxCount = config.maxKeyValuesPerSegment,
                        segmentRefCacheLife = SegmentRefCacheLife.Permanent,
                        enableHashIndexForListSegment = false,
                        initialiseIteratorsInOneSeek = false,
                        mmap = MMAP.Off(ForceSave.Off),
                        deleteDelay = config.deleteDelay,
                        compressions = _ => Seq.empty
                      ),
                    levelStorage = LevelStorage.Memory(dir = memoryLevelPath.resolve(id.toString)),
                    nextLevel = nextLevel,
                    throttle = config.throttle
                  )
                }

              case config: PersistentLevelConfig =>
                implicit val forceSaveApplier: ForceSaveApplier = ForceSaveApplier.On

                if (config.segmentConfig.minSegmentSize > 1.gb) {
                  val exception = new Exception(s"minSegmentSize ${config.segmentConfig.minSegmentSize / 1000000}.MB is too large. Maximum limit is 1.GB.")
                  logger.error(exception.getMessage, exception)
                  IO.failed[swaydb.Error.Level, NextLevel](exception)
                } else if (config.appendixFlushCheckpointSize > 1.gb) {
                  val exception = new Exception(s"appendixFlushCheckpointSize ${config.appendixFlushCheckpointSize / 1000000}.MB is too large. Maximum limit is 1.GB.")
                  logger.error(exception.getMessage, exception)
                  IO.failed[swaydb.Error.Level, NextLevel](exception)
                } else if (config.segmentConfig.segmentFormat.segmentRefCacheLife.isTemporary && blockCacheSweeper.isEmpty && keyValueMemorySweeper.isEmpty) {
                  val exception = new Exception(s"${SegmentRefCacheLife.productPrefix} is ${SegmentRefCacheLife.Temporary.productPrefix} but no cache management configured. See ${MemoryCache.productPrefix} configuration.")
                  logger.error(exception.getMessage, exception)
                  IO.failed[swaydb.Error.Level, NextLevel](exception)
                } else {
                  Level(
                    bloomFilterConfig = BloomFilterConfig(config = config.bloomFilter),
                    hashIndexConfig = block.hashindex.HashIndexBlock.Config(config = config.hashIndex),
                    binarySearchIndexConfig = BinarySearchIndexConfig(config = config.binarySearchIndex),
                    sortedIndexConfig = SortedIndexBlock.Config(config.sortedIndex),
                    valuesConfig = ValuesBlock.Config(config.valuesConfig),
                    segmentConfig = SegmentBlock.Config(config.segmentConfig),
                    levelStorage =
                      LevelStorage.Persistent(
                        dir = config.dir.resolve(id.toString),
                        otherDirs = config.otherDirs.map(dir => dir.copy(path = dir.path.resolve(id.toString))),
                        appendixMMAP = config.mmapAppendixLogs,
                        appendixFlushCheckpointSize = config.appendixFlushCheckpointSize
                      ),
                    nextLevel = nextLevel,
                    throttle = config.throttle
                  )
                }
            }

          def createLevels(levelConfigs: List[LevelConfig],
                           previousLowerLevel: Option[NextLevel]): IO[swaydb.Error.Level, Core[Glass]] =
            levelConfigs match {
              case Nil =>
                implicit val forceSaveApplier: ForceSaveApplier = ForceSaveApplier.On

                createLevel(
                  id = 1,
                  nextLevel = previousLowerLevel,
                  config = config.level1
                ) flatMap {
                  level1 =>
                    val coreState = CoreState()

                    implicit val optimiseWrites: OptimiseWrites = config.level0.optimiseWrites
                    implicit val atomic: Atomic = config.level0.atomic

                    LevelZero(
                      logSize = config.level0.logSize,
                      appliedFunctionsLogSize = config.level0.appliedFunctionsLogSize,
                      clearAppliedFunctionsOnBoot = config.level0.clearAppliedFunctionsOnBoot,
                      storage = config.level0.storage,
                      enableTimer = enableTimer,
                      cacheKeyValueIds = cacheKeyValueIds,
                      coreState = coreState,
                      nextLevel = Some(level1),
                      acceleration = config.level0.acceleration,
                      throttle = config.level0.throttle
                    ) flatMap {
                      zero: LevelZero =>
                        initialiseCompaction(
                          zero = zero,
                          compactionConfig = compactionConfig
                        ) map {
                          implicit compactor =>

                            //trigger initial wakeUp.
                            sendInitialWakeUp(compactor)

                            val readStates: ThreadLocal[ThreadReadState] =
                              ThreadLocal.withInitial[ThreadReadState] {
                                new Supplier[ThreadReadState] {
                                  override def get(): ThreadReadState =
                                    threadStateCache match {
                                      case ThreadStateCache.Limit(hashMapMaxSize, maxProbe) =>
                                        ThreadReadState.limitHashMap(
                                          maxSize = hashMapMaxSize,
                                          probe = maxProbe
                                        )

                                      case ThreadStateCache.NoLimit =>
                                        ThreadReadState.hashMap()

                                      case ThreadStateCache.Off =>
                                        ThreadReadState.limitHashMap(
                                          maxSize = 0,
                                          probe = 0
                                        )
                                    }
                                }
                              }

                            val core =
                              new Core[Glass](
                                zero = zero,
                                coreState = coreState,
                                threadStateCache = threadStateCache,
                                sequencer = Sequencer.synchronised(Bag.glass),
                                readStates = readStates
                              )

                            addShutdownHook(core = core)

                            core
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
              IO[swaydb.Error.Boot, Core[Glass]](core)

            case IO.Left(createError) =>
              IO(LevelCloser.close[Glass]()) match {
                case IO.Right(_) =>
                  IO.failed[swaydb.Error.Boot, Core[Glass]](createError.exception)

                case IO.Left(closeError) =>
                  val createException = createError.exception
                  logger.error("Failed to create", createException)
                  logger.error("Failed to close", closeError.exception)
                  IO.failed[swaydb.Error.Boot, Core[Glass]](createException)
              }
          }

        case IO.Left(error) =>
          IO.failed[swaydb.Error.Boot, Core[Glass]](error.exception)
      }
    }
}
