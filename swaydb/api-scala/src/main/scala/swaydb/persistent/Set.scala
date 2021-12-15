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

package swaydb.persistent

import com.typesafe.scalalogging.LazyLogging
import swaydb.config.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.config.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.config.sequencer.Sequencer
import swaydb.config._
import swaydb.configs.level.{DefaultExecutionContext, DefaultPersistentConfig}
import swaydb.core.Core
import swaydb.core.build.BuildValidator
import swaydb.core.segment.CoreFunctionStore
import swaydb.effect.Dir
import swaydb.function.FunctionConverter
import swaydb.serializers.{Default, Serializer}
import swaydb.slice.Slice
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.utils.StorageUnits._
import swaydb.{Apply, CommonConfigs, KeyOrderConverter, PureFunction}

import java.nio.file.Path
import scala.reflect.ClassTag

object Set extends LazyLogging {

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[A, F <: PureFunction.Set[A], BAG[_]](dir: Path,
                                                 logSize: Int = DefaultConfigs.logSize,
                                                 appliedFunctionsLogSize: Int = 4.mb,
                                                 clearAppliedFunctionsOnBoot: Boolean = false,
                                                 mmapLogs: MMAP.Log = DefaultConfigs.mmap(),
                                                 recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                                                 mmapAppendixLogs: MMAP.Log = DefaultConfigs.mmap(),
                                                 appendixFlushCheckpointSize: Int = 2.mb,
                                                 otherDirs: Seq[Dir] = Seq.empty,
                                                 cacheKeyValueIds: Boolean = true,
                                                 optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                                                 atomic: Atomic = CommonConfigs.atomic(),
                                                 compactionConfig: CompactionConfig = CommonConfigs.compactionConfig(),
                                                 acceleration: LevelZeroMeter => Accelerator = DefaultConfigs.accelerator,
                                                 threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                                                 sortedIndex: SortedIndex = DefaultConfigs.sortedIndex(),
                                                 hashIndex: HashIndex = DefaultConfigs.hashIndex(),
                                                 binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                                                 bloomFilter: BloomFilter = DefaultConfigs.bloomFilter(),
                                                 valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                                                 segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                                                 fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                                 memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                                                 levelZeroThrottle: LevelZeroMeter => LevelZeroThrottle = DefaultConfigs.levelZeroThrottle,
                                                 levelOneThrottle: LevelMeter => LevelThrottle = DefaultConfigs.levelOneThrottle,
                                                 levelTwoThrottle: LevelMeter => LevelThrottle = DefaultConfigs.levelTwoThrottle,
                                                 levelThreeThrottle: LevelMeter => LevelThrottle = DefaultConfigs.levelThreeThrottle,
                                                 levelFourThrottle: LevelMeter => LevelThrottle = DefaultConfigs.levelFourThrottle,
                                                 levelFiveThrottle: LevelMeter => LevelThrottle = DefaultConfigs.levelFiveThrottle,
                                                 levelSixThrottle: LevelMeter => LevelThrottle = DefaultConfigs.levelSixThrottle)(implicit serializer: Serializer[A],
                                                                                                                                  functionClassTag: ClassTag[F],
                                                                                                                                  bag: swaydb.Bag[BAG],
                                                                                                                                  sequencer: Sequencer[BAG] = null,
                                                                                                                                  functions: Functions[F],
                                                                                                                                  byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                                  typedKeyOrder: KeyOrder[A] = null,
                                                                                                                                  buildValidator: BuildValidator = BuildValidator.DisallowOlderVersions(DataType.Set)): BAG[swaydb.Set[A, F, BAG]] =
    bag.suspend {
      val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
      implicit val unitSerializer: Serializer[Nothing] = Default.NothingSerializer
      val functionStore: CoreFunctionStore = FunctionConverter.toFunctionsStore[A, Nothing, Apply.Set[Nothing], F](functions)

      val set =
        Core(
          enableTimer = PureFunction.isOn(functionClassTag),
          cacheKeyValueIds = cacheKeyValueIds,
          threadStateCache = threadStateCache,
          compactionConfig = compactionConfig,
          config =
            DefaultPersistentConfig(
              dir = dir,
              otherDirs = otherDirs,
              logSize = logSize,
              appliedFunctionsLogSize = appliedFunctionsLogSize,
              clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
              mmapLogs = mmapLogs,
              recoveryMode = recoveryMode,
              mmapAppendixLogs = mmapAppendixLogs,
              appendixFlushCheckpointSize = appendixFlushCheckpointSize,
              sortedIndex = sortedIndex,
              hashIndex = hashIndex,
              binarySearchIndex = binarySearchIndex,
              bloomFilter = bloomFilter,
              valuesConfig = valuesConfig,
              segmentConfig = segmentConfig,
              optimiseWrites = optimiseWrites,
              atomic = atomic,
              acceleration = acceleration,
              levelZeroThrottle = levelZeroThrottle,
              levelOneThrottle = levelOneThrottle,
              levelTwoThrottle = levelTwoThrottle,
              levelThreeThrottle = levelThreeThrottle,
              levelFourThrottle = levelFourThrottle,
              levelFiveThrottle = levelFiveThrottle,
              levelSixThrottle = levelSixThrottle
            ),
          fileCache = fileCache,
          memoryCache = memoryCache
        )(keyOrder = keyOrder,
          timeOrder = TimeOrder.long,
          functionStore = functionStore,
          buildValidator = buildValidator
        ) map {
          core =>
            swaydb.Set[A, F, BAG](core.toBag(sequencer))
        }

      set.toBag[BAG]
    }
}
