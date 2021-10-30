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

package swaydb.eventually.persistent

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.{DefaultEventuallyPersistentConfig, DefaultExecutionContext}
import swaydb.core.Core
import swaydb.core.build.BuildValidator
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.CompactionConfig
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.sequencer.Sequencer
import swaydb.data.slice.Slice
import swaydb.data.{Atomic, DataType, Functions, OptimiseWrites}
import swaydb.effect.Dir
import swaydb.function.FunctionConverter
import swaydb.serializers.{Default, Serializer}
import swaydb.utils.StorageUnits._
import swaydb.{Apply, CommonConfigs, KeyOrderConverter, PureFunction}

import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object Set extends LazyLogging {

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[A, F <: PureFunction.Set[A], BAG[_]](dir: Path,
                                                 logSize: Int = DefaultConfigs.logSize,
                                                 appliedFunctionsLogSize: Int = 4.mb,
                                                 clearAppliedFunctionsOnBoot: Boolean = false,
                                                 maxMemoryLevelSize: Int = 100.mb,
                                                 maxSegmentsToPush: Int = 5,
                                                 memoryLevelSegmentSize: Int = DefaultConfigs.segmentSize,
                                                 memoryLevelMaxKeyValuesCountPerSegment: Int = 200000,
                                                 persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                                                 otherDirs: Seq[Dir] = Seq.empty,
                                                 cacheKeyValueIds: Boolean = true,
                                                 mmapPersistentLevelAppendixLogs: MMAP.Log = DefaultConfigs.mmap(),
                                                 memorySegmentDeleteDelay: FiniteDuration = CommonConfigs.segmentDeleteDelay,
                                                 compactionConfig: CompactionConfig = CommonConfigs.compactionConfig(),
                                                 optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                                                 atomic: Atomic = CommonConfigs.atomic(),
                                                 acceleration: LevelZeroMeter => Accelerator = DefaultConfigs.accelerator,
                                                 persistentLevelSortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                                                 persistentLevelHashIndex: HashIndex = DefaultConfigs.hashIndex(),
                                                 binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                                                 mightContainIndex: MightContainIndex = DefaultConfigs.mightContainIndex(),
                                                 valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                                                 segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                                                 fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                                 memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                                                 threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10))(implicit serializer: Serializer[A],
                                                                                                                                                   functionClassTag: ClassTag[F],
                                                                                                                                                   functions: Functions[F],
                                                                                                                                                   bag: swaydb.Bag[BAG],
                                                                                                                                                   sequencer: Sequencer[BAG] = null,
                                                                                                                                                   byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                                                   typedKeyOrder: KeyOrder[A] = null,
                                                                                                                                                   buildValidator: BuildValidator = BuildValidator.DisallowOlderVersions(DataType.Set)): BAG[swaydb.Set[A, F, BAG]] =
    bag.suspend {
      implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
      implicit val unitSerializer: Serializer[Nothing] = Default.NothingSerializer
      val functionStore: FunctionStore = FunctionConverter.toFunctionsStore[A, Nothing, Apply.Set[Nothing], F](functions)

      val set =
        Core(
          enableTimer = PureFunction.isOn(functionClassTag),
          cacheKeyValueIds = cacheKeyValueIds,
          fileCache = fileCache,
          memoryCache = memoryCache,
          threadStateCache = threadStateCache,
          compactionConfig = compactionConfig,
          config =
            DefaultEventuallyPersistentConfig(
              dir = dir,
              otherDirs = otherDirs,
              logSize = logSize,
              appliedFunctionsLogSize = appliedFunctionsLogSize,
              clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
              maxMemoryLevelSize = maxMemoryLevelSize,
              maxSegmentsToPush = maxSegmentsToPush,
              memoryLevelMinSegmentSize = memoryLevelSegmentSize,
              memoryLevelMaxKeyValuesCountPerSegment = memoryLevelMaxKeyValuesCountPerSegment,
              memorySegmentDeleteDelay = memorySegmentDeleteDelay,
              persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
              mmapPersistentLevelAppendixLogs = mmapPersistentLevelAppendixLogs,
              persistentLevelSortedKeyIndex = persistentLevelSortedKeyIndex,
              persistentLevelHashIndex = persistentLevelHashIndex,
              persistentLevelBinarySearchIndex = binarySearchIndex,
              persistentLevelMightContainIndex = mightContainIndex,
              persistentLevelValuesConfig = valuesConfig,
              persistentLevelSegmentConfig = segmentConfig,
              acceleration = acceleration,
              optimiseWrites = optimiseWrites,
              atomic = atomic
            )
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
