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
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.CompactionConfig
import swaydb.data.config._
import swaydb.slice.order.{KeyOrder, TimeOrder}
import swaydb.data.sequencer.Sequencer
import swaydb.slice.Slice
import swaydb.data.{Atomic, DataType, Functions, OptimiseWrites}
import swaydb.effect.Dir
import swaydb.function.FunctionConverter
import swaydb.serializers.Serializer
import swaydb.utils.StorageUnits._
import swaydb.{Apply, CommonConfigs, KeyOrderConverter, PureFunction}

import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object Map extends LazyLogging {

  /**
   * A 3 Leveled in-memory database where the 3rd is persistent.
   *
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[K, V, F <: PureFunction.Map[K, V], BAG[_]](dir: Path,
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
                                                       persistentLevelSortedIndex: SortedIndex = DefaultConfigs.sortedIndex(),
                                                       persistentLevelHashIndex: HashIndex = DefaultConfigs.hashIndex(),
                                                       binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                                                       bloomFilter: BloomFilter = DefaultConfigs.bloomFilter(),
                                                       valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                                                       segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                                                       fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                                       memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                                                       threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10))(implicit keySerializer: Serializer[K],
                                                                                                                                                         valueSerializer: Serializer[V],
                                                                                                                                                         functionClassTag: ClassTag[F],
                                                                                                                                                         functions: Functions[F],
                                                                                                                                                         bag: swaydb.Bag[BAG],
                                                                                                                                                         sequencer: Sequencer[BAG] = null,
                                                                                                                                                         byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                                                         typedKeyOrder: KeyOrder[K] = null,
                                                                                                                                                         buildValidator: BuildValidator = BuildValidator.DisallowOlderVersions(DataType.Map)): BAG[swaydb.Map[K, V, F, BAG]] =
    bag.suspend {

      implicit val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
      val functionStore = FunctionConverter.toFunctionsStore[K, V, Apply.Map[V], F](functions)

      val map =
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
              persistentLevelSortedIndex = persistentLevelSortedIndex,
              persistentLevelHashIndex = persistentLevelHashIndex,
              persistentLevelBinarySearchIndex = binarySearchIndex,
              persistentLevelBloomFilter = bloomFilter,
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
            swaydb.Map[K, V, F, BAG](core.toBag(sequencer))
        }

      map.toBag[BAG]
    }
}
