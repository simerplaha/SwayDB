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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.eventually.persistent

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.{DefaultEventuallyPersistentConfig, DefaultExecutionContext}
import swaydb.core.Core
import swaydb.core.build.BuildValidator
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.config.{ThreadStateCache, _}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.sequencer.Sequencer
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.data.{Atomic, DataType, Functions, OptimiseWrites}
import swaydb.function.FunctionConverter
import swaydb.serializers.Serializer
import swaydb.{Apply, KeyOrderConverter, PureFunction}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

object Map extends LazyLogging {

  /**
   * A 3 Leveled in-memory database where the 3rd is persistent.
   *
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[K, V, F <: PureFunction.Map[K, V], BAG[_]](dir: Path,
                                                       mapSize: Int = 4.mb,
                                                       appliedFunctionsMapSize: Int = 4.mb,
                                                       clearAppliedFunctionsOnBoot: Boolean = false,
                                                       maxMemoryLevelSize: Int = 100.mb,
                                                       maxSegmentsToPush: Int = 5,
                                                       memoryLevelSegmentSize: Int = 2.mb,
                                                       memoryLevelMaxKeyValuesCountPerSegment: Int = 200000,
                                                       persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                                                       otherDirs: Seq[Dir] = Seq.empty,
                                                       cacheKeyValueIds: Boolean = true,
                                                       mmapPersistentLevelAppendix: MMAP.Map = DefaultConfigs.mmap(),
                                                       deleteMemorySegmentsEventually: Boolean = true,
                                                       mergeParallelism: Int = DefaultConfigs.mergeParallelism(),
                                                       optimiseWrites: OptimiseWrites = DefaultConfigs.optimiseWrites(),
                                                       atomic: Atomic = DefaultConfigs.atomic(),
                                                       acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes(),
                                                       persistentLevelSortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                                                       persistentLevelRandomSearchIndex: RandomSearchIndex = DefaultConfigs.randomSearchIndex(),
                                                       binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                                                       mightContainIndex: MightContainIndex = DefaultConfigs.mightContainIndex(),
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
                                                                                                                                                         compactionEC: ExecutionContext = DefaultExecutionContext.compactionEC(mergeParallelism),
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
          config =
            DefaultEventuallyPersistentConfig(
              dir = dir,
              otherDirs = otherDirs,
              mapSize = mapSize,
              appliedFunctionsMapSize = appliedFunctionsMapSize,
              clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
              maxMemoryLevelSize = maxMemoryLevelSize,
              maxSegmentsToPush = maxSegmentsToPush,
              memoryLevelMergeParallelism = mergeParallelism,
              memoryLevelMinSegmentSize = memoryLevelSegmentSize,
              memoryLevelMaxKeyValuesCountPerSegment = memoryLevelMaxKeyValuesCountPerSegment,
              deleteMemorySegmentsEventually = deleteMemorySegmentsEventually,
              persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
              mmapPersistentLevelAppendix = mmapPersistentLevelAppendix,
              persistentLevelMergeParallelism = mergeParallelism,
              persistentLevelSortedKeyIndex = persistentLevelSortedKeyIndex,
              persistentLevelRandomSearchIndex = persistentLevelRandomSearchIndex,
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
            swaydb.Map[K, V, F, BAG](core.toBag(sequencer))
        }

      map.toBag[BAG]
    }
}
