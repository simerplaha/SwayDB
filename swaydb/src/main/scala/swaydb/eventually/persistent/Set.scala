/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
                                                 mapSize: Int = DefaultConfigs.mapSize,
                                                 appliedFunctionsMapSize: Int = 4.mb,
                                                 clearAppliedFunctionsOnBoot: Boolean = false,
                                                 maxMemoryLevelSize: Int = 100.mb,
                                                 maxSegmentsToPush: Int = 5,
                                                 memoryLevelSegmentSize: Int = DefaultConfigs.segmentSize,
                                                 memoryLevelMaxKeyValuesCountPerSegment: Int = 200000,
                                                 persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                                                 otherDirs: Seq[Dir] = Seq.empty,
                                                 cacheKeyValueIds: Boolean = true,
                                                 mmapPersistentLevelAppendix: MMAP.Map = DefaultConfigs.mmap(),
                                                 memorySegmentDeleteDelay: FiniteDuration = CommonConfigs.segmentDeleteDelay,
                                                 compactionConfig: CompactionConfig = CommonConfigs.compactionConfig(),
                                                 optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                                                 atomic: Atomic = CommonConfigs.atomic(),
                                                 acceleration: LevelZeroMeter => Accelerator = DefaultConfigs.accelerator,
                                                 persistentLevelSortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                                                 persistentLevelRandomSearchIndex: RandomSearchIndex = DefaultConfigs.randomSearchIndex(),
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
              mapSize = mapSize,
              appliedFunctionsMapSize = appliedFunctionsMapSize,
              clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
              maxMemoryLevelSize = maxMemoryLevelSize,
              maxSegmentsToPush = maxSegmentsToPush,
              memoryLevelMinSegmentSize = memoryLevelSegmentSize,
              memoryLevelMaxKeyValuesCountPerSegment = memoryLevelMaxKeyValuesCountPerSegment,
              memorySegmentDeleteDelay = memorySegmentDeleteDelay,
              persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
              mmapPersistentLevelAppendix = mmapPersistentLevelAppendix,
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
            swaydb.Set[A, F, BAG](core.toBag(sequencer))
        }

      set.toBag[BAG]
    }
}
