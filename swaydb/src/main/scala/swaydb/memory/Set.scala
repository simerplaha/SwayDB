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

package swaydb.memory

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.{DefaultExecutionContext, DefaultMemoryConfig}
import swaydb.core.Core
import swaydb.core.build.BuildValidator
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config.{FileCache, MemoryCache, ThreadStateCache}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.sequencer.Sequencer
import swaydb.data.slice.Slice
import swaydb.data.{Atomic, DataType, Functions, OptimiseWrites}
import swaydb.function.FunctionConverter
import swaydb.serializers.{Default, Serializer}
import swaydb.{Apply, CommonConfigs, KeyOrderConverter, PureFunction}

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object Set extends LazyLogging {

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[A, F <: PureFunction.Set[A], BAG[_]](logSize: Int = DefaultConfigs.logSize,
                                                 minSegmentSize: Int = DefaultConfigs.segmentSize,
                                                 maxKeyValuesPerSegment: Int = Int.MaxValue,
                                                 fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                                 deleteDelay: FiniteDuration = CommonConfigs.segmentDeleteDelay,
                                                 compactionConfig: CompactionConfig = CommonConfigs.compactionConfig(),
                                                 optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                                                 atomic: Atomic = CommonConfigs.atomic(),
                                                 acceleration: LevelZeroMeter => Accelerator = DefaultConfigs.accelerator,
                                                 levelZeroThrottle: LevelZeroMeter => LevelZeroThrottle = DefaultConfigs.levelZeroThrottle,
                                                 lastLevelThrottle: LevelMeter => LevelThrottle = DefaultConfigs.lastLevelThrottle,
                                                 threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10))(implicit serializer: Serializer[A],
                                                                                                                                                   functionClassTag: ClassTag[F],
                                                                                                                                                   functions: Functions[F],
                                                                                                                                                   bag: swaydb.Bag[BAG],
                                                                                                                                                   sequencer: Sequencer[BAG] = null,
                                                                                                                                                   byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                                                   typedKeyOrder: KeyOrder[A] = null): BAG[swaydb.Set[A, F, BAG]] =
    bag.suspend {
      val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
      implicit val unitSerializer: Serializer[Nothing] = Default.NothingSerializer
      val functionStore: FunctionStore = FunctionConverter.toFunctionsStore[A, Nothing, Apply.Set[Nothing], F](functions)

      val set =
        Core(
          enableTimer = PureFunction.isOn(functionClassTag),
          cacheKeyValueIds = false,
          threadStateCache = threadStateCache,
          compactionConfig = compactionConfig,
          config =
            DefaultMemoryConfig(
              logSize = logSize,
              appliedFunctionsLogSize = 0, //memory does not use appliedFunctions Map
              clearAppliedFunctionsOnBoot = false,
              minSegmentSize = minSegmentSize,
              maxKeyValuesPerSegment = maxKeyValuesPerSegment,
              deleteDelay = deleteDelay,
              acceleration = acceleration,
              levelZeroThrottle = levelZeroThrottle,
              lastLevelThrottle = lastLevelThrottle,
              optimiseWrites = optimiseWrites,
              atomic = atomic
            ),
          fileCache = fileCache,
          memoryCache = MemoryCache.Off
        )(keyOrder = keyOrder,
          timeOrder = TimeOrder.long,
          functionStore = functionStore,
          buildValidator = BuildValidator.DisallowOlderVersions(DataType.Set)
        ) map {
          core =>
            swaydb.Set[A, F, BAG](core.toBag(sequencer))
        }

      set.toBag[BAG]
    }
}
