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
import swaydb.CommonConfigs
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.util.Eithers
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config.{FileCache, ThreadStateCache}
import swaydb.data.order.KeyOrder
import swaydb.data.sequencer.Sequencer
import swaydb.data.slice.Slice
import swaydb.data.{Atomic, Functions, OptimiseWrites}
import swaydb.serializers.Serializer

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object SetMap extends LazyLogging {

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[K, V, BAG[_]](mapSize: Int = DefaultConfigs.mapSize,
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
                          threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10))(implicit keySerializer: Serializer[K],
                                                                                                                            valueSerializer: Serializer[V],
                                                                                                                            bag: swaydb.Bag[BAG],
                                                                                                                            sequencer: Sequencer[BAG] = null,
                                                                                                                            byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                            typedKeyOrder: KeyOrder[K] = null): BAG[swaydb.SetMap[K, V, BAG]] =
    bag.suspend {
      val serialiser: Serializer[(K, V)] = swaydb.SetMap.serialiser(keySerializer, valueSerializer)
      val keyOrder = Eithers.nullCheck(byteKeyOrder, typedKeyOrder, KeyOrder.default)
      val ordering: KeyOrder[Slice[Byte]] = swaydb.SetMap.ordering(keyOrder)

      val set =
        Set[(K, V), Nothing, BAG](
          mapSize = mapSize,
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          fileCache = fileCache,
          deleteDelay = deleteDelay,
          compactionConfig = compactionConfig,
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          acceleration = acceleration,
          levelZeroThrottle = levelZeroThrottle,
          lastLevelThrottle = lastLevelThrottle,
          threadStateCache = threadStateCache
        )(serializer = serialiser,
          functionClassTag = ClassTag.Nothing,
          bag = bag,
          sequencer = sequencer,
          functions = Functions.nothing,
          byteKeyOrder = ordering
        )

      bag.transform(set) {
        set =>
          swaydb.SetMap[K, V, BAG](set)
      }
    }
}
