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
import swaydb.config.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.config.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.config.sequencer.Sequencer
import swaydb.config._
import swaydb.configs.level.DefaultExecutionContext
import swaydb.serializers.Serializer
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.utils.Eithers

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object SetMap extends LazyLogging {

  /**
   * For custom configurations read documentation on website: https://swaydb.simer.au/configuring-levels
   */
  def apply[K, V, BAG[_]](logSize: Int = DefaultConfigs.logSize,
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
          logSize = logSize,
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
