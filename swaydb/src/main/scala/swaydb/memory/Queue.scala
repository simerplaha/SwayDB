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
import swaydb.configs.level.DefaultExecutionContext
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.sequencer.Sequencer
import swaydb.data.slice.Slice
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.serializers.Serializer
import swaydb.{Bag, CommonConfigs}

import scala.concurrent.duration.FiniteDuration

object Queue extends LazyLogging {

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[A, BAG[_]](mapSize: Int = DefaultConfigs.mapSize,
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
                                                                                                                         bag: Bag[BAG],
                                                                                                                         sequencer: Sequencer[BAG] = null): BAG[swaydb.Queue[A]] =
    bag.suspend {
      implicit val queueSerialiser: Serializer[(Long, A)] =
        swaydb.Queue.serialiser[A](serializer)

      implicit val keyOrder: KeyOrder[Slice[Byte]] =
        swaydb.Queue.ordering

      val set =
        Set[(Long, A), Nothing, BAG](
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
        )

      bag.flatMap(set)(set => swaydb.Queue.fromSet(set))
    }
}
