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
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.build.BuildValidator
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.sequencer.Sequencer
import swaydb.data.slice.Slice
import swaydb.data.{Atomic, DataType, OptimiseWrites}
import swaydb.effect.Dir
import swaydb.serializers.Serializer
import swaydb.utils.StorageUnits._
import swaydb.{Bag, CommonConfigs}

import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration

object Queue extends LazyLogging {

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[A, BAG[_]](dir: Path,
                       mapSize: Int = DefaultConfigs.mapSize,
                       mmapMaps: MMAP.Map = DefaultConfigs.mmap(),
                       recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                       mmapAppendix: MMAP.Map = DefaultConfigs.mmap(),
                       appendixFlushCheckpointSize: Int = 2.mb,
                       otherDirs: Seq[Dir] = Seq.empty,
                       cacheKeyValueIds: Boolean = true,
                       compactionConfig: CompactionConfig = CommonConfigs.compactionConfig(),
                       acceleration: LevelZeroMeter => Accelerator = DefaultConfigs.accelerator,
                       threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                       optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                       atomic: Atomic = CommonConfigs.atomic(),
                       sortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                       randomSearchIndex: RandomSearchIndex = DefaultConfigs.randomSearchIndex(),
                       binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                       mightContainIndex: MightContainIndex = DefaultConfigs.mightContainIndex(),
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
                                                                                                        bag: Bag[BAG],
                                                                                                        sequencer: Sequencer[BAG] = null,
                                                                                                        buildValidator: BuildValidator = BuildValidator.DisallowOlderVersions(DataType.Queue)): BAG[swaydb.Queue[A]] =
    bag.suspend {
      implicit val queueSerialiser: Serializer[(Long, A)] =
        swaydb.Queue.serialiser[A](serializer)

      implicit val keyOrder: KeyOrder[Slice[Byte]] =
        swaydb.Queue.ordering

      val set =
        Set[(Long, A), Nothing, BAG](
          dir = dir,
          mapSize = mapSize,
          mmapMaps = mmapMaps,
          recoveryMode = recoveryMode,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          otherDirs = otherDirs,
          cacheKeyValueIds = cacheKeyValueIds,
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          compactionConfig = compactionConfig,
          acceleration = acceleration,
          threadStateCache = threadStateCache,
          sortedKeyIndex = sortedKeyIndex,
          randomSearchIndex = randomSearchIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainIndex = mightContainIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
          levelZeroThrottle = levelZeroThrottle,
          levelOneThrottle = levelOneThrottle,
          levelTwoThrottle = levelTwoThrottle,
          levelThreeThrottle = levelThreeThrottle,
          levelFourThrottle = levelFourThrottle,
          levelFiveThrottle = levelFiveThrottle,
          levelSixThrottle = levelSixThrottle
        )

      bag.flatMap(set)(set => swaydb.Queue.fromSet(set))
    }
}
