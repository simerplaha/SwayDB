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
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.build.BuildValidator
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.CompactionConfig
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
   *
   *
   */
  def apply[A, BAG[_]](dir: Path,
                       mapSize: Int = DefaultConfigs.mapSize,
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
          maxMemoryLevelSize = maxMemoryLevelSize,
          maxSegmentsToPush = maxSegmentsToPush,
          memoryLevelSegmentSize = memoryLevelSegmentSize,
          memoryLevelMaxKeyValuesCountPerSegment = memoryLevelMaxKeyValuesCountPerSegment,
          persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
          otherDirs = otherDirs,
          cacheKeyValueIds = cacheKeyValueIds,
          mmapPersistentLevelAppendix = mmapPersistentLevelAppendix,
          memorySegmentDeleteDelay = memorySegmentDeleteDelay,
          compactionConfig = compactionConfig,
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          acceleration = acceleration,
          persistentLevelSortedKeyIndex = persistentLevelSortedKeyIndex,
          persistentLevelRandomSearchIndex = persistentLevelRandomSearchIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainIndex = mightContainIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
          threadStateCache = threadStateCache
        )

      bag.flatMap(set)(set => swaydb.Queue.fromSet(set))
    }
}
