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
 */

package swaydb.persistent

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Error, IO}

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object SetMap extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  def apply[K, V, F, BAG[_]](dir: Path,
                             mapSize: Int = 4.mb,
                             mmapMaps: Boolean = true,
                             recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                             mmapAppendix: Boolean = true,
                             appendixFlushCheckpointSize: Int = 2.mb,
                             otherDirs: Seq[Dir] = Seq.empty,
                             cacheKeyValueIds: Boolean = true,
                             acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes(),
                             threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                             sortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                             randomKeyIndex: RandomKeyIndex = DefaultConfigs.randomKeyIndex(),
                             binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                             mightContainKeyIndex: MightContainIndex = DefaultConfigs.mightContainKeyIndex(),
                             valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                             segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                             fileCache: FileCache.Enable = DefaultConfigs.fileCache(),
                             memoryCache: MemoryCache = DefaultConfigs.memoryCache,
                             levelZeroThrottle: LevelZeroMeter => FiniteDuration = DefaultConfigs.levelZeroThrottle,
                             levelOneThrottle: LevelMeter => Throttle = DefaultConfigs.levelOneThrottle,
                             levelTwoThrottle: LevelMeter => Throttle = DefaultConfigs.levelTwoThrottle,
                             levelThreeThrottle: LevelMeter => Throttle = DefaultConfigs.levelThreeThrottle,
                             levelFourThrottle: LevelMeter => Throttle = DefaultConfigs.levelFourThrottle,
                             levelFiveThrottle: LevelMeter => Throttle = DefaultConfigs.levelFiveThrottle,
                             levelSixThrottle: LevelMeter => Throttle = DefaultConfigs.levelSixThrottle)(implicit keySerializer: Serializer[K],
                                                                                                         valueSerializer: Serializer[V],
                                                                                                         functionClassTag: ClassTag[F],
                                                                                                         bag: swaydb.Bag[BAG],
                                                                                                         functions: swaydb.Set.Functions[(K, V), F],
                                                                                                         keyOrder: Either[KeyOrder[Slice[Byte]], KeyOrder[K]] = Left(KeyOrder.default)): IO[Error.Boot, swaydb.SetMap[K, V, F, BAG]] = {

    implicit val serialiser: Serializer[(K, V)] = swaydb.SetMap.serialiser(keySerializer, valueSerializer)
    implicit val ordering: Left[KeyOrder[Slice[Byte]], Nothing] = Left(swaydb.SetMap.ordering(keyOrder))

    Set[(K, V), F, BAG](
      dir = dir,
      mapSize = mapSize,
      mmapMaps = mmapMaps,
      recoveryMode = recoveryMode,
      mmapAppendix = mmapAppendix,
      appendixFlushCheckpointSize = appendixFlushCheckpointSize,
      otherDirs = otherDirs,
      cacheKeyValueIds = cacheKeyValueIds,
      acceleration = acceleration,
      threadStateCache = threadStateCache,
      sortedKeyIndex = sortedKeyIndex,
      randomKeyIndex = randomKeyIndex,
      binarySearchIndex = binarySearchIndex,
      mightContainKeyIndex = mightContainKeyIndex,
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
    ) map {
      set =>
        swaydb.SetMap[K, V, F, BAG](set)
    }

  }
}