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
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.persistent

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.util.Eithers
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Error, IO}

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object SetMap extends LazyLogging {

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
                                                                                                         byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                         typedKeyOrder: KeyOrder[K] = null): IO[Error.Boot, swaydb.SetMap[K, V, F, BAG]] = {

    val serialiser: Serializer[(K, V)] = swaydb.SetMap.serialiser(keySerializer, valueSerializer)
    val nullCheckedOrder = Eithers.nullCheck(byteKeyOrder, typedKeyOrder, KeyOrder.default)
    val ordering: KeyOrder[Slice[Byte]] = swaydb.SetMap.ordering(nullCheckedOrder)

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
    )(serializer = serialiser,
      functionClassTag = functionClassTag,
      bag = bag,
      functions = functions,
      byteKeyOrder = ordering
    ) map {
      set =>
        swaydb.SetMap[K, V, F, BAG](set)
    }

  }
}
