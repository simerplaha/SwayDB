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

package swaydb.memory

import com.typesafe.scalalogging.LazyLogging
import swaydb.KeyOrderConverter
import swaydb.configs.level.{DefaultExecutionContext, DefaultMemoryConfig}
import swaydb.core.Core
import swaydb.core.build.BuildValidator
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{FileCache, MemoryCache, ThreadStateCache}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.reflect.ClassTag

object Set extends LazyLogging {

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[A, F, BAG[_]](mapSize: Int = 4.mb,
                          minSegmentSize: Int = 2.mb,
                          maxKeyValuesPerSegment: Int = Int.MaxValue,
                          fileCache: FileCache.Enable = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                          deleteSegmentsEventually: Boolean = true,
                          shutdownTimeout: FiniteDuration = 30.seconds,
                          acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes(),
                          levelZeroThrottle: LevelZeroMeter => FiniteDuration = DefaultConfigs.levelZeroThrottle,
                          lastLevelThrottle: LevelMeter => Throttle = DefaultConfigs.lastLevelThrottle,
                          threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10))(implicit serializer: Serializer[A],
                                                                                                                            functionClassTag: ClassTag[F],
                                                                                                                            bag: swaydb.Bag[BAG],
                                                                                                                            functions: swaydb.Set.Functions[A, F],
                                                                                                                            byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                            typedKeyOrder: KeyOrder[A] = null,
                                                                                                                            compactionEC: ExecutionContext = DefaultExecutionContext.compactionEC): BAG[swaydb.Set[A, F, BAG]] =
    bag.suspend {
      val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
      val coreFunctions: FunctionStore.Memory = functions.core

      val set =
        Core(
          enableTimer = functionClassTag != ClassTag.Nothing,
          cacheKeyValueIds = false,
          threadStateCache = threadStateCache,
          shutdownTimeout = shutdownTimeout,
          config =
            DefaultMemoryConfig(
              mapSize = mapSize,
              minSegmentSize = minSegmentSize,
              maxKeyValuesPerSegment = maxKeyValuesPerSegment,
              deleteSegmentsEventually = deleteSegmentsEventually,
              levelZeroThrottle = levelZeroThrottle,
              lastLevelThrottle = lastLevelThrottle,
              acceleration = acceleration
            ),
          fileCache = fileCache,
          memoryCache = MemoryCache.Disable
        )(keyOrder = keyOrder,
          timeOrder = TimeOrder.long,
          functionStore = coreFunctions,
          buildValidator = BuildValidator.DisallowOlderVersions
        ) map {
          db =>
            swaydb.Set[A, F, BAG](db.toBag)
        }

      set.toBag[BAG]
    }
}
