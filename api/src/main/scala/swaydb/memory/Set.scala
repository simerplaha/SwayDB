/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with SwayDB. If not, see <https://www.gnu.org/licenses/>.
 */

package swaydb.memory

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.DefaultMemoryConfig
import swaydb.core.Core
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.api.grouping.GroupBy
import swaydb.data.config.{ActorConfig, FileCache, MemoryCache}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Error, IO, SwayDB}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}

object Set extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[T](mapSize: Int = 4.mb,
               maxOpenSegments: Int = 100,
               segmentSize: Int = 2.mb,
               memoryCacheSize: Int = 500.mb, //cacheSize for memory database is used for evicting decompressed key-values
               memorySweeperPollInterval: FiniteDuration = 10.seconds,
               fileSweeperPollInterval: FiniteDuration = 10.seconds,
               mightContainFalsePositiveRate: Double = 0.01,
               compressDuplicateValues: Boolean = false,
               deleteSegmentsEventually: Boolean = true,
               groupBy: Option[GroupBy.KeyValues] = None,
               acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                     keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                     fileSweeperEC: ExecutionContext = SwayDB.defaultExecutionContext,
                                                                                     memorySweeperEC: ExecutionContext = SwayDB.defaultExecutionContext): IO[Error.Boot, swaydb.Set[T, IO.ApiIO]] =
    Core(
      config = DefaultMemoryConfig(
        mapSize = mapSize,
        segmentSize = segmentSize,
        mightContainFalsePositiveRate = mightContainFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues,
        deleteSegmentsEventually = deleteSegmentsEventually,
        keyValueGroupBy = groupBy,
        acceleration = acceleration
      ),
      fileCache =
        FileCache.Enable.default(
          maxOpen = maxOpenSegments,
          interval = fileSweeperPollInterval,
          ec = fileSweeperEC
        ),
      memoryCache =
        MemoryCache.EnableKeyValueCache(
          capacity = memoryCacheSize,
          actorConfig = ActorConfig.Timer(
            delay = memorySweeperPollInterval,
            ec = memorySweeperEC
          )
        )
    ) map {
      db =>
        swaydb.Set[T](db)
    }
}
