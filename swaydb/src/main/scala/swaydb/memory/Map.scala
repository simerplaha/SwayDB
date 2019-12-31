/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb.memory

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.DefaultMemoryConfig
import swaydb.core.Core
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.config.{FileCache, MemoryCache}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{IO, KeyOrderConverter, SwayDB}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.reflect.ClassTag

object Map extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def functionStore: FunctionStore = FunctionStore.memory()

  def apply[K, V, F, T[_]](mapSize: Int = 4.mb,
                           segmentSize: Int = 2.mb,
                           memoryCacheSize: Int = 500.mb,
                           maxOpenSegments: Int = 100,
                           maxCachedKeyValuesPerSegment: Int = 10,
                           fileSweeperPollInterval: FiniteDuration = 10.seconds,
                           mightContainFalsePositiveRate: Double = 0.01,
                           deleteSegmentsEventually: Boolean = true,
                           acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                                 valueSerializer: Serializer[V],
                                                                                                 functionClassTag: ClassTag[F],
                                                                                                 tag: swaydb.Tag[T],
                                                                                                 keyOrder: Either[KeyOrder[Slice[Byte]], KeyOrder[K]] = Left(KeyOrder.default),
                                                                                                 fileSweeperEC: ExecutionContext = SwayDB.sweeperExecutionContext): IO[swaydb.Error.Boot, swaydb.Map[K, V, F, T]] = {
    implicit val bytesKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytes(keyOrder)

    Core(
      enableTimer = functionClassTag != ClassTag.Nothing,
      config = DefaultMemoryConfig(
        mapSize = mapSize,
        segmentSize = segmentSize,
        mightContainFalsePositiveRate = mightContainFalsePositiveRate,
        deleteSegmentsEventually = deleteSegmentsEventually,
        acceleration = acceleration
      ),
      fileCache =
        FileCache.Enable.default(
          maxOpen = maxOpenSegments,
          interval = fileSweeperPollInterval,
          ec = fileSweeperEC
        ),
      memoryCache =
        MemoryCache.KeyValueCacheOnly(
          cacheCapacity = memoryCacheSize,
          maxCachedKeyValueCountPerSegment = Some(maxCachedKeyValuesPerSegment),
          memorySweeper = None
        )
    ) map {
      db =>
        swaydb.Map[K, V, F, T](db.toTag)
    }
  }
}
