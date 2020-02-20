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

package swaydb.memory

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{FileCache, ThreadStateCache}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Error, IO}

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object SetMap extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  implicit def functionStore: FunctionStore = FunctionStore.memory()

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[K, V, F, BAG[_]](mapSize: Int = 4.mb,
                             minSegmentSize: Int = 2.mb,
                             maxKeyValuesPerSegment: Int = 200000,
                             fileCache: FileCache.Enable = DefaultConfigs.fileCache(),
                             deleteSegmentsEventually: Boolean = true,
                             acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes(),
                             levelZeroThrottle: LevelZeroMeter => FiniteDuration = DefaultConfigs.levelZeroThrottle,
                             lastLevelThrottle: LevelMeter => Throttle = DefaultConfigs.lastLevelThrottle,
                             threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10))(implicit keySerializer: Serializer[K],
                                                                                                                               valueSerializer: Serializer[V],
                                                                                                                               functionClassTag: ClassTag[F],
                                                                                                                               bag: swaydb.Bag[BAG],
                                                                                                                               keyOrder: Either[KeyOrder[Slice[Byte]], KeyOrder[K]] = Left(KeyOrder.default)): IO[Error.Boot, swaydb.SetMap[K, V, F, BAG]] = {
    implicit val serialiser: Serializer[(K, V)] = swaydb.SetMap.serialiser(keySerializer, valueSerializer)
    implicit val ordering: Left[KeyOrder[Slice[Byte]], Nothing] = Left(swaydb.SetMap.ordering(keyOrder))

    Set[(K, V), F, BAG](
      mapSize = mapSize,
      minSegmentSize = minSegmentSize,
      maxKeyValuesPerSegment = maxKeyValuesPerSegment,
      fileCache = fileCache,
      deleteSegmentsEventually = deleteSegmentsEventually,
      acceleration = acceleration,
      levelZeroThrottle = levelZeroThrottle,
      lastLevelThrottle = lastLevelThrottle,
      threadStateCache = threadStateCache
    ) map {
      set =>
        swaydb.SetMap[K, V, F, BAG](set)
    }
  }
}
