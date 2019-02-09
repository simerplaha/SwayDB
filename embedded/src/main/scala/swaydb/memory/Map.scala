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
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import swaydb.data.io.IO
import swaydb.configs.level.DefaultMemoryConfig
import swaydb.core.CoreBlockingAPI
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Map, SwayDB}

object Map extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  /**
    * A 2 Leveled (Level0 & Level1), in-memory database.
    *
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    *
    * @param mapSize         size of Level0 maps before they are converted into Segments
    * @param segmentSize     size of Level1 Segments
    * @param acceleration    Controls the write speed.
    * @param keySerializer   Converts keys to Bytes
    * @param valueSerializer Converts values to Bytes
    * @param ordering        Sort order for keys
    * @param ec
    * @tparam K
    * @tparam V
    * @return
    */

  def apply[K, V](mapSize: Int = 4.mb,
                  segmentSize: Int = 2.mb,
                  cacheSize: Int = 500.mb,
                  cacheCheckDelay: FiniteDuration = 7.seconds,
                  bloomFilterFalsePositiveRate: Double = 0.01,
                  compressDuplicateValues: Boolean = false,
                  deleteSegmentsEventually: Boolean = true,
                  groupingStrategy: Option[KeyValueGroupingStrategy] = None,
                  acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                     valueSerializer: Serializer[V],
                                                                                     keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                     ec: ExecutionContext = SwayDB.defaultExecutionContext): IO[Map[K, V]] =
    CoreBlockingAPI(
      config = DefaultMemoryConfig(
        mapSize = mapSize,
        segmentSize = segmentSize,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues,
        deleteSegmentsEventually = deleteSegmentsEventually,
        groupingStrategy = groupingStrategy,
        acceleration = acceleration
      ),
      maxOpenSegments = 0,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      //memory Segments are never closed.
      segmentsOpenCheckDelay = Duration.Zero
    ) map {
      core =>
        swaydb.Map[K, V](new SwayDB(core))
    }

}
