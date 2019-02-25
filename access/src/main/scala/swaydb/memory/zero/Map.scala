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

package swaydb.memory.zero

import com.typesafe.scalalogging.LazyLogging
import scala.concurrent.ExecutionContext
import swaydb.SwayDB
import swaydb.configs.level.DefaultMemoryZeroConfig
import swaydb.core.BlockingCoreAPI
import swaydb.core.function.FunctionStore
import swaydb.data.IO
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer

object Map extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  /**
    * A single level zero only database. Does not need compaction.
    */

  def apply[K, V](mapSize: Int = 4.mb,
                  acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                     valueSerializer: Serializer[V],
                                                                                     keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                     ec: ExecutionContext = SwayDB.defaultExecutionContext): IO[swaydb.Map[K, V]] =
    BlockingCoreAPI(
      config = DefaultMemoryZeroConfig(
        mapSize = mapSize,
        acceleration = acceleration
      )
    ) map {
      core =>
        swaydb.Map[K, V](new SwayDB(core))
    }

}
