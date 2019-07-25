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

package swaydb.persistent.zero

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.DefaultPersistentZeroConfig
import swaydb.core.BlockingCore
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.config.{Dir, RecoveryMode}
import swaydb.Error
import swaydb.data.io.Tag
import swaydb.data.io.Tag.CoreIO
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{IO, SwayDB}

import scala.concurrent.ExecutionContext

object Map extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  def apply[K, V](dir: Path,
                  mapSize: Int = 4.mb,
                  mmapMaps: Boolean = true,
                  recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                  otherDirs: Seq[Dir] = Seq.empty,
                  acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                        valueSerializer: Serializer[V],
                                                                                        keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                        ec: ExecutionContext = SwayDB.defaultExecutionContext): IO[Error.BootUp, swaydb.Map[K, V, CoreIO]] =
    BlockingCore(
      config = DefaultPersistentZeroConfig(
        dir = dir,
        otherDirs = otherDirs,
        mapSize = mapSize, mmapMaps = mmapMaps,
        recoveryMode = recoveryMode,
        acceleration = acceleration
      )
    ) map {
      db =>
        swaydb.Map[K, V, Tag.CoreIO](db)
    }
}
