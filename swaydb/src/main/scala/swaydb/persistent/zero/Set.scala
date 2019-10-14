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
import swaydb.core.Core
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.config.{Dir, RecoveryMode}
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Error, IO, KeyOrderConverter, SwayDB}

import scala.concurrent.ExecutionContext
import scala.reflect.ClassTag

object Set extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  def apply[A, F, T[_]](dir: Path,
                        mapSize: Int = 4.mb,
                        mmapMaps: Boolean = true,
                        recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                        otherDirs: Seq[Dir] = Seq.empty,
                        acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[A],
                                                                                              functionClassTag: ClassTag[F],
                                                                                              tag: swaydb.Tag[T],
                                                                                              keyOrder: Either[KeyOrder[Slice[Byte]], KeyOrder[A]] = Left(KeyOrder.default),
                                                                                              ec: Option[ExecutionContext] = Some(SwayDB.defaultExecutionContext)): IO[Error.Boot, swaydb.Set[A, F, T]] = {
    implicit val bytesKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytes(keyOrder)
    
    Core(
      enableTimer = functionClassTag != ClassTag.Nothing,
      config = DefaultPersistentZeroConfig(
        dir = dir,
        otherDirs = otherDirs,
        recoveryMode = recoveryMode,
        mapSize = mapSize,
        mmapMaps = mmapMaps,
        acceleration = acceleration
      )
    ) map {
      db =>
        swaydb.Set[A, F, T](db.toTag)
    }
  }
}
