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

package swaydb.persistent

import com.typesafe.scalalogging.LazyLogging
import java.nio.file.Path
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.util.Try
import swaydb.configs.level.{DefaultGroupingStrategy, DefaultPersistentConfig}
import swaydb.core.CoreAPI
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Set, SwayDB}

object Set extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  /**
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    */
  def apply[T](dir: Path,
               maxOpenSegments: Int = 1000,
               cacheSize: Int = 100.mb,
               mapSize: Int = 4.mb,
               mmapMaps: Boolean = true,
               recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
               mmapAppendix: Boolean = true,
               mmapSegments: MMAP = MMAP.WriteAndRead,
               segmentSize: Int = 2.mb,
               appendixFlushCheckpointSize: Int = 2.mb,
               otherDirs: Seq[Dir] = Seq.empty,
               cacheCheckDelay: FiniteDuration = 7.seconds,
               segmentsOpenCheckDelay: FiniteDuration = 5.seconds,
               bloomFilterFalsePositiveRate: Double = 0.01,
               compressDuplicateValues: Boolean = true,
               lastLevelGroupingStrategy: Option[KeyValueGroupingStrategy] = Some(DefaultGroupingStrategy()),
               acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                  keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                  ec: ExecutionContext = SwayDB.defaultExecutionContext): Try[Set[T]] = {
    CoreAPI(
      config = DefaultPersistentConfig(
        dir = dir,
        otherDirs = otherDirs,
        recoveryMode = recoveryMode,
        mapSize = mapSize,
        mmapMaps = mmapMaps,
        mmapSegments = mmapSegments,
        mmapAppendix = mmapAppendix,
        segmentSize = segmentSize,
        compressDuplicateValues = compressDuplicateValues,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        groupingStrategy = lastLevelGroupingStrategy,
        acceleration = acceleration
      ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        swaydb.Set[T](new SwayDB(core))
    }
  }

}
