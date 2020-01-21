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

package swaydb.persistent

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.DefaultPersistentConfig
import swaydb.core.Core
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Error, IO, KeyOrderConverter, SwayDB}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.reflect.ClassTag

object Set extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit def functionStore: FunctionStore = FunctionStore.memory()

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[A, F, T[_]](dir: Path,
                        maxOpenSegments: Int = 1000,
                        memoryCacheSize: Int = 100.mb,
                        mapSize: Int = 4.mb,
                        mmapMaps: Boolean = true,
                        recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                        mmapAppendix: Boolean = true,
                        mmapSegments: MMAP = MMAP.WriteAndRead,
                        minSegmentSize: Int = 2.mb,
                        maxKeyValuesPerSegment: Int = 100000,
                        appendixFlushCheckpointSize: Int = 2.mb,
                        otherDirs: Seq[Dir] = Seq.empty,
                        memorySweeperPollInterval: FiniteDuration = 10.seconds,
                        fileSweeperPollInterval: FiniteDuration = 10.seconds,
                        mightContainFalsePositiveRate: Double = 0.01,
                        blockSize: Int = 4098,
                        compressDuplicateValues: Boolean = true,
                        deleteSegmentsEventually: Boolean = true,
                        acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[A],
                                                                                              functionClassTag: ClassTag[F],
                                                                                              tag: swaydb.Tag[T],
                                                                                              keyOrder: Either[KeyOrder[Slice[Byte]], KeyOrder[A]] = Left(KeyOrder.default),
                                                                                              fileSweeperEC: ExecutionContext = SwayDB.sweeperExecutionContext,
                                                                                              memorySweeperEC: ExecutionContext = SwayDB.sweeperExecutionContext): IO[Error.Boot, swaydb.Set[A, F, T]] = {
    implicit val bytesKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytes(keyOrder)

    Core(
      enableTimer = functionClassTag != ClassTag.Nothing,
      config = DefaultPersistentConfig(
        dir = dir,
        otherDirs = otherDirs,
        recoveryMode = recoveryMode,
        mapSize = mapSize,
        mmapMaps = mmapMaps,
        mmapSegments = mmapSegments,
        mmapAppendix = mmapAppendix,
        minSegmentSize = minSegmentSize,
        maxKeyValuesPerSegment = maxKeyValuesPerSegment,
        compressDuplicateValues = compressDuplicateValues,
        deleteSegmentsEventually = deleteSegmentsEventually,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        mightContainFalsePositiveRate = mightContainFalsePositiveRate,
        acceleration = acceleration
      ),
      fileCache =
        FileCache.Enable.default(
          maxOpen = maxOpenSegments,
          interval = fileSweeperPollInterval,
          ec = fileSweeperEC
        ),
      memoryCache =
        MemoryCache.Enabled.default(
          minIOSeekSize = blockSize,
          skipBlockCacheSeekSize = blockSize * 10,
          memorySize = memoryCacheSize,
          interval = memorySweeperPollInterval,
          ec = memorySweeperEC
        )
    ) map {
      db =>
        swaydb.Set[A, F, T](db.toTag)
    }
  }
}
