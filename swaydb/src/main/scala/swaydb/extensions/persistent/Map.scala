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

package swaydb.extensions.persistent

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
import swaydb.extensions.{Extend, Key}
import swaydb.serializers.Serializer
import swaydb.{Error, IO, KeyOrderConverter, SwayDB, extensions}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.reflect.ClassTag

object Map extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  def apply[K, V, F](dir: Path,
                     maxOpenSegments: Int = 1000,
                     memoryCacheSize: Int = 100.mb,
                     blockSize: Int = 4098,
                     mapSize: Int = 4.mb,
                     mmapMaps: Boolean = true,
                     recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                     mmapAppendix: Boolean = true,
                     mmapSegments: MMAP = MMAP.WriteAndRead,
                     segmentSize: Int = 2.mb,
                     appendixFlushCheckpointSize: Int = 2.mb,
                     otherDirs: Seq[Dir] = Seq.empty,
                     memorySweeperPollInterval: FiniteDuration = 10.seconds,
                     fileSweeperPollInterval: FiniteDuration = 10.seconds,
                     mightContainFalsePositiveRate: Double = 0.01,
                     compressDuplicateValues: Boolean = true,
                     deleteSegmentsEventually: Boolean = true,
                     acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                           valueSerializer: Serializer[V],
                                                                                           functionClassTag: ClassTag[F],
                                                                                           keyOrder: Either[KeyOrder[Slice[Byte]], KeyOrder[K]] = Left(KeyOrder.default),
                                                                                           fileSweeperEC: ExecutionContext = SwayDB.defaultExecutionContext,
                                                                                           memorySweeperEC: ExecutionContext = SwayDB.defaultExecutionContext): IO[Error.Boot, IO.ApiIO[extensions.Map[K, V]]] = {
    implicit val bytesKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytes(keyOrder)

    Core(
      enableTimer = functionClassTag != ClassTag.Nothing,
      config = DefaultPersistentConfig(
        dir = dir,
        otherDirs = otherDirs,
        mapSize = mapSize, mmapMaps = mmapMaps,
        mmapSegments = mmapSegments,
        recoveryMode = recoveryMode,
        mmapAppendix = mmapAppendix,
        segmentSize = segmentSize,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        mightContainFalsePositiveRate = mightContainFalsePositiveRate,
        compressDuplicateValues = compressDuplicateValues,
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
        MemoryCache.Enabled.default(
          minIOSeekSize = blockSize,
          memorySize = memoryCacheSize,
          interval = memorySweeperPollInterval,
          ec = memorySweeperEC
        )
    ) map {
      db =>
        implicit val optionValueSerializer: Serializer[Option[V]] =
          new Serializer[Option[V]] {
            override def write(data: Option[V]): Slice[Byte] =
              data.map(valueSerializer.write).getOrElse(Slice.emptyBytes)

            override def read(data: Slice[Byte]): Option[V] =
              if (data.isEmpty)
                None
              else
                Some(valueSerializer.read(data))
          }

        val map = swaydb.Map[Key[K], Option[V], Nothing, IO.ApiIO](db)
        Extend(map = map)(
          keySerializer = keySerializer,
          optionValueSerializer = optionValueSerializer,
          keyOrder = Key.ordering(bytesKeyOrder)
        )
    }
  }
}
