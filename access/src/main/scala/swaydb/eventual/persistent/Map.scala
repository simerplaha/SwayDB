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

package swaydb.eventual.persistent

import com.typesafe.scalalogging.LazyLogging
import java.nio.file.Path
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import swaydb.SwayDB
import swaydb.configs.level.{DefaultGroupingStrategy, DefaultMemoryPersistentConfig}
import swaydb.core.BlockingCoreAPI
import swaydb.core.function.FunctionStore
import swaydb.data.IO
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer

object Map extends LazyLogging {

  implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long
  implicit val functionStore: FunctionStore = FunctionStore.memory()

  /**
    * A 3 Leveled in-memory database where the 3rd is persistent.
    *
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    *
    * @param dir                        Root directory for all Level where appendix folder & files are created
    * @param otherDirs                  Secondary directories for all Levels where Segments get distributed.
    * @param maxOpenSegments            Number of concurrent Segments opened
    * @param mapSize                    Size of LevelZero's maps (WAL)
    * @param maxMemoryLevelSize         Total size of in-memory Level (Level1) before Segments gets pushed to persistent Level (Level2)
    * @param maxSegmentsToPush          Numbers of Segments to push from in-memory Level (Level1) to persistent Level (Level2)
    * @param memoryLevelSegmentSize     Size of Level1's Segments
    * @param persistentLevelSegmentSize Size of Level2's Segments
    * @param mmapPersistentSegments     Memory-maps Level2 Segments
    * @param mmapPersistentAppendix     Memory-maps Level2's appendix file
    * @param cacheSize                  Size of
    * @param cacheCheckDelay            Sets the max interval at which key-values get dropped from the cache. The delays
    *                                   are dynamically adjusted based on the current size of the cache to stay close the set
    *                                   cacheSize.
    * @param segmentsOpenCheckDelay     Sets the max interval at which Segments get closed. The delays
    *                                   are dynamically adjusted based on the current number of open Segments.
    * @param acceleration               Controls the write speed.
    * @param keySerializer              Converts keys to Bytes
    * @param valueSerializer            Converts values to Bytes
    * @param ordering                   Sort order for keys
    * @param ec                         ExecutionContext
    * @tparam K Type of key
    * @tparam V Type of value
    * @return Database instance
    */
  def apply[K, V](dir: Path,
                  maxOpenSegments: Int = 1000,
                  mapSize: Int = 4.mb,
                  maxMemoryLevelSize: Int = 100.mb,
                  maxSegmentsToPush: Int = 5,
                  memoryLevelSegmentSize: Int = 2.mb,
                  persistentLevelSegmentSize: Int = 4.mb,
                  persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                  mmapPersistentSegments: MMAP = MMAP.WriteAndRead,
                  mmapPersistentAppendix: Boolean = true,
                  cacheSize: Int = 100.mb, //cacheSize for memory database is used for evicting decompressed key-values & persistent key-values in-memory
                  otherDirs: Seq[Dir] = Seq.empty,
                  cacheCheckDelay: FiniteDuration = 7.seconds,
                  segmentsOpenCheckDelay: FiniteDuration = 5.seconds,
                  bloomFilterFalsePositiveRate: Double = 0.01,
                  compressDuplicateValues: Boolean = true,
                  deleteSegmentsEventually: Boolean = true,
                  groupingStrategy: Option[KeyValueGroupingStrategy] = Some(DefaultGroupingStrategy()),
                  acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                     valueSerializer: Serializer[V],
                                                                                     keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                     ec: ExecutionContext = SwayDB.defaultExecutionContext): IO[swaydb.Map[K, V]] =
    BlockingCoreAPI(
      config =
        DefaultMemoryPersistentConfig(
          dir = dir,
          otherDirs = otherDirs,
          mapSize = mapSize,
          maxMemoryLevelSize = maxMemoryLevelSize,
          maxSegmentsToPush = maxSegmentsToPush,
          memoryLevelSegmentSize = memoryLevelSegmentSize,
          persistentLevelSegmentSize = persistentLevelSegmentSize,
          persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
          mmapPersistentSegments = mmapPersistentSegments,
          mmapPersistentAppendix = mmapPersistentAppendix,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          compressDuplicateValues = compressDuplicateValues,
          deleteSegmentsEventually = deleteSegmentsEventually,
          groupingStrategy = groupingStrategy,
          acceleration = acceleration
        ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        swaydb.Map[K, V](new SwayDB(core))
    }
}
