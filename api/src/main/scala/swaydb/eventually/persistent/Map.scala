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

package swaydb.eventually.persistent

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.{DefaultEventuallyPersistentConfig, DefaultGroupBy}
import swaydb.core.BlockingCore
import swaydb.core.function.FunctionStore
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.api.grouping.GroupBy
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{IO, SwayDB, Tag}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}

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
   * @param keyValueCacheSize                  Size of
   * @param keyValueCacheCheckDelay            Sets the max interval at which key-values get dropped from the cache. The delays
   *                                   are dynamically adjusted based on the current size of the cache to stay close the set
   *                                   cacheSize.
   * @param segmentsOpenCheckDelay     Sets the max interval at which Segments get closed. The delays
   *                                   are dynamically adjusted based on the current number of open Segments.
   * @param acceleration               Controls the write speed.
   * @param keySerializer              Converts keys to Bytes
   * @param valueSerializer            Converts values to Bytes
   * @param keyOrder                   Sort order for keys
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
                  keyValueCacheSize: Int = 100.mb, //cacheSize for memory database is used for evicting decompressed key-values & persistent key-values in-memory
                  otherDirs: Seq[Dir] = Seq.empty,
                  keyValueCacheCheckDelay: FiniteDuration = 5.seconds,
                  segmentsOpenCheckDelay: FiniteDuration = 5.seconds,
                  blockCacheSize: Option[Int] = Some(4098),
                  mightContainFalsePositiveRate: Double = 0.01,
                  compressDuplicateValues: Boolean = true,
                  deleteSegmentsEventually: Boolean = false,
                  groupBy: Option[GroupBy.KeyValues] = Some(DefaultGroupBy()),
                  acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                        valueSerializer: Serializer[V],
                                                                                        keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                                        fileSweeperEC: ExecutionContext = SwayDB.defaultExecutionContext,
                                                                                        memorySweeperEC: ExecutionContext = SwayDB.defaultExecutionContext): IO[swaydb.Error.Boot, swaydb.Map[K, V, IO.ApiIO]] =
    BlockingCore(
      config =
        DefaultEventuallyPersistentConfig(
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
          mightContainFalsePositiveRate = mightContainFalsePositiveRate,
          compressDuplicateValues = compressDuplicateValues,
          deleteSegmentsEventually = deleteSegmentsEventually,
          groupBy = groupBy,
          acceleration = acceleration
        ),
      maxOpenSegments = maxOpenSegments,
      keyValueCacheSize = Some(keyValueCacheSize),
      keyValueCacheCheckDelay = keyValueCacheCheckDelay,
      blockCacheSize = blockCacheSize,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay,
      fileSweeperEC = fileSweeperEC,
      memorySweeperEC = memorySweeperEC
    ) map {
      db =>
        swaydb.Map[K, V, IO.ApiIO](db)
    }
}
