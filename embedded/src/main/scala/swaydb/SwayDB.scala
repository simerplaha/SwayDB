/*
 * Copyright (C) 2018 Simer Plaha (@simerplaha)
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

package swaydb

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.{DefaultGroupingStrategy, DefaultMemoryConfig, DefaultMemoryPersistentConfig, DefaultPersistentConfig}
import swaydb.core.data.{Memory, Value}
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.LevelZeroMapEntryWriter
import swaydb.core.tool.AppendixRepairer
import swaydb.core.CoreAPI
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.api.grouping.KeyValueGroupingStrategy
import swaydb.data.compaction.LevelMeter
import swaydb.data.config._
import swaydb.data.repairAppendix.RepairResult.OverlappingSegments
import swaydb.data.repairAppendix._
import swaydb.data.request
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Serializer
import swaydb.data.{SwayMap, SwaySet}
import swaydb.table.Table

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try}

/**
  * Instance used for creating/initialising databases.
  */
object SwayDB extends LazyLogging {

  val rootTable =
    Table.buildTable(Slice.writeIntUnsigned(0))

  /**
    * Default execution context for all databases.
    *
    * This can be overridden by provided an implicit parameter in the scope of where the database is initialized.
    */
  def defaultExecutionContext = new ExecutionContext {
    val threadPool = new ForkJoinPool(100)

    def execute(runnable: Runnable) =
      threadPool execute runnable

    def reportFailure(exception: Throwable): Unit =
      logger.error("Execution context failure", exception)
  }

  /**
    * A pre-configured, 8 Leveled, persistent database where Level1 accumulates a minimum of 10 Segments before
    * pushing Segments to lower Level.
    *
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    *
    * @param dir                         Root directory for all Level where appendix folder & files are created
    * @param otherDirs                   Secondary directories for all Levels where Segments get distributed.
    * @param maxOpenSegments             Number of concurrent Segments opened
    * @param cacheSize                   Size of in-memory key-values
    * @param mapSize                     Size of LevelZero's maps (WAL)
    * @param mmapMaps                    Memory-maps LevelZero maps files if set to true else reverts java.nio.FileChannel
    * @param mmapAppendix                Memory-maps Levels appendix files if set to true else reverts java.nio.FileChannel
    * @param mmapSegments                Memory-maps Levels Segment files if set to true else reverts java.nio.FileChannel
    * @param segmentSize                 Minimum size of Segment files in each Level
    * @param appendixFlushCheckpointSize Size of the appendix file before it's flushed. Appendix files are append only log files.
    *                                    Flushing removes deleted entries in the file hence reducing the size of the file.
    * @param cacheCheckDelay             Sets the max interval at which key-values get dropped from the cache. The delays
    *                                    are dynamically adjusted based on the current size of the cache to stay close the set
    *                                    cacheSize.
    * @param segmentsOpenCheckDelay      Sets the max interval at which Segments get closed. The delays
    *                                    are dynamically adjusted based on the current number of open Segments.
    * @param acceleration                Controls the write speed.
    * @param keySerializer               Converts keys to Bytes
    * @param valueSerializer             Converts values to Bytes
    * @param ordering                    Sort order for keys
    * @param ec                          ExecutionContext
    * @tparam K Type of key
    * @tparam V Type of value
    * @return Database instance
    */

  def persistent[K, V](dir: Path,
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
                       minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds,
                       compressDuplicateValues: Boolean = true,
                       lastLevelGroupingStrategy: Option[KeyValueGroupingStrategy] = Some(DefaultGroupingStrategy()),
                       acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                          valueSerializer: Serializer[V],
                                                                                          ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                          ec: ExecutionContext = defaultExecutionContext): Try[SwayMap[K, V]] =
    CoreAPI(
      config = DefaultPersistentConfig(
        dir = dir,
        otherDirs = otherDirs,
        mapSize = mapSize, mmapMaps = mmapMaps,
        mmapSegments = mmapSegments,
        recoveryMode = recoveryMode,
        mmapAppendix = mmapAppendix,
        segmentSize = segmentSize,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = lastLevelGroupingStrategy,
        acceleration = acceleration
      ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        SwayMap[K, V](new SwayDB(core), rootTable)
    }

  /**
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    */
  def persistentSet[T](dir: Path,
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
                       minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds,
                       compressDuplicateValues: Boolean = true,
                       lastLevelGroupingStrategy: Option[KeyValueGroupingStrategy] = Some(DefaultGroupingStrategy()),
                       acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                          ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                          ec: ExecutionContext = defaultExecutionContext): Try[SwaySet[T]] = {
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
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        groupingStrategy = lastLevelGroupingStrategy,
        acceleration = acceleration
      ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        SwaySet[T](new SwayDB(core), rootTable)
    }
  }

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

  def memory[K, V](mapSize: Int = 4.mb,
                   segmentSize: Int = 2.mb,
                   cacheSize: Int = 500.mb,
                   cacheCheckDelay: FiniteDuration = 7.seconds,
                   bloomFilterFalsePositiveRate: Double = 0.01,
                   minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds,
                   compressDuplicateValues: Boolean = false,
                   groupingStrategy: Option[KeyValueGroupingStrategy] = None,
                   acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                      valueSerializer: Serializer[V],
                                                                                      ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                      ec: ExecutionContext = defaultExecutionContext): Try[SwayMap[K, V]] = {
    val order = Table.ordering(customOrder = ordering)
    CoreAPI(
      config = DefaultMemoryConfig(
        mapSize = mapSize,
        segmentSize = segmentSize,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        compressDuplicateValues = compressDuplicateValues,
        groupingStrategy = groupingStrategy,
        acceleration = acceleration
      ),
      maxOpenSegments = 0,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      //memory Segments are never closed.
      segmentsOpenCheckDelay = Duration.Zero
    )(ec, order) map {
      core =>
        SwayMap[K, V](new SwayDB(core), rootTable)
    }
  }

  /**
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    */
  def memorySet[T](mapSize: Int = 4.mb,
                   segmentSize: Int = 2.mb,
                   cacheSize: Int = 500.mb, //cacheSize for memory database is used for evicting decompressed key-values
                   cacheCheckDelay: FiniteDuration = 7.seconds,
                   bloomFilterFalsePositiveRate: Double = 0.01,
                   minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds,
                   compressDuplicateValues: Boolean = false,
                   groupingStrategy: Option[KeyValueGroupingStrategy] = None,
                   acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                      ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                      ec: ExecutionContext = defaultExecutionContext): Try[SwaySet[T]] =
    CoreAPI(
      config = DefaultMemoryConfig(
        mapSize = mapSize,
        segmentSize = segmentSize,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
        compressDuplicateValues = compressDuplicateValues,
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
        SwaySet[T](new SwayDB(core), rootTable)
    }

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
  def memoryPersistent[K, V](dir: Path,
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
                             minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds,
                             compressDuplicateValues: Boolean = true,
                             groupingStrategy: Option[KeyValueGroupingStrategy] = Some(DefaultGroupingStrategy()),
                             acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                                valueSerializer: Serializer[V],
                                                                                                ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                                ec: ExecutionContext = defaultExecutionContext): Try[SwayMap[K, V]] =
    CoreAPI(
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
          minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
          compressDuplicateValues = compressDuplicateValues,
          groupingStrategy = groupingStrategy,
          acceleration = acceleration
        ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        SwayMap[K, V](new SwayDB(core), rootTable)
    }

  /**
    * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
    */
  def memoryPersistentSet[T](dir: Path,
                             maxOpenSegments: Int = 1000,
                             mapSize: Int = 4.mb,
                             maxMemoryLevelSize: Int = 100.mb,
                             maxSegmentsToPush: Int = 5,
                             memoryLevelSegmentSize: Int = 2.mb,
                             persistentLevelSegmentSize: Int = 4.mb,
                             persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                             mmapPersistentSegments: MMAP = MMAP.WriteAndRead,
                             mmapPersistentAppendix: Boolean = true,
                             cacheSize: Int = 100.mb,
                             otherDirs: Seq[Dir] = Seq.empty,
                             cacheCheckDelay: FiniteDuration = 7.seconds,
                             segmentsOpenCheckDelay: FiniteDuration = 5.seconds,
                             bloomFilterFalsePositiveRate: Double = 0.01,
                             minTimeLeftToUpdateExpiration: FiniteDuration = 10.seconds,
                             compressDuplicateValues: Boolean = true,
                             groupingStrategy: Option[KeyValueGroupingStrategy] = Some(DefaultGroupingStrategy()),
                             acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                                ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                                ec: ExecutionContext = defaultExecutionContext): Try[SwaySet[T]] =
    CoreAPI(
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
          minTimeLeftToUpdateExpiration = minTimeLeftToUpdateExpiration,
          compressDuplicateValues = compressDuplicateValues,
          groupingStrategy = groupingStrategy,
          acceleration = acceleration
        ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        SwaySet[T](new SwayDB(core), rootTable)
    }

  /**
    * Creates a database based on the input config.
    *
    * @param config                 Configuration to use to create the database
    * @param maxSegmentsOpen        Number of concurrent opened Segments
    * @param cacheSize              Size of in-memory key-values. For Memory database this set the size of uncompressed key-values.
    *                               If compression is used for memory database the this field can be ignored.
    * @param cacheCheckDelay        Sets the max interval at which key-values get dropped from the cache. The delays
    *                               are dynamically adjusted based on the current size of the cache to stay close the set
    *                               cacheSize.
    *                               If compression is not used for memory database the this field can be ignored.
    * @param segmentsOpenCheckDelay For persistent Levels only. This can property is not used for databases.
    *                               Sets the max interval at which Segments get closed. The delays
    *                               are dynamically adjusted based on the current number of open Segments.
    * @param keySerializer          Converts keys to Bytes
    * @param valueSerializer        Converts values to Bytes
    * @param ordering               Sort order for keys
    * @param ec                     ExecutionContext
    * @tparam K Type of key
    * @tparam V Type of value
    * @return Database instance
    */
  def apply[K, V](config: SwayDBPersistentConfig,
                  maxSegmentsOpen: Int,
                  cacheSize: Int,
                  cacheCheckDelay: FiniteDuration,
                  segmentsOpenCheckDelay: FiniteDuration)(implicit keySerializer: Serializer[K],
                                                          valueSerializer: Serializer[V],
                                                          ordering: Ordering[Slice[Byte]],
                                                          ec: ExecutionContext): Try[SwayMap[K, V]] =
    CoreAPI(
      config = config,
      maxOpenSegments = maxSegmentsOpen,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        SwayMap[K, V](new SwayDB(core), rootTable)
    }

  def apply[T](config: SwayDBPersistentConfig,
               maxSegmentsOpen: Int,
               cacheSize: Int,
               cacheCheckDelay: FiniteDuration,
               segmentsOpenCheckDelay: FiniteDuration)(implicit serializer: Serializer[T],
                                                       ordering: Ordering[Slice[Byte]],
                                                       ec: ExecutionContext): Try[SwaySet[T]] =
    CoreAPI(
      config = config,
      maxOpenSegments = maxSegmentsOpen,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        SwaySet[T](new SwayDB(core), rootTable)
    }

  def apply[K, V](config: SwayDBMemoryConfig,
                  cacheSize: Int,
                  cacheCheckDelay: FiniteDuration)(implicit keySerializer: Serializer[K],
                                                   valueSerializer: Serializer[V],
                                                   ordering: Ordering[Slice[Byte]],
                                                   ec: ExecutionContext): Try[SwayMap[K, V]] =
    CoreAPI(
      config = config,
      maxOpenSegments = 0,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = Duration.Zero
    ) map {
      core =>
        SwayMap[K, V](new SwayDB(core), rootTable)
    }

  def apply[T](config: SwayDBMemoryConfig,
               cacheSize: Int,
               cacheCheckDelay: FiniteDuration)(implicit serializer: Serializer[T],
                                                ordering: Ordering[Slice[Byte]],
                                                ec: ExecutionContext): Try[SwaySet[T]] =
    CoreAPI(
      config = config,
      maxOpenSegments = 0,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = Duration.Zero
    ) map {
      core =>
        SwaySet[T](new SwayDB(core), rootTable)
    }

  /**
    * Documentation: http://www.swaydb.io/api/repairAppendix
    */
  def repairAppendix[K](levelPath: Path,
                        repairStrategy: AppendixRepairStrategy)(implicit serializer: Serializer[K],
                                                                ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                ec: ExecutionContext = defaultExecutionContext): Try[RepairResult[K]] =
  //convert to typed result.
    AppendixRepairer(levelPath, repairStrategy) match {
      case Failure(OverlappingSegmentsException(segmentInfo, overlappingSegmentInfo)) =>
        Success(
          OverlappingSegments[K](
            segmentInfo =
              SegmentInfo(
                path = segmentInfo.path,
                minKey = serializer.read(segmentInfo.minKey),
                maxKey =
                  segmentInfo.maxKey match {
                    case swaydb.data.segment.MaxKey.Fixed(maxKey) =>
                      swaydb.data.repairAppendix.MaxKey.Fixed(serializer.read(maxKey))

                    case swaydb.data.segment.MaxKey.Range(fromKey, maxKey) =>
                      swaydb.data.repairAppendix.MaxKey.Range(fromKey = serializer.read(fromKey), maxKey = serializer.read(maxKey))
                  },
                segmentSize = segmentInfo.segmentSize,
                keyValueCount = segmentInfo.keyValueCount
              ),
            overlappingSegmentInfo =
              SegmentInfo(
                path = overlappingSegmentInfo.path,
                minKey = serializer.read(overlappingSegmentInfo.minKey),
                maxKey =
                  overlappingSegmentInfo.maxKey match {
                    case swaydb.data.segment.MaxKey.Fixed(maxKey) =>
                      swaydb.data.repairAppendix.MaxKey.Fixed(serializer.read(maxKey))

                    case swaydb.data.segment.MaxKey.Range(fromKey, maxKey) =>
                      swaydb.data.repairAppendix.MaxKey.Range(fromKey = serializer.read(fromKey), maxKey = serializer.read(maxKey))
                  },
                segmentSize = overlappingSegmentInfo.segmentSize,
                keyValueCount = overlappingSegmentInfo.keyValueCount
              )
          )
        )
      case Failure(exception) =>
        Failure(exception)

      case Success(_) =>
        Success(RepairResult.Repaired)
    }
}

private[swaydb] class SwayDB(api: CoreAPI) {

  def subTable(key: Slice[Byte],
               value: Option[Slice[Byte]])(implicit table: Table): Try[(Table, Level0Meter)] = {
    val subTableEntry = Table.buildSubTableRow(key, table)
    contains(subTableEntry) flatMap {
      exists =>
        if (exists) {
          Success(Table.buildTable(key), api.level0Meter)
        } else {
          val subTableBatch = request.Batch.Put(subTableEntry, value, None)

          val subTable = Table.buildTable(key)

          val tableStart = request.Batch.Put(subTable.start, value, None)
          val tableEnd = request.Batch.Put(subTable.end, value, None)
          batch(Seq(subTableBatch, tableStart, tableEnd)) map {
            meter =>
              (subTable, meter)
          }
        }
    }
  }

  def put(key: Slice[Byte])(implicit table: Table) =
    api.put(Table.buildRowKey(key, table))

  def put(key: Slice[Byte], value: Option[Slice[Byte]])(implicit table: Table) =
    api.put(Table.buildRowKey(key, table), value)

  def put(key: Slice[Byte], value: Option[Slice[Byte]], expireAt: Deadline)(implicit table: Table): Try[Level0Meter] =
    api.put(Table.buildRowKey(key, table), value, expireAt)

  def update(key: Slice[Byte], value: Option[Slice[Byte]])(implicit table: Table): Try[Level0Meter] =
    api.update(Table.buildRowKey(key, table), value)

  def update(from: Slice[Byte], to: Slice[Byte], value: Option[Slice[Byte]])(implicit table: Table): Try[Level0Meter] =
    api.update(Table.buildRowKey(from, table), Table.buildRowKey(to, table), value)

  def expire(key: Slice[Byte], at: Deadline)(implicit table: Table): Try[Level0Meter] =
    api.remove(Table.buildRowKey(key, table), at)

  def expire(from: Slice[Byte], to: Slice[Byte], at: Deadline)(implicit table: Table): Try[Level0Meter] =
    api.remove(Table.buildRowKey(from, table), Table.buildRowKey(to, table), at)

  def remove(key: Slice[Byte])(implicit table: Table) =
    api.remove(Table.buildRowKey(key, table))

  def remove(from: Slice[Byte], to: Slice[Byte])(implicit table: Table): Try[Level0Meter] =
    api.remove(Table.buildRowKey(from, table), Table.buildRowKey(to, table))

  def batch(entries: Iterable[request.Batch])(implicit table: Table) =
    entries.foldLeft(Option.empty[MapEntry[Slice[Byte], Memory.SegmentResponse]]) {
      case (mapEntry, batchEntry) =>
        val nextEntry =
          batchEntry match {
            case request.Batch.Put(key, value, expire) =>
              val rowKey = Table.buildRowKey(key, table)
              MapEntry.Put[Slice[Byte], Memory.Put](rowKey, Memory.Put(rowKey, value, expire))(LevelZeroMapEntryWriter.Level0PutWriter)

            case request.Batch.Remove(key, expire) =>
              val rowKey = Table.buildRowKey(key, table)
              MapEntry.Put[Slice[Byte], Memory.Remove](rowKey, Memory.Remove(rowKey, expire))(LevelZeroMapEntryWriter.Level0RemoveWriter)

            case request.Batch.Update(key, value) =>
              val rowKey = Table.buildRowKey(key, table)
              MapEntry.Put[Slice[Byte], Memory.Update](rowKey, Memory.Update(rowKey, value))(LevelZeroMapEntryWriter.Level0UpdateWriter)

            case request.Batch.RemoveRange(fromKey, toKey, expire) =>
              val fromRowKey = Table.buildRowKey(fromKey, table)
              val toRowKey = Table.buildRowKey(toKey, table)
              (MapEntry.Put[Slice[Byte], Memory.Range](fromRowKey, Memory.Range(fromRowKey, toRowKey, None, Value.Remove(expire)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
                MapEntry.Put[Slice[Byte], Memory.Remove](toRowKey, Memory.Remove(toRowKey, expire))(LevelZeroMapEntryWriter.Level0RemoveWriter)

            case request.Batch.UpdateRange(fromKey, toKey, value) =>
              val fromRowKey = Table.buildRowKey(fromKey, table)
              val toRowKey = Table.buildRowKey(toKey, table)
              (MapEntry.Put[Slice[Byte], Memory.Range](fromRowKey, Memory.Range(fromRowKey, toRowKey, None, Value.Update(value, None)))(LevelZeroMapEntryWriter.Level0RangeWriter): MapEntry[Slice[Byte], Memory.SegmentResponse]) ++
                MapEntry.Put[Slice[Byte], Memory.Update](toRowKey, Memory.Update(toRowKey))(LevelZeroMapEntryWriter.Level0UpdateWriter)
          }
        Some(mapEntry.map(_ ++ nextEntry) getOrElse nextEntry)
    } map {
      entry =>
        api.put(entry)
    } getOrElse Failure(new Exception("Cannot write empty batch"))

  def head(implicit table: Table): Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    api.after(table.start).map(_.map {
      case (key, value) =>
        (Table.dropTableBytes(key), value)
    })

  def last(implicit table: Table): Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    api.before(table.end).map(_.map {
      case (key, value) =>
        (Table.dropTableBytes(key), value)
    })

  def keyValueCount: Try[Int] =
    api.bloomFilterKeyValueCount

  def contains(key: Slice[Byte])(implicit table: Table): Try[Boolean] =
    api contains Table.buildRowKey(key, table)

  def mightContain(key: Slice[Byte])(implicit table: Table): Try[Boolean] =
    api mightContain Table.buildRowKey(key, table)

  def get(key: Slice[Byte])(implicit table: Table): Try[Option[Option[Slice[Byte]]]] =
    api get Table.buildRowKey(key, table)

  def getKey(key: Slice[Byte])(implicit table: Table): Try[Option[Slice[Byte]]] =
    api.getKey(Table.buildRowKey(key, table)).map(_.map(Table.dropTableBytes))

  def getKeyValue(key: Slice[Byte])(implicit table: Table): Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    api.getKeyValue(Table.buildRowKey(key, table)).map(_.map {
      case (key, value) =>
        (Table.dropTableBytes(key), value)
    })

  def beforeKey(key: Slice[Byte])(implicit table: Table): Try[Option[Slice[Byte]]] =
    api.beforeKey(Table.buildRowKey(key, table)).map(_.map(Table.dropTableBytes))

  def before(key: Slice[Byte])(implicit table: Table): Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    api.before(Table.buildRowKey(key, table)).map(_.map {
      case (key, value) =>
        (Table.dropTableBytes(key), value)
    })

  def afterKey(key: Slice[Byte])(implicit table: Table): Try[Option[Slice[Byte]]] =
    api.afterKey(Table.buildRowKey(key, table)).map(_.map(Table.dropTableBytes))

  def after(key: Slice[Byte])(implicit table: Table): Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    api.after(Table.buildRowKey(key, table)).map(_.map {
      case (key, value) =>
        (Table.dropTableBytes(key), value)
    })

  def headKey: Try[Option[Slice[Byte]]] =
    api.headKey.map(_.map(Table.dropTableBytes))

  def lastKey: Try[Option[Slice[Byte]]] =
    api.lastKey.map(_.map(Table.dropTableBytes))

  def sizeOfSegments: Long =
    api.sizeOfSegments

  def level0Meter: Level0Meter =
    api.level0Meter

  def level1Meter: LevelMeter =
    api.level1Meter

  def levelMeter(levelNumber: Int): Option[LevelMeter] =
    api.levelMeter(levelNumber)

  def valueSize(key: Slice[Byte]): Try[Option[Int]] =
    api.valueSize(key)

  def deadline(key: Slice[Byte]): Try[Option[Deadline]] =
    api.deadline(key)
}