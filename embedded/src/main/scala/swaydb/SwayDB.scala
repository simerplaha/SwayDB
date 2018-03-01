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
import swaydb.api.SwayDBAPI
import swaydb.configs.level.{MemoryConfig, PersistentConfig}
import swaydb.core.CoreAPI
import swaydb.core.data.Value
import swaydb.core.map.MapEntry
import swaydb.core.map.serializer.LevelZeroMapEntryWriter
import swaydb.core.tool.AppendixRepairer
import swaydb.data.accelerate.{Accelerator, Level0Meter}
import swaydb.data.compaction.LevelMeter
import swaydb.data.config._
import swaydb.data.repairAppendix.RepairResult.OverlappingSegments
import swaydb.data.repairAppendix._
import swaydb.data.request
import swaydb.data.slice.Slice
import swaydb.order.KeyOrder
import swaydb.serializers.Serializer
import swaydb.types.{SwayDBMap, SwayDBSet}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.forkjoin.ForkJoinPool
import scala.util.{Failure, Success, Try}

/**
  * Instance used for creating/initialising databases.
  */
object SwayDB extends LazyLogging {

  /**
    * Default execution context for all databases.
    *
    * This can be overridden by provided an implicit parameter in the scope of where the database is initialized.
    */
  def defaultExecutionContext = new ExecutionContext {
    lazy val threadPool = new ForkJoinPool(100)

    def execute(runnable: Runnable) =
      threadPool execute runnable

    def reportFailure(exception: Throwable): Unit =
      logger.error("Execution context failure", exception)
  }

  /**
    * A pre-configured, 8 Leveled, persistent database where Level1 accumulates a minimum of 10 Segments before
    * pushing Segments to lower Level.
    *
    * For custom configurations read documentation on website: http://www.swaydb.io/#configuring-levels
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
                       cacheSize: Long = 100.mb,
                       mapSize: Int = 4.mb,
                       mmapMaps: Boolean = true,
                       recoveryMode: RecoveryMode = RecoveryMode.ReportCorruption,
                       mmapAppendix: Boolean = true,
                       mmapSegments: MMAP = MMAP.WriteAndRead,
                       segmentSize: Int = 2.mb,
                       appendixFlushCheckpointSize: Int = 2.mb,
                       otherDirs: Seq[Dir] = Seq.empty,
                       cacheCheckDelay: FiniteDuration = 5.seconds,
                       segmentsOpenCheckDelay: FiniteDuration = 5.seconds,
                       bloomFilterFalsePositiveRate: Double = 0.1,
                       acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                          valueSerializer: Serializer[V],
                                                                                          ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                          ec: ExecutionContext = defaultExecutionContext): Try[SwayDBMap[K, V]] =
    CoreAPI(
      config = PersistentConfig(
        dir = dir,
        otherDirs = otherDirs,
        mapSize = mapSize, mmapMaps = mmapMaps,
        mmapSegments = mmapSegments,
        recoveryMode = recoveryMode,
        mmapAppendix = mmapAppendix,
        segmentSize = segmentSize,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        acceleration = acceleration
      ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        SwayDBMap[K, V](new SwayDB(core))
    }

  /**
    * For custom configurations read documentation on website: http://www.swaydb.io/#configuring-levels
    */
  def persistentSet[T](dir: Path,
                       maxOpenSegments: Int = 1000,
                       cacheSize: Long = 100.mb,
                       mapSize: Int = 4.mb,
                       mmapMaps: Boolean = true,
                       recoveryMode: RecoveryMode = RecoveryMode.ReportCorruption,
                       mmapAppendix: Boolean = true,
                       mmapSegments: MMAP = MMAP.WriteAndRead,
                       segmentSize: Int = 2.mb,
                       appendixFlushCheckpointSize: Int = 2.mb,
                       otherDirs: Seq[Dir] = Seq.empty,
                       cacheCheckDelay: FiniteDuration = 5.seconds,
                       segmentsOpenCheckDelay: FiniteDuration = 5.seconds,
                       bloomFilterFalsePositiveRate: Double = 0.01,
                       acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                          ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                          ec: ExecutionContext = defaultExecutionContext): Try[SwayDBSet[T]] =
    CoreAPI(
      config = PersistentConfig(
        dir = dir,
        otherDirs = otherDirs,
        recoveryMode = recoveryMode,
        mapSize = mapSize,
        mmapMaps = mmapMaps,
        mmapSegments = mmapSegments,
        mmapAppendix = mmapAppendix,
        segmentSize = segmentSize,
        appendixFlushCheckpointSize = appendixFlushCheckpointSize,
        bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
        acceleration = acceleration
      ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        SwayDBSet[T](new SwayDB(core))
    }

  /**
    * A 2 Leveled (Level0 & Level1), in-memory database.
    *
    * For custom configurations read documentation on website: http://www.swaydb.io/#configuring-levels
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
                   bloomFilterFalsePositiveRate: Double = 0.1,
                   acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                      valueSerializer: Serializer[V],
                                                                                      ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                      ec: ExecutionContext = defaultExecutionContext): Try[SwayDBMap[K, V]] =
    CoreAPI(
      config = MemoryConfig(
        mapSize,
        segmentSize,
        bloomFilterFalsePositiveRate,
        acceleration
      ),
      maxOpenSegments = 0,
      cacheSize = 0,
      cacheCheckDelay = Duration.Zero,
      segmentsOpenCheckDelay = Duration.Zero
    ) map {
      core =>
        SwayDBMap[K, V](new SwayDB(core))
    }

  /**
    * For custom configurations read documentation on website: http://www.swaydb.io/#configuring-levels
    */
  def memorySet[T](mapSize: Int = 4.mb,
                   segmentSize: Int = 2.mb,
                   bloomFilterFalsePositiveRate: Double = 0.01,
                   acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                      ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                      ec: ExecutionContext = defaultExecutionContext): Try[SwayDBSet[T]] =
    CoreAPI(
      config = MemoryConfig(
        mapSize,
        segmentSize,
        bloomFilterFalsePositiveRate,
        acceleration
      ),
      maxOpenSegments = 0,
      cacheSize = 0,
      cacheCheckDelay = Duration.Zero,
      segmentsOpenCheckDelay = Duration.Zero
    ) map {
      core =>
        SwayDBSet[T](new SwayDB(core))
    }

  /**
    * A 3 Leveled in-memory database where the 3rd is persistent.
    *
    * For custom configurations read documentation on website: http://www.swaydb.io/#configuring-levels
    *
    * @param dir                           Root directory for all Level where appendix folder & files are created
    * @param otherDirs                     Secondary directories for all Levels where Segments get distributed.
    * @param maxOpenSegments               Number of concurrent Segments opened
    * @param mapSize                       Size of LevelZero's maps (WAL)
    * @param maxMemoryLevelSize            Total size of in-memory Level (Level1) before Segments gets pushed to persistent Level (Level2)
    * @param maxSegmentsToPush             Numbers of Segments to push from in-memory Level (Level1) to persistent Level (Level2)
    * @param memoryLevelSegmentSize        Size of Level1's Segments
    * @param persistentLevelSegmentSize    Size of Level2's Segments
    * @param cachePersistentLevelKeyValues If true adds keys to cache on create for Level2
    * @param mmapPersistentSegments        Memory-maps Level2 Segments
    * @param mmapPersistentAppendix        Memory-maps Level2's appendix file
    * @param cacheSize                     Size of
    * @param cacheCheckDelay               Sets the max interval at which key-values get dropped from the cache. The delays
    *                                      are dynamically adjusted based on the current size of the cache to stay close the set
    *                                      cacheSize.
    * @param segmentsOpenCheckDelay        Sets the max interval at which Segments get closed. The delays
    *                                      are dynamically adjusted based on the current number of open Segments.
    * @param acceleration                  Controls the write speed.
    * @param keySerializer                 Converts keys to Bytes
    * @param valueSerializer               Converts values to Bytes
    * @param ordering                      Sort order for keys
    * @param ec                            ExecutionContext
    * @tparam K Type of key
    * @tparam V Type of value
    * @return Database instance
    */
  def memoryPersistent[K, V](dir: Path,
                             maxOpenSegments: Int = 1000,
                             mapSize: Int = 4.mb,
                             maxMemoryLevelSize: Int = 100.mb,
                             maxSegmentsToPush: Int = 10,
                             memoryLevelSegmentSize: Int = 2.mb,
                             persistentLevelSegmentSize: Int = 2.mb,
                             persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                             cachePersistentLevelKeyValues: Boolean = false,
                             mmapPersistentSegments: MMAP = MMAP.WriteAndRead,
                             mmapPersistentAppendix: Boolean = true,
                             cacheSize: Long = 100.mb,
                             otherDirs: Seq[Dir] = Seq.empty,
                             cacheCheckDelay: FiniteDuration = 5.seconds,
                             segmentsOpenCheckDelay: FiniteDuration = 5.seconds,
                             bloomFilterFalsePositiveRate: Double = 0.1,
                             acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit keySerializer: Serializer[K],
                                                                                                valueSerializer: Serializer[V],
                                                                                                ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                                ec: ExecutionContext = defaultExecutionContext): Try[SwayDBMap[K, V]] =
    CoreAPI(
      config =
        MemoryConfig(
          dir = dir,
          otherDirs = otherDirs,
          mapSize = mapSize,
          maxMemoryLevelSize = maxMemoryLevelSize,
          maxSegmentsToPush = maxSegmentsToPush,
          memoryLevelSegmentSize = memoryLevelSegmentSize,
          persistentLevelSegmentSize = persistentLevelSegmentSize,
          persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
          cachePersistentLevelKeyValues = cachePersistentLevelKeyValues,
          mmapPersistentSegments = mmapPersistentSegments,
          mmapPersistentAppendix = mmapPersistentAppendix,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          acceleration = acceleration
        ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        SwayDBMap[K, V](new SwayDB(core))
    }

  /**
    * For custom configurations read documentation on website: http://www.swaydb.io/#configuring-levels
    */
  def memoryPersistentSet[T](dir: Path,
                             maxOpenSegments: Int = 1000,
                             mapSize: Int = 4.mb,
                             maxMemoryLevelSize: Int = 100.mb,
                             maxSegmentsToPush: Int = 10,
                             memoryLevelSegmentSize: Int = 2.mb,
                             persistentLevelSegmentSize: Int = 2.mb,
                             persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                             cachePersistentLevelKeyValues: Boolean = false,
                             mmapPersistentSegments: MMAP = MMAP.WriteAndRead,
                             mmapPersistentAppendix: Boolean = true,
                             cacheSize: Long = 100.mb,
                             otherDirs: Seq[Dir] = Seq.empty,
                             cacheCheckDelay: FiniteDuration = 5.seconds,
                             segmentsOpenCheckDelay: FiniteDuration = 5.seconds,
                             bloomFilterFalsePositiveRate: Double = 0.01,
                             acceleration: Level0Meter => Accelerator = Accelerator.noBrakes())(implicit serializer: Serializer[T],
                                                                                                ordering: Ordering[Slice[Byte]] = KeyOrder.default,
                                                                                                ec: ExecutionContext = defaultExecutionContext): Try[SwayDBSet[T]] =
    CoreAPI(
      config =
        MemoryConfig(
          dir = dir,
          otherDirs = otherDirs,
          mapSize = mapSize,
          maxMemoryLevelSize = maxMemoryLevelSize,
          maxSegmentsToPush = maxSegmentsToPush,
          memoryLevelSegmentSize = memoryLevelSegmentSize,
          persistentLevelSegmentSize = persistentLevelSegmentSize,
          persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
          cachePersistentLevelKeyValues = cachePersistentLevelKeyValues,
          mmapPersistentSegments = mmapPersistentSegments,
          mmapPersistentAppendix = mmapPersistentAppendix,
          bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate,
          acceleration = acceleration
        ),
      maxOpenSegments = maxOpenSegments,
      cacheSize = cacheSize,
      cacheCheckDelay = cacheCheckDelay,
      segmentsOpenCheckDelay = segmentsOpenCheckDelay
    ) map {
      core =>
        SwayDBSet[T](new SwayDB(core))
    }

  /**
    * Creates a database based on the input config.
    *
    * @param config                 Configuration to use to create the database
    * @param maxSegmentsOpen        Number of concurrent opened Segments
    * @param cacheSize              For persistent Levels only. This can property will be ignored for MemoryLevels.
    *                               Size of in-memory key-values
    * @param cacheCheckDelay        For persistent Levels only. This can property will be ignored for MemoryLevels.
    *                               Sets the max interval at which key-values get dropped from the cache. The delays
    *                               are dynamically adjusted based on the current size of the cache to stay close the set
    *                               cacheSize.
    * @param segmentsOpenCheckDelay For persistent Levels only. This can property will be ignored for MemoryLevels.
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
                  cacheSize: Long,
                  cacheCheckDelay: FiniteDuration,
                  segmentsOpenCheckDelay: FiniteDuration)(implicit keySerializer: Serializer[K],
                                                          valueSerializer: Serializer[V],
                                                          ordering: Ordering[Slice[Byte]],
                                                          ec: ExecutionContext): Try[SwayDBMap[K, V]] =
    CoreAPI(config, maxSegmentsOpen, cacheSize, cacheCheckDelay, segmentsOpenCheckDelay) map {
      core =>
        SwayDBMap[K, V](new SwayDB(core))
    }

  def apply[T](config: SwayDBPersistentConfig,
               maxSegmentsOpen: Int,
               cacheSize: Long,
               cacheCheckDelay: FiniteDuration,
               segmentsOpenCheckDelay: FiniteDuration)(implicit serializer: Serializer[T],
                                                       ordering: Ordering[Slice[Byte]],
                                                       ec: ExecutionContext): Try[SwayDBSet[T]] =
    CoreAPI(config, maxSegmentsOpen, cacheSize, cacheCheckDelay, segmentsOpenCheckDelay) map {
      core =>
        SwayDBSet[T](new SwayDB(core))
    }

  def apply[K, V](config: SwayDBMemoryConfig)(implicit keySerializer: Serializer[K],
                                              valueSerializer: Serializer[V],
                                              ordering: Ordering[Slice[Byte]],
                                              ec: ExecutionContext): Try[SwayDBMap[K, V]] =
    CoreAPI(config, 0, 0, Duration.Zero, Duration.Zero) map {
      core =>
        SwayDBMap[K, V](new SwayDB(core))
    }

  def apply[T](config: SwayDBMemoryConfig)(implicit serializer: Serializer[T],
                                           ordering: Ordering[Slice[Byte]],
                                           ec: ExecutionContext): Try[SwayDBSet[T]] =
    CoreAPI(config, 0, 0, Duration.Zero, Duration.Zero) map {
      core =>
        SwayDBSet[T](new SwayDB(core))
    }

  /**
    * Documentation: http://www.swaydb.io/#api/repairAppendix
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
                maxKey = serializer.read(segmentInfo.maxKey),
                segmentSize = segmentInfo.segmentSize,
                keyValueCount = segmentInfo.keyValueCount
              ),
            overlappingSegmentInfo =
              SegmentInfo(
                path = overlappingSegmentInfo.path,
                minKey = serializer.read(overlappingSegmentInfo.minKey),
                maxKey = serializer.read(overlappingSegmentInfo.maxKey),
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

private[swaydb] class SwayDB(api: CoreAPI) extends SwayDBAPI {

  override def put(key: Slice[Byte]) =
    api.put(key)

  override def put(key: Slice[Byte], value: Slice[Byte]) =
    api.put(key, value)

  override def put(key: Slice[Byte], value: Option[Slice[Byte]]) =
    api.put(key, value)

  override def put(entries: Iterable[request.Batch]) =
    entries.foldLeft(Option.empty[MapEntry[Slice[Byte], Value]]) {
      case (mapEntry, batchEntry) =>
        val nextEntry =
          batchEntry match {
            case request.Batch.Put(key, value) =>
              MapEntry.Add[Slice[Byte], Value.Put](key, Value.Put(value))(LevelZeroMapEntryWriter.Level0AddWriter)

            case request.Batch.Remove(key) =>
              MapEntry.Add[Slice[Byte], Value.Remove](key, Value.Remove)(LevelZeroMapEntryWriter.Level0RemoveWriter)
          }
        Some(mapEntry.map(_ ++ nextEntry) getOrElse nextEntry)
    } map {
      entry =>
        api.put(entry)
    } getOrElse Failure(new Exception("Cannot write empty batch"))

  override def remove(key: Slice[Byte]) =
    api.remove(key)

  override def head: Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    api.head

  override def last: Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    api.last

  override def keyValueCount: Try[Int] =
    api.keyValueCount

  override def contains(key: Slice[Byte]): Try[Boolean] =
    api contains key

  def mightContain(key: Slice[Byte]): Try[Boolean] =
    api mightContain key

  override def get(key: Slice[Byte]): Try[Option[Option[Slice[Byte]]]] =
    api.get(key)

  override def getKey(key: Slice[Byte]): Try[Option[Slice[Byte]]] =
    api.getKey(key)

  override def getKeyValue(key: Slice[Byte]): Try[Option[(Slice[Byte], Option[Slice[Byte]])]] =
    api.getKeyValue(key)

  override def beforeKey(key: Slice[Byte]) =
    api.beforeKey(key)

  override def before(key: Slice[Byte]) =
    api.before(key)

  override def afterKey(key: Slice[Byte]) =
    api.afterKey(key)

  override def after(key: Slice[Byte]) =
    api.after(key)

  override def headKey: Try[Option[Slice[Byte]]] =
    api.headKey

  override def lastKey: Try[Option[Slice[Byte]]] =
    api.lastKey

  override def sizeOfSegments: Long =
    api.sizeOfSegments

  override def level0Meter: Level0Meter =
    api.level0Meter

  override def level1Meter: LevelMeter =
    api.level1Meter

  override def levelMeter(levelNumber: Int): Option[LevelMeter] =
    api.levelMeter(levelNumber)
}