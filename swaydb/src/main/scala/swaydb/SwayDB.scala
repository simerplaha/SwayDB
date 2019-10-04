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

package swaydb

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.core.Core
import swaydb.core.actor.FileSweeper
import swaydb.core.data._
import swaydb.core.function.FunctionStore
import swaydb.core.level.tool.AppendixRepairer
import swaydb.data.MaxKey
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.repairAppendix.RepairResult.OverlappingSegments
import swaydb.data.repairAppendix._
import swaydb.data.slice.Slice
import swaydb.serializers.Serializer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.concurrent.forkjoin.ForkJoinPool

/**
 * Instance used for creating/initialising databases.
 */
object SwayDB extends LazyLogging {

  private implicit val memoryFunctionStore: FunctionStore = FunctionStore.memory()
  private implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  /**
   * Default execution context for all databases.
   *
   * This can be overridden by provided an implicit parameter in the scope of where the database is initialized.
   */
  lazy val defaultExecutionContext = new ExecutionContext {
    val threadPool = new ForkJoinPool(Runtime.getRuntime.availableProcessors())

    def execute(runnable: Runnable): Unit =
      threadPool execute runnable

    def reportFailure(exception: Throwable): Unit =
      logger.error("Execution context failure", exception)
  }

  /**
   * Creates a database based on the input config.
   *
   * @param config                         Configuration to use to create the database
   * @param keySerializer                  Converts keys to Bytes
   * @param valueSerializer                Converts values to Bytes
   * @param keyOrder                       Sort order for keys
   * @tparam K Type of key
   * @tparam V Type of value
   *
   * @return Database instance
   */
  def apply[K, V](config: SwayDBPersistentConfig,
                  fileCache: FileCache.Enable,
                  memoryCache: MemoryCache)(implicit keySerializer: Serializer[K],
                                            valueSerializer: Serializer[V],
                                            keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Boot, swaydb.Map[K, V, IO.ApiIO]] =
    Core(
      config = config,
      fileCache = fileCache,
      memoryCache = memoryCache
    ) map {
      db =>
        swaydb.Map[K, V, IO.ApiIO](db)
    }

  def apply[T](config: SwayDBPersistentConfig,
               fileCache: FileCache.Enable,
               memoryCache: MemoryCache)(implicit serializer: Serializer[T],
                                         keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Boot, swaydb.Set[T, IO.ApiIO]] =
    Core(
      config = config,
      fileCache = fileCache,
      memoryCache = memoryCache
    ) map {
      db =>
        swaydb.Set[T](db)
    }

  def apply[K, V](config: SwayDBMemoryConfig,
                  fileCache: FileCache.Enable,
                  memoryCache: MemoryCache)(implicit keySerializer: Serializer[K],
                                            valueSerializer: Serializer[V],
                                            keyOrder: KeyOrder[Slice[Byte]],
                                            fileSweeperEC: ExecutionContext,
                                            memorySweeperEC: ExecutionContext): IO[swaydb.Error.Boot, swaydb.Map[K, V, IO.ApiIO]] =
    Core(
      config = config,
      fileCache = fileCache,
      memoryCache = memoryCache
    ) map {
      db =>
        swaydb.Map[K, V, IO.ApiIO](db)
    }

  def apply[T](config: SwayDBMemoryConfig,
               fileCache: FileCache.Enable,
               memoryCache: MemoryCache)(implicit serializer: Serializer[T],
                                         keyOrder: KeyOrder[Slice[Byte]],
                                         fileSweeperEC: ExecutionContext,
                                         memorySweeperEC: ExecutionContext): IO[swaydb.Error.Boot, swaydb.Set[T, IO.ApiIO]] =
    Core(
      config = config,
      fileCache = fileCache,
      memoryCache = memoryCache
    ) map {
      db =>
        swaydb.Set[T](db)
    }

  def apply[T](config: LevelZeroPersistentConfig)(implicit serializer: Serializer[T],
                                                  keyOrder: KeyOrder[Slice[Byte]],
                                                  mmapCleanerEC: Option[ExecutionContext]): IO[swaydb.Error.Boot, swaydb.Set[T, IO.ApiIO]] =
    Core(
      config = config
    ) map {
      db =>
        swaydb.Set[T](db)
    }

  def apply[T](config: LevelZeroMemoryConfig)(implicit serializer: Serializer[T],
                                              keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Boot, swaydb.Set[T, IO.ApiIO]] =
    Core(
      config = config
    ) map {
      db =>
        swaydb.Set[T](db)
    }

  private def toCoreFunctionOutput[V](output: swaydb.Apply[V])(implicit valueSerializer: Serializer[V]): SwayFunctionOutput =
    output match {
      case Apply.Nothing =>
        SwayFunctionOutput.Nothing

      case Apply.Remove =>
        SwayFunctionOutput.Remove

      case Apply.Expire(deadline) =>
        SwayFunctionOutput.Expire(deadline)

      case update: Apply.Update[V] =>
        val untypedValue: Slice[Byte] = valueSerializer.write(update.value)
        SwayFunctionOutput.Update(Some(untypedValue), update.deadline)
    }

  private[swaydb] def toCoreFunction[K, V](f: (K, Option[Deadline]) => Apply[V])(implicit keySerializer: Serializer[K],
                                                                                 valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(key: Slice[Byte], deadline: Option[Deadline]) =
      toCoreFunctionOutput(f(key.read[K], deadline))

    swaydb.core.data.SwayFunction.KeyDeadline(function)
  }

  private[swaydb] def toCoreFunction[K, V](f: (K, V, Option[Deadline]) => Apply[V])(implicit keySerializer: Serializer[K],
                                                                                    valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(key: Slice[Byte], value: Option[Slice[Byte]], deadline: Option[Deadline]) =
      toCoreFunctionOutput(f(key.read[K], value.read[V], deadline))

    swaydb.core.data.SwayFunction.KeyValueDeadline(function)
  }

  private[swaydb] def toCoreFunction[K, V](f: V => Apply[V])(implicit valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(value: Option[Slice[Byte]]) =
      toCoreFunctionOutput(f(value.read[V]))

    swaydb.core.data.SwayFunction.Value(function)
  }

  /**
   * Documentation: http://www.swaydb.io/api/repairAppendix
   */
  def repairAppendix[K](levelPath: Path,
                        repairStrategy: AppendixRepairStrategy)(implicit serializer: Serializer[K],
                                                                fileSweeper: FileSweeper.Enabled,
                                                                keyOrder: KeyOrder[Slice[Byte]] = KeyOrder.default,
                                                                ec: ExecutionContext = defaultExecutionContext): IO[swaydb.Error.Level, RepairResult[K]] =
  //convert to typed result.
    AppendixRepairer(levelPath, repairStrategy) match {
      case IO.Left(swaydb.Error.Fatal(OverlappingSegmentsException(segmentInfo, overlappingSegmentInfo))) =>
        IO.Right[swaydb.Error.Segment, RepairResult[K]](
          OverlappingSegments[K](
            segmentInfo =
              SegmentInfo(
                path = segmentInfo.path,
                minKey = serializer.read(segmentInfo.minKey),
                maxKey =
                  segmentInfo.maxKey match {
                    case MaxKey.Fixed(maxKey) =>
                      MaxKey.Fixed(serializer.read(maxKey))

                    case MaxKey.Range(fromKey, maxKey) =>
                      MaxKey.Range(fromKey = serializer.read(fromKey), maxKey = serializer.read(maxKey))
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
                    case MaxKey.Fixed(maxKey) =>
                      MaxKey.Fixed(serializer.read(maxKey))

                    case MaxKey.Range(fromKey, maxKey) =>
                      MaxKey.Range(fromKey = serializer.read(fromKey), maxKey = serializer.read(maxKey))
                  },
                segmentSize = overlappingSegmentInfo.segmentSize,
                keyValueCount = overlappingSegmentInfo.keyValueCount
              )
          )
        )
      case IO.Left(error) =>
        IO.Left(error)

      case IO.Right(_) =>
        IO.Right[swaydb.Error.Segment, RepairResult[K]](RepairResult.Repaired)
    }
}
