/*
 * Copyright (c) 2020 Simer Plaha (@simerplaha)
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

package swaydb

import java.nio.file.Path
import java.util.concurrent.{ExecutorService, Executors}

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.SingleThreadFactory
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
import swaydb.data.slice.{Slice, SliceOptional}
import swaydb.serializers.Serializer

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

/**
 * Instance used for creating/initialising databases.
 */
object SwayDB extends LazyLogging {

  private implicit def memoryFunctionStore: FunctionStore = FunctionStore.memory()
  private implicit val timeOrder: TimeOrder[Slice[Byte]] = TimeOrder.long

  /**
   * Default execution context for all databases.
   *
   * This can be overridden by provided an implicit parameter in the scope of where the database is initialized.
   */
  lazy val sweeperExecutionContext = new ExecutionContext {
    val threadPool = Executors.newSingleThreadExecutor(SingleThreadFactory.create())

    def execute(runnable: Runnable): Unit =
      threadPool execute runnable

    def reportFailure(exception: Throwable): Unit =
      logger.error("sweeperExecutionContext context failure", exception)
  }

  lazy val sweeperExecutorService: ExecutorService =
    sweeperExecutionContext.threadPool

  /**
   * Creates a database based on the input config.
   *
   * @param config          Configuration to use to create the database
   * @param keySerializer   Converts keys to Bytes
   * @param valueSerializer Converts values to Bytes
   * @param keyOrder        Sort order for keys
   * @tparam K Type of key
   * @tparam V Type of value
   * @return Database instance
   */
  def apply[K, V, F](config: SwayDBPersistentConfig,
                     fileCache: FileCache.Enable,
                     memoryCache: MemoryCache)(implicit keySerializer: Serializer[K],
                                               valueSerializer: Serializer[V],
                                               functionClassTag: ClassTag[F],
                                               keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Boot, swaydb.Map[K, V, F, IO.ApiIO]] =
    Core(
      config = config,
      enableTimer = functionClassTag != ClassTag.Nothing,
      fileCache = fileCache,
      memoryCache = memoryCache
    ) map {
      db =>
        swaydb.Map[K, V, F, IO.ApiIO](db)
    }

  def apply[T, F](config: SwayDBPersistentConfig,
                  fileCache: FileCache.Enable,
                  memoryCache: MemoryCache)(implicit serializer: Serializer[T],
                                            functionClassTag: ClassTag[F],
                                            keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Boot, swaydb.Set[T, F, IO.ApiIO]] =
    Core(
      config = config,
      enableTimer = functionClassTag != ClassTag.Nothing,
      fileCache = fileCache,
      memoryCache = memoryCache
    ) map {
      db =>
        swaydb.Set[T, F, IO.ApiIO](db)
    }

  def apply[K, V, F](config: SwayDBMemoryConfig,
                     fileCache: FileCache.Enable,
                     memoryCache: MemoryCache)(implicit keySerializer: Serializer[K],
                                               valueSerializer: Serializer[V],
                                               functionClassTag: ClassTag[F],
                                               keyOrder: KeyOrder[Slice[Byte]],
                                               fileSweeperEC: ExecutionContext,
                                               memorySweeperEC: ExecutionContext): IO[swaydb.Error.Boot, swaydb.Map[K, V, F, IO.ApiIO]] =
    Core(
      config = config,
      enableTimer = functionClassTag != ClassTag.Nothing,
      fileCache = fileCache,
      memoryCache = memoryCache
    ) map {
      db =>
        swaydb.Map[K, V, F, IO.ApiIO](db)
    }

  def apply[T, F](config: SwayDBMemoryConfig,
                  fileCache: FileCache.Enable,
                  memoryCache: MemoryCache)(implicit serializer: Serializer[T],
                                            functionClassTag: ClassTag[F],
                                            keyOrder: KeyOrder[Slice[Byte]],
                                            fileSweeperEC: ExecutionContext,
                                            memorySweeperEC: ExecutionContext): IO[swaydb.Error.Boot, swaydb.Set[T, F, IO.ApiIO]] =
    Core(
      config = config,
      enableTimer = functionClassTag != ClassTag.Nothing,
      fileCache = fileCache,
      memoryCache = memoryCache
    ) map {
      db =>
        swaydb.Set[T, F, IO.ApiIO](db)
    }

  def apply[T, F](config: LevelZeroPersistentConfig)(implicit serializer: Serializer[T],
                                                     functionClassTag: ClassTag[F],
                                                     keyOrder: KeyOrder[Slice[Byte]],
                                                     mmapCleanerEC: Option[ExecutionContext]): IO[swaydb.Error.Boot, swaydb.Set[T, F, IO.ApiIO]] =
    Core(
      config = config,
      enableTimer = false
    ) map {
      db =>
        swaydb.Set[T, F, IO.ApiIO](db)
    }

  def apply[T, F](config: LevelZeroMemoryConfig)(implicit serializer: Serializer[T],
                                                 functionClassTag: ClassTag[F],
                                                 keyOrder: KeyOrder[Slice[Byte]]): IO[swaydb.Error.Boot, swaydb.Set[T, F, IO.ApiIO]] =
    Core(
      config = config,
      enableTimer = false
    ) map {
      db =>
        swaydb.Set[T, F, IO.ApiIO](db)
    }

  private def toCoreFunctionOutput[V](output: Apply[V])(implicit valueSerializer: Serializer[V]): SwayFunctionOutput =
    output match {
      case Apply.Nothing =>
        SwayFunctionOutput.Nothing

      case Apply.Remove =>
        SwayFunctionOutput.Remove

      case Apply.Expire(deadline) =>
        SwayFunctionOutput.Expire(deadline)

      case update: Apply.Update[V] =>
        val untypedValue: Slice[Byte] = valueSerializer.write(update.value)
        SwayFunctionOutput.Update(untypedValue, update.deadline)
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

    def function(key: Slice[Byte], value: SliceOptional[Byte], deadline: Option[Deadline]) =
      toCoreFunctionOutput(f(key.read[K], value.read[V], deadline))

    swaydb.core.data.SwayFunction.KeyValueDeadline(function)
  }

  private[swaydb] def toCoreFunction[K, V](f: V => Apply[V])(implicit valueSerializer: Serializer[V]): swaydb.core.data.SwayFunction = {
    import swaydb.serializers._

    def function(value: SliceOptional[Byte]) =
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
                                                                ec: ExecutionContext = sweeperExecutionContext): IO[swaydb.Error.Level, RepairResult[K]] =
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
