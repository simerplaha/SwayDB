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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.persistent

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.{Apply, KeyOrderConverter, PureFunction}
import swaydb.configs.level.{DefaultExecutionContext, DefaultPersistentConfig}
import swaydb.core.Core
import swaydb.core.build.BuildValidator
import swaydb.core.function.FunctionStore
import swaydb.data.{DataType, NonEmptyList}
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.order.{KeyOrder, TimeOrder}
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.function.FunctionConverter
import swaydb.serializers.{Default, Serializer}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.reflect.ClassTag

object Set extends LazyLogging {

  /**
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[A, F <: PureFunction.Set[A], BAG[_]](dir: Path,
                                                 mapSize: Int = 4.mb,
                                                 appliedFunctionsMapSize: Int = 4.mb,
                                                 clearAppliedFunctionsOnBoot: Boolean = false,
                                                 mmapMaps: MMAP.Map = DefaultConfigs.mmap(),
                                                 recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                                                 mmapAppendix: MMAP.Map = DefaultConfigs.mmap(),
                                                 appendixFlushCheckpointSize: Int = 2.mb,
                                                 otherDirs: Seq[Dir] = Seq.empty,
                                                 cacheKeyValueIds: Boolean = true,
                                                 shutdownTimeout: FiniteDuration = 30.seconds,
                                                 acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes(),
                                                 threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                                                 sortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                                                 randomKeyIndex: RandomKeyIndex = DefaultConfigs.randomKeyIndex(),
                                                 binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                                                 mightContainKeyIndex: MightContainIndex = DefaultConfigs.mightContainKeyIndex(),
                                                 valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                                                 segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                                                 fileCache: FileCache.Enable = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                                 memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                                                 levelZeroThrottle: LevelZeroMeter => FiniteDuration = DefaultConfigs.levelZeroThrottle,
                                                 levelOneThrottle: LevelMeter => Throttle = DefaultConfigs.levelOneThrottle,
                                                 levelTwoThrottle: LevelMeter => Throttle = DefaultConfigs.levelTwoThrottle,
                                                 levelThreeThrottle: LevelMeter => Throttle = DefaultConfigs.levelThreeThrottle,
                                                 levelFourThrottle: LevelMeter => Throttle = DefaultConfigs.levelFourThrottle,
                                                 levelFiveThrottle: LevelMeter => Throttle = DefaultConfigs.levelFiveThrottle,
                                                 levelSixThrottle: LevelMeter => Throttle = DefaultConfigs.levelSixThrottle)(implicit serializer: Serializer[A],
                                                                                                                             functionClassTag: ClassTag[F],
                                                                                                                             bag: swaydb.Bag[BAG],
                                                                                                                             functions: NonEmptyList[F],
                                                                                                                             byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                             typedKeyOrder: KeyOrder[A] = null,
                                                                                                                             compactionEC: ExecutionContext = DefaultExecutionContext.compactionEC,
                                                                                                                             buildValidator: BuildValidator = BuildValidator.DisallowOlderVersions(DataType.Set)): BAG[swaydb.Set[A, F, BAG]] =
    bag.suspend {
      val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
      implicit val unitSerializer: Serializer[Nothing] = Default.NothingSerializer
      val functionStore: FunctionStore = FunctionConverter.toFunctionsStore[A, Nothing, Apply.Set[Nothing], F](functions)

      val coreIO =
        Core(
          enableTimer = functionClassTag != ClassTag.Nothing && functionClassTag != ClassTag.Unit && functionClassTag != ClassTag.Null,
          cacheKeyValueIds = cacheKeyValueIds,
          threadStateCache = threadStateCache,
          shutdownTimeout = shutdownTimeout,
          config =
            DefaultPersistentConfig(
              dir = dir,
              otherDirs = otherDirs,
              mapSize = mapSize,
              appliedFunctionsMapSize = appliedFunctionsMapSize,
              clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
              mmapMaps = mmapMaps,
              recoveryMode = recoveryMode,
              mmapAppendix = mmapAppendix,
              appendixFlushCheckpointSize = appendixFlushCheckpointSize,
              sortedKeyIndex = sortedKeyIndex,
              randomKeyIndex = randomKeyIndex,
              binarySearchIndex = binarySearchIndex,
              mightContainKeyIndex = mightContainKeyIndex,
              valuesConfig = valuesConfig,
              segmentConfig = segmentConfig,
              acceleration = acceleration,
              levelZeroThrottle = levelZeroThrottle,
              levelOneThrottle = levelOneThrottle,
              levelTwoThrottle = levelTwoThrottle,
              levelThreeThrottle = levelThreeThrottle,
              levelFourThrottle = levelFourThrottle,
              levelFiveThrottle = levelFiveThrottle,
              levelSixThrottle = levelSixThrottle
            ),
          fileCache = fileCache,
          memoryCache = memoryCache
        )(keyOrder = keyOrder,
          timeOrder = TimeOrder.long,
          functionStore = functionStore,
          buildValidator = buildValidator
        ) map {
          db =>
            swaydb.Set[A, F, BAG](db.toBag)
        }

      coreIO.toBag[BAG]
    }
}
