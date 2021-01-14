/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.persistent

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.build.BuildValidator
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionExecutionContext, LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.sequencer.Sequencer
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.data.{Atomic, DataType, Functions, OptimiseWrites}
import swaydb.function.FunctionConverter
import swaydb.multimap.{MultiKey, MultiValue}
import swaydb.serializers.Serializer
import swaydb.{Apply, CommonConfigs, KeyOrderConverter, MultiMap, PureFunction}

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object MultiMap extends LazyLogging {

  /**
   * MultiMap is not a new Core type but is just a wrapper implementation on [[swaydb.Map]] type
   * with custom key-ordering to support nested Map.
   *
   * @tparam M   Map's key type.
   * @tparam K   Key-values key type
   * @tparam V   Values type
   * @tparam F   Function type
   * @tparam BAG Effect type
   */
  def apply[M, K, V, F <: PureFunction.Map[K, V], BAG[_]](dir: Path,
                                                          mapSize: Int = DefaultConfigs.mapSize,
                                                          appliedFunctionsMapSize: Int = 4.mb,
                                                          clearAppliedFunctionsOnBoot: Boolean = false,
                                                          mmapMaps: MMAP.Map = DefaultConfigs.mmap(),
                                                          recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                                                          mmapAppendix: MMAP.Map = DefaultConfigs.mmap(),
                                                          appendixFlushCheckpointSize: Int = 2.mb,
                                                          otherDirs: Seq[Dir] = Seq.empty,
                                                          cacheKeyValueIds: Boolean = true,
                                                          compactionExecutionContext: CompactionExecutionContext.Create = CommonConfigs.compactionExecutionContext(),
                                                          optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                                                          atomic: Atomic = CommonConfigs.atomic(),
                                                          acceleration: LevelZeroMeter => Accelerator = DefaultConfigs.accelerator,
                                                          threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                                                          sortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                                                          randomSearchIndex: RandomSearchIndex = DefaultConfigs.randomSearchIndex(),
                                                          binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                                                          mightContainIndex: MightContainIndex = DefaultConfigs.mightContainIndex(),
                                                          valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                                                          segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                                                          fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                                          memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                                                          levelZeroThrottle: LevelZeroMeter => FiniteDuration = DefaultConfigs.levelZeroThrottle,
                                                          levelOneThrottle: LevelMeter => Throttle = DefaultConfigs.levelOneThrottle,
                                                          levelTwoThrottle: LevelMeter => Throttle = DefaultConfigs.levelTwoThrottle,
                                                          levelThreeThrottle: LevelMeter => Throttle = DefaultConfigs.levelThreeThrottle,
                                                          levelFourThrottle: LevelMeter => Throttle = DefaultConfigs.levelFourThrottle,
                                                          levelFiveThrottle: LevelMeter => Throttle = DefaultConfigs.levelFiveThrottle,
                                                          levelSixThrottle: LevelMeter => Throttle = DefaultConfigs.levelSixThrottle)(implicit keySerializer: Serializer[K],
                                                                                                                                      mapKeySerializer: Serializer[M],
                                                                                                                                      valueSerializer: Serializer[V],
                                                                                                                                      functionClassTag: ClassTag[F],
                                                                                                                                      functions: Functions[F],
                                                                                                                                      bag: swaydb.Bag[BAG],
                                                                                                                                      sequencer: Sequencer[BAG] = null,
                                                                                                                                      byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                                      typedKeyOrder: KeyOrder[K] = null,
                                                                                                                                      buildValidator: BuildValidator = BuildValidator.DisallowOlderVersions(DataType.MultiMap)): BAG[MultiMap[M, K, V, F, BAG]] =
    bag.suspend {
      implicit val multiKeySerializer: Serializer[MultiKey[M, K]] = MultiKey.serializer(keySerializer, mapKeySerializer)
      implicit val multiValueSerializer: Serializer[MultiValue[V]] = MultiValue.serialiser(valueSerializer)
      val mapFunctions = FunctionConverter.toMultiMap[M, K, V, Apply.Map[V], F](functions)

      val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
      val internalKeyOrder: KeyOrder[Slice[Byte]] = MultiKey.ordering(keyOrder)

      //the inner map with custom keyOrder and custom key-value types to support nested Maps.
      val map =
        swaydb.persistent.Map[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG](
          dir = dir,
          mapSize = mapSize,
          appliedFunctionsMapSize = appliedFunctionsMapSize,
          clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
          mmapMaps = mmapMaps,
          recoveryMode = recoveryMode,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          otherDirs = otherDirs,
          cacheKeyValueIds = cacheKeyValueIds,
          compactionExecutionContext = compactionExecutionContext,
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          acceleration = acceleration,
          threadStateCache = threadStateCache,
          sortedKeyIndex = sortedKeyIndex,
          randomSearchIndex = randomSearchIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainIndex = mightContainIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
          levelZeroThrottle = levelZeroThrottle,
          levelOneThrottle = levelOneThrottle,
          levelTwoThrottle = levelTwoThrottle,
          levelThreeThrottle = levelThreeThrottle,
          levelFourThrottle = levelFourThrottle,
          levelFiveThrottle = levelFiveThrottle,
          levelSixThrottle = levelSixThrottle
        )(keySerializer = multiKeySerializer,
          valueSerializer = multiValueSerializer,
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]]],
          bag = bag,
          sequencer = sequencer,
          functions = mapFunctions,
          byteKeyOrder = internalKeyOrder,
          buildValidator = buildValidator and BuildValidator.MultiMapFileExists(dir.resolve(swaydb.MultiMap.folderName))
        )

      bag.flatMap(map) {
        map =>
          swaydb.MultiMap.withPersistentCounter(
            path = dir,
            mmap = mmapMaps,
            map = map
          )
      }
    }
}
