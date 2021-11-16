/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.eventually.persistent

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.build.BuildValidator
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.CompactionConfig
import swaydb.data.config._
import swaydb.slice.order.KeyOrder
import swaydb.data.sequencer.Sequencer
import swaydb.slice.Slice
import swaydb.data.{Atomic, DataType, Functions, OptimiseWrites}
import swaydb.effect.Dir
import swaydb.function.FunctionConverter
import swaydb.multimap.{MultiKey, MultiValue}
import swaydb.serializers.Serializer
import swaydb.utils.StorageUnits._
import swaydb.{Apply, CommonConfigs, KeyOrderConverter, MultiMap, PureFunction}

import java.nio.file.Path
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object MultiMap extends LazyLogging {

  /**
   * A 3 Leveled in-memory database where the 3rd is persistent.
   *
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[M, K, V, F <: PureFunction.Map[K, V], BAG[_]](dir: Path,
                                                          logSize: Int = DefaultConfigs.logSize,
                                                          appliedFunctionsLogSize: Int = 4.mb,
                                                          clearAppliedFunctionsOnBoot: Boolean = false,
                                                          maxMemoryLevelSize: Int = 100.mb,
                                                          maxSegmentsToPush: Int = 5,
                                                          memoryLevelSegmentSize: Int = DefaultConfigs.segmentSize,
                                                          memoryLevelMaxKeyValuesCountPerSegment: Int = 200000,
                                                          persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                                                          otherDirs: Seq[Dir] = Seq.empty,
                                                          cacheKeyValueIds: Boolean = true,
                                                          mmapPersistentLevelAppendixLogs: MMAP.Log = DefaultConfigs.mmap(),
                                                          memorySegmentDeleteDelay: FiniteDuration = CommonConfigs.segmentDeleteDelay,
                                                          compactionConfig: CompactionConfig = CommonConfigs.compactionConfig(),
                                                          optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                                                          atomic: Atomic = CommonConfigs.atomic(),
                                                          acceleration: LevelZeroMeter => Accelerator = DefaultConfigs.accelerator,
                                                          persistentLevelSortedIndex: SortedIndex = DefaultConfigs.sortedIndex(),
                                                          persistentLevelHashIndex: HashIndex = DefaultConfigs.hashIndex(),
                                                          binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                                                          bloomFilter: BloomFilter = DefaultConfigs.bloomFilter(),
                                                          valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                                                          segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                                                          fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                                          memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                                                          threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10))(implicit keySerializer: Serializer[K],
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
      val functionStore = FunctionConverter.toMultiMap[M, K, V, Apply.Map[V], F](functions)

      val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
      val internalKeyOrder: KeyOrder[Slice[Byte]] = MultiKey.ordering(keyOrder)

      //the inner map with custom keyOrder and custom key-value types to support nested Maps.
      val map =
        swaydb.eventually.persistent.Map[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG](
          dir = dir,
          logSize = logSize,
          appliedFunctionsLogSize = appliedFunctionsLogSize,
          clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
          maxMemoryLevelSize = maxMemoryLevelSize,
          maxSegmentsToPush = maxSegmentsToPush,
          memoryLevelSegmentSize = memoryLevelSegmentSize,
          memoryLevelMaxKeyValuesCountPerSegment = memoryLevelMaxKeyValuesCountPerSegment,
          persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
          otherDirs = otherDirs,
          cacheKeyValueIds = cacheKeyValueIds,
          mmapPersistentLevelAppendixLogs = mmapPersistentLevelAppendixLogs,
          memorySegmentDeleteDelay = memorySegmentDeleteDelay,
          compactionConfig = compactionConfig,
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          acceleration = acceleration,
          persistentLevelSortedIndex = persistentLevelSortedIndex,
          persistentLevelHashIndex = persistentLevelHashIndex,
          binarySearchIndex = binarySearchIndex,
          bloomFilter = bloomFilter,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
          threadStateCache = threadStateCache
        )(keySerializer = multiKeySerializer,
          valueSerializer = multiValueSerializer,
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]]],
          bag = bag,
          sequencer = sequencer,
          functions = functionStore,
          byteKeyOrder = internalKeyOrder,
          buildValidator = buildValidator and BuildValidator.MultiMapFileExists(dir.resolve(swaydb.MultiMap.folderName))
        )

      bag.flatMap(map) {
        map =>
          swaydb.MultiMap.withPersistentCounter(
            path = dir,
            mmap = mmapPersistentLevelAppendixLogs,
            map = map
          )
      }
    }
}
