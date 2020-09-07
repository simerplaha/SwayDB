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

package swaydb.eventually.persistent

import java.nio.file.Path

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.build.BuildValidator
import swaydb.data.DataType
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.config.{ThreadStateCache, _}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.multimap.{MultiKey, MultiValue}
import swaydb.serializers.Serializer
import swaydb.{Apply, KeyOrderConverter, MultiMap, PureFunction}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.reflect.ClassTag

object MultiMap extends LazyLogging {

  /**
   * A 3 Leveled in-memory database where the 3rd is persistent.
   *
   * For custom configurations read documentation on website: http://www.swaydb.io/configuring-levels
   */
  def apply[M, K, V, F, BAG[_]](dir: Path,
                                mapSize: Int = 4.mb,
                                maxMemoryLevelSize: Int = 100.mb,
                                maxSegmentsToPush: Int = 5,
                                memoryLevelSegmentSize: Int = 2.mb,
                                memoryLevelMaxKeyValuesCountPerSegment: Int = 200000,
                                persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                                otherDirs: Seq[Dir] = Seq.empty,
                                shutdownTimeout: FiniteDuration = 30.seconds,
                                cacheKeyValueIds: Boolean = true,
                                mmapPersistentLevelAppendix: MMAP.Map = DefaultConfigs.mmap(),
                                deleteMemorySegmentsEventually: Boolean = true,
                                acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes(),
                                persistentLevelSortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                                persistentLevelRandomKeyIndex: RandomKeyIndex = DefaultConfigs.randomKeyIndex(),
                                binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                                mightContainKeyIndex: MightContainIndex = DefaultConfigs.mightContainKeyIndex(),
                                valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                                segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                                fileCache: FileCache.Enable = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                                threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10))(implicit keySerializer: Serializer[K],
                                                                                                                                  mapKeySerializer: Serializer[M],
                                                                                                                                  valueSerializer: Serializer[V],
                                                                                                                                  functionClassTag: ClassTag[F],
                                                                                                                                  bag: swaydb.Bag[BAG],
                                                                                                                                  functions: swaydb.MultiMap.Functions[M, K, V, F],
                                                                                                                                  byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                                  typedKeyOrder: KeyOrder[K] = null,
                                                                                                                                  compactionEC: ExecutionContext = DefaultExecutionContext.compactionEC,
                                                                                                                                  buildValidator: BuildValidator = BuildValidator.DisallowOlderVersions(DataType.MultiMap)): BAG[MultiMap[M, K, V, F, BAG]] =
    bag.suspend {

      implicit val innerMapKeySerialiser: Serializer[MultiKey[M, K]] = MultiKey.serializer(keySerializer, mapKeySerializer)
      implicit val optionValueSerializer: Serializer[MultiValue[V]] = MultiValue.serialiser(valueSerializer)

      val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
      val internalKeyOrder: KeyOrder[Slice[Byte]] = MultiKey.ordering(keyOrder)

      //the inner map with custom keyOrder and custom key-value types to support nested Maps.
      val map =
        swaydb.eventually.persistent.Map[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG](
          dir = dir,
          mapSize = mapSize,
          maxMemoryLevelSize = maxMemoryLevelSize,
          maxSegmentsToPush = maxSegmentsToPush,
          memoryLevelSegmentSize = memoryLevelSegmentSize,
          memoryLevelMaxKeyValuesCountPerSegment = memoryLevelMaxKeyValuesCountPerSegment,
          persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
          otherDirs = otherDirs,
          shutdownTimeout = shutdownTimeout,
          cacheKeyValueIds = cacheKeyValueIds,
          mmapPersistentLevelAppendix = mmapPersistentLevelAppendix,
          deleteMemorySegmentsEventually = deleteMemorySegmentsEventually,
          acceleration = acceleration,
          persistentLevelSortedKeyIndex = persistentLevelSortedKeyIndex,
          persistentLevelRandomKeyIndex = persistentLevelRandomKeyIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainKeyIndex = mightContainKeyIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
          threadStateCache = threadStateCache
        )(keySerializer = innerMapKeySerialiser,
          valueSerializer = optionValueSerializer,
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]]],
          bag = bag,
          functions = functions.innerFunctions,
          byteKeyOrder = internalKeyOrder,
          compactionEC = compactionEC,
          buildValidator = buildValidator
        )

      bag.flatMap(map) {
        map =>
          swaydb.MultiMap.withPersistentCounter(
            path = dir,
            mmap = mmapPersistentLevelAppendix,
            map = map
          )
      }
    }
}
