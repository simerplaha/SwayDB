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

package swaydb.memory

import com.typesafe.scalalogging.LazyLogging
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.map.counter.CounterMap
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.sequencer.Sequencer
import swaydb.data.slice.Slice
import swaydb.data.{Atomic, Functions, OptimiseWrites}
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
  def apply[M, K, V, F <: PureFunction.Map[K, V], BAG[_]](mapSize: Int = DefaultConfigs.mapSize,
                                                          minSegmentSize: Int = DefaultConfigs.segmentSize,
                                                          maxKeyValuesPerSegment: Int = Int.MaxValue,
                                                          fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                                          deleteDelay: FiniteDuration = CommonConfigs.segmentDeleteDelay,
                                                          compactionConfig: CompactionConfig = CommonConfigs.compactionConfig(),
                                                          optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                                                          atomic: Atomic = CommonConfigs.atomic(),
                                                          acceleration: LevelZeroMeter => Accelerator = DefaultConfigs.accelerator,
                                                          levelZeroThrottle: LevelZeroMeter => LevelZeroThrottle = DefaultConfigs.levelZeroThrottle,
                                                          lastLevelThrottle: LevelMeter => LevelThrottle = DefaultConfigs.lastLevelThrottle,
                                                          threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10))(implicit keySerializer: Serializer[K],
                                                                                                                                                            mapKeySerializer: Serializer[M],
                                                                                                                                                            valueSerializer: Serializer[V],
                                                                                                                                                            functionClassTag: ClassTag[F],
                                                                                                                                                            functions: Functions[F],
                                                                                                                                                            bag: swaydb.Bag[BAG],
                                                                                                                                                            sequencer: Sequencer[BAG] = null,
                                                                                                                                                            byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                                                            typedKeyOrder: KeyOrder[K] = null): BAG[MultiMap[M, K, V, F, BAG]] =
    bag.suspend {
      implicit val multiKeySerializer: Serializer[MultiKey[M, K]] = MultiKey.serializer(keySerializer, mapKeySerializer)
      implicit val multiValueSerializer: Serializer[MultiValue[V]] = MultiValue.serialiser(valueSerializer)
      val mapFunctions = FunctionConverter.toMultiMap[M, K, V, Apply.Map[V], F](functions)

      val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
      val internalKeyOrder: KeyOrder[Slice[Byte]] = MultiKey.ordering(keyOrder)

      //the inner map with custom keyOrder and custom key-value types to support nested Maps.
      val map =
        swaydb.memory.Map[MultiKey[M, K], MultiValue[V], PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]], BAG](
          mapSize = mapSize,
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          fileCache = fileCache,
          deleteDelay = deleteDelay,
          compactionConfig = compactionConfig,
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          acceleration = acceleration,
          levelZeroThrottle = levelZeroThrottle,
          lastLevelThrottle = lastLevelThrottle,
          threadStateCache = threadStateCache
        )(keySerializer = multiKeySerializer,
          valueSerializer = multiValueSerializer,
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[PureFunction[MultiKey[M, K], MultiValue[V], Apply.Map[MultiValue[V]]]]],
          bag = bag,
          sequencer = sequencer,
          functions = mapFunctions,
          byteKeyOrder = internalKeyOrder
        )

      bag.flatMap(map) {
        map =>
          implicit val counter = CounterMap.memory()

          swaydb.MultiMap[M, K, V, F, BAG](map)
      }
    }
}
