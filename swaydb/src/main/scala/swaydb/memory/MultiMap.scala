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

package swaydb.memory

import com.typesafe.scalalogging.LazyLogging
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.serializers.Serializer
import swaydb.{Apply, Error, IO, KeyOrderConverter, MultiMap, MultiMapKey, PureFunction}

import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object MultiMap extends LazyLogging {

  /**
   * MultiMap is not a new Core type but is just a wrapper implementation on [[swaydb.Map]] type
   * with custom key-ordering to support nested Map.
   */
  def apply[K, V, F, BAG[_]](mapSize: Int = 4.mb,
                             minSegmentSize: Int = 2.mb,
                             maxKeyValuesPerSegment: Int = Int.MaxValue,
                             fileCache: FileCache.Enable = DefaultConfigs.fileCache(),
                             deleteSegmentsEventually: Boolean = true,
                             acceleration: LevelZeroMeter => Accelerator = Accelerator.noBrakes(),
                             levelZeroThrottle: LevelZeroMeter => FiniteDuration = DefaultConfigs.levelZeroThrottle,
                             lastLevelThrottle: LevelMeter => Throttle = DefaultConfigs.lastLevelThrottle,
                             threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10))(implicit keySerializer: Serializer[K],
                                                                                                                               valueSerializer: Serializer[V],
                                                                                                                               functionClassTag: ClassTag[F],
                                                                                                                               bag: swaydb.Bag[BAG],
                                                                                                                               functions: swaydb.MultiMap.Functions[K, V, F],
                                                                                                                               byteKeyOrder: KeyOrder[Slice[Byte]] = null,
                                                                                                                               typedKeyOrder: KeyOrder[K] = null): BAG[MultiMap[K, V, F, BAG]] = {

    implicit val mapKeySerializer: Serializer[MultiMapKey[K]] = MultiMapKey.serializer(keySerializer)
    implicit val optionValueSerializer: Serializer[Option[V]] = Serializer.toNestedOption(valueSerializer)

    val keyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.typedToBytesNullCheck(byteKeyOrder, typedKeyOrder)
    val internalKeyOrder: KeyOrder[Slice[Byte]] = MultiMapKey.ordering(keyOrder)

    //the inner map with custom keyOrder and custom key-value types to support nested Maps.
    val map =
      swaydb.memory.Map[MultiMapKey[K], Option[V], PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]], BAG](
        mapSize = mapSize,
        minSegmentSize = minSegmentSize,
        maxKeyValuesPerSegment = maxKeyValuesPerSegment,
        fileCache = fileCache,
        deleteSegmentsEventually = deleteSegmentsEventually,
        acceleration = acceleration,
        levelZeroThrottle = levelZeroThrottle,
        lastLevelThrottle = lastLevelThrottle,
        threadStateCache = threadStateCache
      )(keySerializer = mapKeySerializer,
        valueSerializer = optionValueSerializer,
        functionClassTag = functionClassTag.asInstanceOf[ClassTag[PureFunction[MultiMapKey[K], Option[V], Apply.Map[Option[V]]]]],
        bag = bag,
        functions = functions.innerFunctions,
        byteKeyOrder = internalKeyOrder
      ).toBag[BAG]

    bag.flatMap(map) {
      map =>
        swaydb.MultiMap[K, V, F, BAG](map)
    }
  }


}
