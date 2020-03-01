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
 */

package swaydb.java.memory

import swaydb.core.util.Eithers
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{FileCache, ThreadStateCache}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.java.data.slice.ByteSlice
import swaydb.data.util.Java.JavaFunction
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.java.{KeyComparator, KeyOrderConverter, PureFunction, Return}
import swaydb.memory.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.{Apply, Bag}

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration._
import scala.reflect.ClassTag

object MapConfig {

  class Config[K, V, F](@BeanProperty var mapSize: Int = 4.mb,
                        @BeanProperty var minSegmentSize: Int = 2.mb,
                        @BeanProperty var maxKeyValuesPerSegment: Int = Int.MaxValue,
                        @BooleanBeanProperty var deleteSegmentsEventually: Boolean = true,
                        @BeanProperty var fileCache: FileCache.Enable = DefaultConfigs.fileCache(),
                        @BeanProperty var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                        @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                        @BeanProperty var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
                        @BeanProperty var lastLevelThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.lastLevelThrottle _).asJava,
                        @BeanProperty var byteComparator: KeyComparator[ByteSlice] = null,
                        @BeanProperty var typedComparator: KeyComparator[K] = null,
                        keySerializer: Serializer[K],
                        valueSerializer: Serializer[V],
                        functionClassTag: ClassTag[_]) {

    private def comparator: Either[KeyComparator[ByteSlice], KeyComparator[K]] =
      Eithers.nullCheck(
        left = byteComparator,
        right = typedComparator,
        default = swaydb.java.SwayDB.defaultComparator
      )

    implicit def scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, keySerializer)

    private val functions = swaydb.Map.Functions[K, V, swaydb.PureFunction[K, V, Apply.Map[V]]]()(keySerializer, valueSerializer)

    def registerFunctions(functions: F*): Config[K, V, F] = {
      functions.foreach(registerFunction(_))
      this
    }

    def registerFunction(function: F): Config[K, V, F] = {
      val scalaFunction = PureFunction.asScala(function.asInstanceOf[swaydb.java.PureFunction[K, V, Return.Map[V]]])
      functions.register(scalaFunction)
      this
    }

    def removeFunction(function: F): Config[K, V, F] = {
      val scalaFunction = function.asInstanceOf[swaydb.java.PureFunction[K, V, Return.Map[V]]].id.asInstanceOf[Slice[Byte]]
      functions.core.remove(scalaFunction)
      this
    }

    def init(): swaydb.java.Map[K, V, F] = {
      val scalaMap =
        swaydb.memory.Map[K, V, swaydb.PureFunction[K, V, Apply.Map[V]], Bag.Less](
          mapSize = mapSize,
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          fileCache = fileCache,
          deleteSegmentsEventually = deleteSegmentsEventually,
          acceleration = acceleration.asScala,
          levelZeroThrottle = levelZeroThrottle.asScala,
          lastLevelThrottle = lastLevelThrottle.asScala,
          threadStateCache = threadStateCache
        )(keySerializer = keySerializer,
          valueSerializer = valueSerializer,
          functions = functions.asInstanceOf[swaydb.Map.Functions[K, V, swaydb.PureFunction[K, V, Apply.Map[V]]]],
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[swaydb.PureFunction[K, V, Apply.Map[V]]]],
          bag = Bag.less,
          byteKeyOrder = scalaKeyOrder
        ).get

      swaydb.java.Map[K, V, F](scalaMap)
    }
  }

  def withFunctions[K, V](keySerializer: JavaSerializer[K],
                          valueSerializer: JavaSerializer[V]): Config[K, V, swaydb.java.PureFunction[K, V, Return.Map[V]]] =
    new Config(
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag(classOf[swaydb.PureFunction[K, V, Apply.Map[V]]])
    )

  def withoutFunctions[K, V](keySerializer: JavaSerializer[K],
                             valueSerializer: JavaSerializer[V]): Config[K, V, Void] =
    new Config[K, V, Void](
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Nothing
    )
}
