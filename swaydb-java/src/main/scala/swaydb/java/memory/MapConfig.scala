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

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.java.data.slice.ByteSlice
import swaydb.java.data.util.Java.JavaFunction
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.java.{IO, KeyComparator, KeyOrderConverter, PureFunction, Return}
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
                        @BeanProperty var maxKeyValuesPerSegmentGroup: Int = 10,
                        @BooleanBeanProperty var deleteSegmentsEventually: Boolean = true,
                        @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                        @BeanProperty var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
                        @BeanProperty var lastLevelThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.lastLevelThrottle _).asJava,
                        @BeanProperty var comparator: IO[KeyComparator[ByteSlice], KeyComparator[K]] = IO.leftNeverException[KeyComparator[ByteSlice], KeyComparator[K]](swaydb.java.SwayDB.defaultComparator),
                        keySerializer: Serializer[K],
                        valueSerializer: Serializer[V],
                        functionClassTag: ClassTag[_]) {

    implicit def scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, keySerializer)

    private val functions = swaydb.Map.Functions[K, V, swaydb.PureFunction[K, V, Apply.Map[V]]]()(keySerializer, valueSerializer)

    def registerFunctions(functions: F*): Unit =
      functions.foreach(registerFunction(_))

    def registerFunction(function: F): Unit = {
      val scalaFunction = PureFunction.asScala(function.asInstanceOf[swaydb.java.PureFunction[K, V, Return.Map[V]]])
      functions.register(scalaFunction)
    }

    def removeFunction(function: F): Unit = {
      val scalaFunction = function.asInstanceOf[swaydb.java.PureFunction[K, V, Return.Map[V]]].id.asInstanceOf[Slice[Byte]]
      functions.core.remove(scalaFunction)
    }

    def init(): swaydb.java.Map[K, V, F] = {
      val scalaMap =
        swaydb.memory.Map[K, V, swaydb.PureFunction[K, V, Apply.Map[V]], Bag.Less](
          mapSize = mapSize,
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegmentGroup,
          deleteSegmentsEventually = deleteSegmentsEventually,
          acceleration = acceleration.asScala,
          levelZeroThrottle = levelZeroThrottle.asScala,
          lastLevelThrottle = lastLevelThrottle.asScala
        )(keySerializer = keySerializer,
          valueSerializer = valueSerializer,
          functions = functions.asInstanceOf[swaydb.Map.Functions[K, V, swaydb.PureFunction[K, V, Apply.Map[V]]]],
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[swaydb.PureFunction[K, V, Apply.Map[V]]]],
          bag = Bag.less,
          keyOrder = Left(scalaKeyOrder)
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