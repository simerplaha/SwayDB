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
import swaydb.java._
import swaydb.memory.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.{Apply, Bag}

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration._
import scala.reflect.ClassTag

object SetConfig {

  class Config[A, F](@BeanProperty var mapSize: Int = 4.mb,
                     @BeanProperty var minSegmentSize: Int = 2.mb,
                     @BeanProperty var maxKeyValuesPerSegmentGroup: Int = 10,
                     @BooleanBeanProperty var deleteSegmentsEventually: Boolean = true,
                     @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                     @BeanProperty var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
                     @BeanProperty var lastLevelThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.lastLevelThrottle _).asJava,
                     @BeanProperty var comparator: IO[KeyComparator[ByteSlice], KeyComparator[A]] = IO.leftNeverException[KeyComparator[ByteSlice], KeyComparator[A]](swaydb.java.SwayDB.defaultComparator),
                     serializer: Serializer[A],
                     functionClassTag: ClassTag[_]) {

    private val functions = swaydb.Set.Functions[A, swaydb.PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]]()(serializer)

    def registerFunctions(functions: F*): Config[A, F] = {
      functions.foreach(registerFunction(_))
      this
    }

    def registerFunction(function: F): Config[A, F] = {
      val scalaFunction = PureFunction.asScala(function.asInstanceOf[swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]])
      functions.register(scalaFunction)
      this
    }

    def removeFunction(function: F): Config[A, F] = {
      val scalaFunction = function.asInstanceOf[swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]].id.asInstanceOf[Slice[Byte]]
      functions.core.remove(scalaFunction)
      this
    }

    implicit def scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, serializer)

    def init(): swaydb.java.Set[A, F] = {
      val scalaMap =
        swaydb.memory.Set[A, swaydb.PureFunction.OnKey[A, Void, Apply.Set[Void]], Bag.Less](
          mapSize = mapSize,
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegmentGroup,
          deleteSegmentsEventually = deleteSegmentsEventually,
          acceleration = acceleration.asScala,
          levelZeroThrottle = levelZeroThrottle.asScala,
          lastLevelThrottle = lastLevelThrottle.asScala
        )(serializer = serializer,
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[swaydb.PureFunction.OnKey[A, Void, Apply.Set[Void]]]],
          bag = Bag.less,
          functions = functions.asInstanceOf[swaydb.Set.Functions[A, swaydb.PureFunction.OnKey[A, Void, Apply.Set[Void]]]],
          keyOrder = Left(scalaKeyOrder)
        ).get

      swaydb.java.Set[A, F](scalaMap)
    }
  }

  def withFunctions[A](serializer: JavaSerializer[A]): Config[A, swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]] =
    new Config(
      serializer = SerializerConverter.toScala(serializer),
      functionClassTag = ClassTag(classOf[swaydb.PureFunction.OnKey[A, Void, Apply.Set[Void]]])
    )

  def withoutFunctions[A](serializer: JavaSerializer[A]): Config[A, Void] =
    new Config[A, Void](
      serializer = SerializerConverter.toScala(serializer),
      functionClassTag = ClassTag.Nothing
    )

}
