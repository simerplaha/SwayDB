/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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

package swaydb.java.memory.zero

import java.util.Comparator

import swaydb.Tag
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.Functions
import swaydb.data.util.StorageUnits._
import swaydb.java.data.slice.ByteSlice
import swaydb.java.data.util.Java.JavaFunction
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.java.{IO, KeyOrderConverter}
import swaydb.serializers.Serializer

import scala.beans.BeanProperty
import scala.compat.java8.FunctionConverters._
import scala.reflect.ClassTag

object Map {

  class Builder[K, V, F, SF](@BeanProperty var mapSize: Int = 4.mb,
                             @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                             @BeanProperty var comparator: IO[Comparator[ByteSlice], Comparator[K]] = IO.leftNeverException[Comparator[ByteSlice], Comparator[K]](swaydb.java.SwayDB.defaultComparator),
                             keySerializer: Serializer[K],
                             valueSerializer: Serializer[V],
                             functionClassTag: ClassTag[SF]) {

    implicit def scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, keySerializer)

    def init(): IO[Throwable, swaydb.java.MapIO[K, V, F]] =
      IO.fromScala {
        swaydb.IO {
          val scalaMap =
            swaydb.memory.zero.Map[K, V, SF, swaydb.IO.ThrowableIO](
              mapSize = mapSize,
              acceleration = acceleration.asScala
            )(keySerializer = keySerializer,
              valueSerializer = valueSerializer,
              functionClassTag = functionClassTag,
              tag = Tag.throwableIO,
              keyOrder = Left(scalaKeyOrder)
            ).get

          swaydb.java.MapIO[K, V, F](scalaMap)
        }
      }
  }

  def configWithFunctions[K, V](keySerializer: JavaSerializer[K],
                                valueSerializer: JavaSerializer[V]): Builder[K, V, swaydb.java.PureFunction.OnKey[K, V, swaydb.Apply.Map[V]], swaydb.PureFunction.OnKey[K, V, swaydb.Apply.Map[V]]] =
    new Builder(
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Any.asInstanceOf[ClassTag[swaydb.PureFunction.OnKey[K, V, swaydb.Apply.Map[V]]]]
    )

  def config[K, V](keySerializer: JavaSerializer[K],
                   valueSerializer: JavaSerializer[V]): Builder[K, V, Functions.Disabled, Functions.Disabled] =
    new Builder(
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Nothing.asInstanceOf[ClassTag[Functions.Disabled]]
    )
}
