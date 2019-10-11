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
import swaydb.java.IO
import swaydb.java.data.slice.ByteSlice
import swaydb.java.data.util.Java.{JavaFunction, _}
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.serializers.Serializer

import scala.beans.BeanProperty
import scala.compat.java8.FunctionConverters._
import scala.reflect.ClassTag

object Map {

  class Builder[K, V, F](@BeanProperty var mapSize: Int = 4.mb,
                         @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                         @BeanProperty var keyOrder: Comparator[ByteSlice] = swaydb.java.SwayDB.defaultComparator,
                         keySerializer: Serializer[K],
                         valueSerializer: Serializer[V],
                         functionClassTag: ClassTag[F]) {

    implicit val scalaKeyOrder = KeyOrder(keyOrder.asScala)

    def start(): IO[Throwable, swaydb.java.Map[K, V, F]] =
      IO.fromScala {
        swaydb.IO {
          val scalaMap =
            swaydb.memory.zero.Map[K, V, F, swaydb.IO.ThrowableIO](
              mapSize = mapSize,
              acceleration = acceleration.asScala
            )(keySerializer = keySerializer,
              valueSerializer = valueSerializer,
              functionClassTag = functionClassTag,
              tag = Tag.throwableIO,
              keyOrder = scalaKeyOrder.asInstanceOf[KeyOrder[Slice[Byte]]]
            ).get

          swaydb.java.Map[K, V, F](scalaMap)
        }
      }
  }

  def enableFunctions[K, V, F](keySerializer: JavaSerializer[K],
                               valueSerializer: JavaSerializer[V]): Builder[K, V, F] =
    new Builder(
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Any.asInstanceOf[ClassTag[F]]
    )

  def disableFunctions[K, V](keySerializer: JavaSerializer[K],
                             valueSerializer: JavaSerializer[V]): Builder[K, V, Functions.Disabled] =
    new Builder(
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Nothing.asInstanceOf[ClassTag[Functions.Disabled]]
    )
}
