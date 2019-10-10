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
import java.util.concurrent.ExecutorService

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.Functions
import swaydb.data.util.StorageUnits._
import swaydb.java.IO
import swaydb.java.data.util.Javaz.{JavaFunction, _}
import swaydb.serializers.Serializer
import swaydb.{SwayDB, Tag}

import scala.beans.BeanProperty
import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration.{FiniteDuration, _}
import scala.reflect.ClassTag

object Map {

  class Builder[K, V, F](@BeanProperty var mapSize: Int = 4.mb,
                         @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                         @BeanProperty var keyOrder: Comparator[Slice[Byte]] = KeyOrder.defaultComparator,
                         @BeanProperty implicit var keySerializer: Serializer[K],
                         @BeanProperty implicit var valueSerializer: Serializer[V],
                         final implicit val functionClassTag: ClassTag[F]) {

    implicit val scalaKeyOrder = KeyOrder(keyOrder.asScala)

    def start(): IO[Throwable, swaydb.java.Map[K, V, F]] =
      IO {
        swaydb.IO {
          val scalaMap =
            swaydb.memory.zero.Map[K, V, F, swaydb.IO.ThrowableIO](
              mapSize = mapSize,
              acceleration = acceleration.asScala,
            )(keySerializer = keySerializer,
              valueSerializer = valueSerializer,
              functionClassTag = functionClassTag,
              tag = Tag.throwableIO,
              keyOrder = scalaKeyOrder
            ).get

          swaydb.java.Map[K, V, F](scalaMap)
        }
      }
  }

  def enableFunctions[K, V, F](keySerializer: Serializer[K],
                               valueSerializer: Serializer[V]): Builder[K, V, F] =
    new Builder(
      keySerializer = keySerializer,
      valueSerializer = valueSerializer,
      functionClassTag = ClassTag.Any.asInstanceOf[ClassTag[F]]
    )

  def disableFunctions[K, V](keySerializer: Serializer[K],
                             valueSerializer: Serializer[V]): Builder[K, V, Functions.Disabled] =
    new Builder(
      keySerializer = keySerializer,
      valueSerializer = valueSerializer,
      functionClassTag = ClassTag.Nothing.asInstanceOf[ClassTag[Functions.Disabled]]
    )
}
