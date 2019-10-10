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

package swaydb.java.memory

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

object Set {

  class Builder[A, F](@BeanProperty var mapSize: Int = 4.mb,
                      @BeanProperty var segmentSize: Int = 2.mb,
                      @BeanProperty var maxOpenSegments: Int = 100,
                      @BeanProperty var maxCachedKeyValuesPerSegment: Int = 10,
                      @BeanProperty var fileSweeperPollInterval: FiniteDuration = 10.seconds,
                      @BeanProperty var mightContainFalsePositiveRate: Double = 0.01,
                      @BeanProperty var deleteSegmentsEventually: Boolean = true,
                      @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                      @BeanProperty var keyOrder: Comparator[Slice[Byte]] = KeyOrder.defaultComparator,
                      @BeanProperty var fileSweeperExecutorService: ExecutorService = SwayDB.defaultExecutorService,
                      @BeanProperty implicit var serializer: Serializer[A],
                      final implicit val functionClassTag: ClassTag[F]) {

    implicit val scalaKeyOrder = KeyOrder(keyOrder.asScala)
    implicit val fileSweeperEC = fileSweeperExecutorService.asScala

    def start(): IO[Throwable, swaydb.java.Set[A, F]] =
      IO {
        swaydb.IO {
          val scalaMap =
            swaydb.memory.Set[A, F, swaydb.IO.ThrowableIO](
              mapSize = mapSize,
              segmentSize = segmentSize,
              maxOpenSegments = maxOpenSegments,
              maxCachedKeyValuesPerSegment = maxCachedKeyValuesPerSegment,
              fileSweeperPollInterval = fileSweeperPollInterval,
              mightContainFalsePositiveRate = mightContainFalsePositiveRate,
              deleteSegmentsEventually = deleteSegmentsEventually,
              acceleration = acceleration.asScala,
            )(serializer = serializer,
              functionClassTag = functionClassTag,
              tag = Tag.throwableIO,
              keyOrder = scalaKeyOrder,
              fileSweeperEC = fileSweeperEC
            ).get

          swaydb.java.Set[A, F](scalaMap)
        }
      }
  }

  def enableFunctions[A, F](implicit keySerializer: Serializer[A]): Builder[A, F] =
    new Builder(
      serializer = keySerializer,
      functionClassTag = ClassTag.Any.asInstanceOf[ClassTag[F]]
    )

  def disableFunctions[A](implicit serializer: Serializer[A]): Builder[A, Functions.Disabled] =
    new Builder(
      serializer = serializer,
      functionClassTag = ClassTag.Nothing.asInstanceOf[ClassTag[Functions.Disabled]]
    )

}
