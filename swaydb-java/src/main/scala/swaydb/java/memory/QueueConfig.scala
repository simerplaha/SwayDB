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
import swaydb.data.config.{FileCache, ThreadStateCache}
import swaydb.data.util.StorageUnits._
import swaydb.data.util.Java.JavaFunction
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.memory.DefaultConfigs
import swaydb.serializers.Serializer

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration._

object QueueConfig {

  class Config[A](@BeanProperty var mapSize: Int = 4.mb,
                  @BeanProperty var minSegmentSize: Int = 2.mb,
                  @BeanProperty var maxKeyValuesPerSegment: Int = Int.MaxValue,
                  @BooleanBeanProperty var deleteSegmentsEventually: Boolean = true,
                  @BeanProperty var fileCache: FileCache.Enable = DefaultConfigs.fileCache(),
                  @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                  @BeanProperty var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
                  @BeanProperty var lastLevelThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.lastLevelThrottle _).asJava,
                  @BeanProperty var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                  serializer: Serializer[A]) {

    def init(): swaydb.java.Queue[A] = {
      val scalaQueue =
        swaydb.memory.Queue[A](
          mapSize = mapSize,
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          fileCache = fileCache,
          deleteSegmentsEventually = deleteSegmentsEventually,
          acceleration = acceleration.asScala,
          levelZeroThrottle = levelZeroThrottle.asScala,
          lastLevelThrottle = lastLevelThrottle.asScala,
          threadStateCache = threadStateCache
        )(serializer = serializer).get

      swaydb.java.Queue[A](scalaQueue)
    }
  }

  def configure[A](serializer: JavaSerializer[A]): Config[A] =
    new Config(serializer = SerializerConverter.toScala(serializer))
}
