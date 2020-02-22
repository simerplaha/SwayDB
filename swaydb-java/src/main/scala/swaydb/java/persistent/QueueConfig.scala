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

package swaydb.java.persistent

import java.nio.file.Path

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.java._
import swaydb.java.data.slice.ByteSlice
import swaydb.java.data.util.Java.JavaFunction
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.persistent.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.{Apply, Bag}

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object QueueConfig {

  class Config[A](@BeanProperty var dir: Path,
                  @BeanProperty var mapSize: Int = 4.mb,
                  @BooleanBeanProperty var mmapMaps: Boolean = true,
                  @BeanProperty var recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                  @BooleanBeanProperty var mmapAppendix: Boolean = true,
                  @BeanProperty var appendixFlushCheckpointSize: Int = 2.mb,
                  @BeanProperty var otherDirs: Seq[Dir] = Seq.empty,
                  @BooleanBeanProperty var cacheKeyValueIds: Boolean = true,
                  @BeanProperty var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                  @BeanProperty var sortedKeyIndex: SortedKeyIndex.Enable = DefaultConfigs.sortedKeyIndex(),
                  @BeanProperty var randomKeyIndex: RandomKeyIndex.Enable = DefaultConfigs.randomKeyIndex(),
                  @BeanProperty var binarySearchIndex: BinarySearchIndex.FullIndex = DefaultConfigs.binarySearchIndex(),
                  @BeanProperty var mightContainKeyIndex: MightContainIndex.Enable = DefaultConfigs.mightContainKeyIndex(),
                  @BeanProperty var valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                  @BeanProperty var segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                  @BeanProperty var fileCache: FileCache.Enable = DefaultConfigs.fileCache(),
                  @BeanProperty var memoryCache: MemoryCache = DefaultConfigs.memoryCache(),
                  @BeanProperty var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
                  @BeanProperty var levelOneThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelOneThrottle _).asJava,
                  @BeanProperty var levelTwoThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelTwoThrottle _).asJava,
                  @BeanProperty var levelThreeThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelThreeThrottle _).asJava,
                  @BeanProperty var levelFourThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelFourThrottle _).asJava,
                  @BeanProperty var levelFiveThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelFiveThrottle _).asJava,
                  @BeanProperty var levelSixThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelSixThrottle _).asJava,
                  @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                  serializer: Serializer[A]) {

    def init(): swaydb.java.Queue[A] = {
      val scalaQueue =
        swaydb.persistent.Queue[A](
          dir = dir,
          mapSize = mapSize,
          mmapMaps = mmapMaps,
          recoveryMode = recoveryMode,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          otherDirs = otherDirs,
          cacheKeyValueIds = cacheKeyValueIds,
          acceleration = acceleration.asScala,
          levelZeroThrottle = levelZeroThrottle.asScala,
          levelOneThrottle = levelOneThrottle.asScala,
          levelTwoThrottle = levelTwoThrottle.asScala,
          levelThreeThrottle = levelThreeThrottle.asScala,
          levelFourThrottle = levelFourThrottle.asScala,
          levelFiveThrottle = levelFiveThrottle.asScala,
          levelSixThrottle = levelSixThrottle.asScala,
          threadStateCache = threadStateCache,
          sortedKeyIndex = sortedKeyIndex,
          randomKeyIndex = randomKeyIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainKeyIndex = mightContainKeyIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache
        )(serializer = serializer).get

      swaydb.java.Queue[A](scalaQueue)
    }
  }

  def withFunctions[A](dir: Path,
                       keySerializer: JavaSerializer[A]): Config[A] =
    new Config(
      dir = dir,
      serializer = SerializerConverter.toScala(keySerializer)
    )

  def withoutFunctions[A](dir: Path,
                          serializer: JavaSerializer[A]): Config[A] =
    new Config[A](
      dir = dir,
      serializer = SerializerConverter.toScala(serializer)
    )
}
