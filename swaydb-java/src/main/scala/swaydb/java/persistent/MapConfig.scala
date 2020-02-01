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
import java.util.Comparator
import java.util.concurrent.ExecutorService

import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.java.data.slice.ByteSlice
import swaydb.java.data.util.Java.JavaFunction
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.java.{IO, KeyOrderConverter, Return}
import swaydb.persistent.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.{Apply, Bag, SwayDB}

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration.FiniteDuration
import scala.reflect.ClassTag

object MapConfig {

  class Config[K, V, F <: swaydb.java.PureFunction[K, V, Return.Map[V]], SF](@BeanProperty var dir: Path,
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
                                                                             @BeanProperty var comparator: IO[Comparator[ByteSlice], Comparator[K]] = IO.leftNeverException[Comparator[ByteSlice], Comparator[K]](swaydb.java.SwayDB.defaultComparator),
                                                                             keySerializer: Serializer[K],
                                                                             valueSerializer: Serializer[V],
                                                                             functionClassTag: ClassTag[SF]) {

    implicit def scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, keySerializer)

    def init(): swaydb.java.Map[K, V, F] = {
      val scalaMap =
        swaydb.persistent.Map[K, V, SF, Bag.Less](
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
        )(keySerializer = keySerializer,
          valueSerializer = valueSerializer,
          functionClassTag = functionClassTag,
          bag = Bag.less,
          keyOrder = Left(scalaKeyOrder)
        ).get

      swaydb.java.Map[K, V, F](scalaMap)
    }
  }

  def withFunctions[K, V, F](dir: Path,
                             keySerializer: JavaSerializer[K],
                             valueSerializer: JavaSerializer[V]): Config[K, V, swaydb.java.PureFunction[K, V, Return.Map[V]], swaydb.PureFunction[K, V, Apply.Map[V]]] =
    new Config(
      dir = dir,
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Any.asInstanceOf[ClassTag[swaydb.PureFunction[K, V, Apply.Map[V]]]]
    )

  def withoutFunctions[K, V](dir: Path,
                             keySerializer: JavaSerializer[K],
                             valueSerializer: JavaSerializer[V]): Config[K, V, swaydb.java.PureFunction.VoidM[K, V], Void] =
    new Config[K, V, swaydb.java.PureFunction.VoidM[K, V], Void](
      dir = dir,
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Nothing.asInstanceOf[ClassTag[Void]]
    )

}
