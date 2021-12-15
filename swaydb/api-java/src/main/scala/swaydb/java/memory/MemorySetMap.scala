/*
 * Copyright 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package swaydb.java.memory

import swaydb.config.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.config.{Atomic, FileCache, OptimiseWrites, ThreadStateCache}
import swaydb.configs.level.DefaultExecutionContext
import swaydb.java._
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.memory.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.utils.Eithers
import swaydb.utils.Java.JavaFunction
import swaydb.{Bag, CommonConfigs, Glass}

import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration._

object MemorySetMap {

  final class Config[K, V](private var logSize: Int = DefaultConfigs.logSize,
                           private var minSegmentSize: Int = DefaultConfigs.segmentSize,
                           private var maxKeyValuesPerSegment: Int = Int.MaxValue,
                           private var deleteDelay: FiniteDuration = CommonConfigs.segmentDeleteDelay,
                           private var optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                           private var atomic: Atomic = CommonConfigs.atomic(),
                           private var compactionConfig: Option[CompactionConfig] = None,
                           private var fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                           private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = DefaultConfigs.accelerator.asJava,
                           private var levelZeroThrottle: JavaFunction[LevelZeroMeter, LevelZeroThrottle] = (DefaultConfigs.levelZeroThrottle _).asJava,
                           private var lastLevelThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.lastLevelThrottle _).asJava,
                           private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                           private var byteComparator: KeyComparator[Slice[java.lang.Byte]] = null,
                           private var typedComparator: KeyComparator[K] = null,
                           keySerializer: Serializer[K],
                           valueSerializer: Serializer[V]) {

    def setLogSize(logSize: Int) = {
      this.logSize = logSize
      this
    }

    def setCompactionConfig(config: CompactionConfig) = {
      this.compactionConfig = Some(config)
      this
    }

    def setOptimiseWrites(optimiseWrites: OptimiseWrites) = {
      this.optimiseWrites = optimiseWrites
      this
    }

    def setAtomic(atomic: Atomic) = {
      this.atomic = atomic
      this
    }

    def setMinSegmentSize(minSegmentSize: Int) = {
      this.minSegmentSize = minSegmentSize
      this
    }

    def setMaxKeyValuesPerSegment(maxKeyValuesPerSegment: Int) = {
      this.maxKeyValuesPerSegment = maxKeyValuesPerSegment
      this
    }

    def setDeleteDelay(deleteDelay: FiniteDuration) = {
      this.deleteDelay = deleteDelay
      this
    }

    def setFileCache(fileCache: FileCache.On) = {
      this.fileCache = fileCache
      this
    }

    def setAcceleration(acceleration: JavaFunction[LevelZeroMeter, Accelerator]) = {
      this.acceleration = acceleration
      this
    }

    def setLevelZeroThrottle(levelZeroThrottle: JavaFunction[LevelZeroMeter, LevelZeroThrottle]) = {
      this.levelZeroThrottle = levelZeroThrottle
      this
    }

    def setLastLevelThrottle(lastLevelThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.lastLevelThrottle = lastLevelThrottle
      this
    }

    def setThreadStateCache(threadStateCache: ThreadStateCache) = {
      this.threadStateCache = threadStateCache
      this
    }

    def setByteKeyComparator(byteComparator: KeyComparator[Slice[java.lang.Byte]]) = {
      this.byteComparator = byteComparator
      this
    }

    def setTypedKeyComparator(typedComparator: KeyComparator[K]) = {
      this.typedComparator = typedComparator
      this
    }

    def get(): swaydb.java.SetMap[K, V] = {
      val comparator: Either[KeyComparator[Slice[java.lang.Byte]], KeyComparator[K]] =
        Eithers.nullCheck(
          left = byteComparator,
          right = typedComparator,
          default = KeyComparator.lexicographic
        )

      val scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, keySerializer)

      val scalaMap =
        swaydb.memory.SetMap[K, V, Glass](
          logSize = logSize,
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          fileCache = fileCache,
          deleteDelay = deleteDelay,
          compactionConfig = compactionConfig getOrElse CommonConfigs.compactionConfig(),
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          acceleration = acceleration.asScala,
          levelZeroThrottle = levelZeroThrottle.asScala,
          lastLevelThrottle = lastLevelThrottle.asScala,
          threadStateCache = threadStateCache
        )(keySerializer = keySerializer,
          valueSerializer = valueSerializer,
          bag = Bag.glass,
          byteKeyOrder = scalaKeyOrder
        )

      swaydb.java.SetMap[K, V](scalaMap)
    }
  }

  def config[K, V](keySerializer: JavaSerializer[K],
                   valueSerializer: JavaSerializer[V]): Config[K, V] =
    new Config[K, V](
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer)
    )
}
