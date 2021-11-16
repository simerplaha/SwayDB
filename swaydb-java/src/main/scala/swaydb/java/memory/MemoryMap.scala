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

import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.util.Eithers
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config.{FileCache, ThreadStateCache}
import swaydb.data.order.KeyOrder
import swaydb.slice.Slice
import swaydb.data.{Atomic, Functions, OptimiseWrites}
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.java.{KeyComparator, KeyOrderConverter}
import swaydb.memory.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.utils.Java.JavaFunction
import swaydb.{Apply, Bag, CommonConfigs, Glass, PureFunction}

import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration._
import scala.reflect.ClassTag

object MemoryMap {

  final class Config[K, V, F](private var logSize: Int = DefaultConfigs.logSize,
                              private var minSegmentSize: Int = DefaultConfigs.segmentSize,
                              private var maxKeyValuesPerSegment: Int = Int.MaxValue,
                              private var deleteDelay: FiniteDuration = CommonConfigs.segmentDeleteDelay,
                              private var optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                              private var atomic: Atomic = CommonConfigs.atomic(),
                              private var compactionConfig: Option[CompactionConfig] = None,
                              private var fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                              private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                              private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = DefaultConfigs.accelerator.asJava,
                              private var levelZeroThrottle: JavaFunction[LevelZeroMeter, LevelZeroThrottle] = (DefaultConfigs.levelZeroThrottle _).asJava,
                              private var lastLevelThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.lastLevelThrottle _).asJava,
                              private var byteComparator: KeyComparator[Slice[java.lang.Byte]] = null,
                              private var typedComparator: KeyComparator[K] = null)(implicit functionClassTag: ClassTag[F],
                                                                                    keySerializer: Serializer[K],
                                                                                    valueSerializer: Serializer[V],
                                                                                    functions: Functions[F],
                                                                                    evd: F <:< PureFunction[K, V, Apply.Map[V]]) {

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

    def setThreadStateCache(threadStateCache: ThreadStateCache) = {
      this.threadStateCache = threadStateCache
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

    def setByteKeyComparator(byteComparator: KeyComparator[Slice[java.lang.Byte]]) = {
      this.byteComparator = byteComparator
      this
    }

    def setTypedKeyComparator(typedComparator: KeyComparator[K]) = {
      this.typedComparator = typedComparator
      this
    }

    def get(): swaydb.java.Map[K, V, F] = {
      val comparator: Either[KeyComparator[Slice[java.lang.Byte]], KeyComparator[K]] =
        Eithers.nullCheck(
          left = byteComparator,
          right = typedComparator,
          default = KeyComparator.lexicographic
        )

      val scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, keySerializer)

      val scalaMap =
        swaydb.memory.Map[K, V, PureFunction.Map[K, V], Glass](
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
          functions = functions.asInstanceOf[Functions[PureFunction.Map[K, V]]],
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[PureFunction.Map[K, V]]],
          bag = Bag.glass,
          byteKeyOrder = scalaKeyOrder
        )

      swaydb.java.Map[K, V, F](scalaMap.asInstanceOf[swaydb.Map[K, V, F, Glass]])

    }
  }

  def functionsOn[K, V](keySerializer: JavaSerializer[K],
                        valueSerializer: JavaSerializer[V],
                        functions: java.lang.Iterable[PureFunction[K, V, Apply.Map[V]]]): Config[K, V, PureFunction[K, V, Apply.Map[V]]] = {
    implicit val pureFunctions = Functions(functions)
    implicit val scalaKeySerializer: Serializer[K] = SerializerConverter.toScala(keySerializer)
    implicit val scalaValueSerializer: Serializer[V] = SerializerConverter.toScala(valueSerializer)

    new Config()
  }

  def functionsOff[K, V](keySerializer: JavaSerializer[K],
                         valueSerializer: JavaSerializer[V]): Config[K, V, Void] = {
    implicit val scalaKeySerializer: Serializer[K] = SerializerConverter.toScala(keySerializer)
    implicit val scalaValueSerializer: Serializer[V] = SerializerConverter.toScala(valueSerializer)
    implicit val evidence: Void <:< PureFunction[K, V, Apply.Map[V]] = null

    new Config[K, V, Void]()
  }
}
