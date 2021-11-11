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
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config.{FileCache, ThreadStateCache}
import swaydb.data.{Atomic, OptimiseWrites}
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.memory.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.utils.Java.JavaFunction
import swaydb.{Bag, CommonConfigs, Glass}

import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration._

object MemoryQueue {

  final class Config[A](private var logSize: Int = DefaultConfigs.logSize,
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
                        serializer: Serializer[A]) {

    def setLogSize(logSize: Int) = {
      this.logSize = logSize
      this
    }

    def setCompactionConfig(config: CompactionConfig) = {
      this.compactionConfig = Some(config)
      this
    }

    def setMinSegmentSize(minSegmentSize: Int) = {
      this.minSegmentSize = minSegmentSize
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

    def get(): swaydb.java.Queue[A] = {
      val scalaQueue =
        swaydb.memory.Queue[A, Glass](
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
        )(serializer = serializer, bag = Bag.glass)

      swaydb.java.Queue[A](scalaQueue)
    }
  }

  def config[A](serializer: JavaSerializer[A]): Config[A] =
    new Config(serializer = SerializerConverter.toScala(serializer))
}
