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
 *
 * Additional permission under the GNU Affero GPL version 3 section 7:
 * If you modify this Program or any covered work, only by linking or
 * combining it with separate works, the licensors of this Program grant
 * you additional permission to convey the resulting work.
 */

package swaydb.java.memory

import java.util.concurrent.ExecutorService

import swaydb.Bag
import swaydb.configs.level.DefaultExecutionContext
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{FileCache, ThreadStateCache}
import swaydb.data.util.Java.JavaFunction
import swaydb.data.util.StorageUnits._
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.memory.DefaultConfigs
import swaydb.serializers.Serializer

import scala.compat.java8.FunctionConverters._
import scala.concurrent.{ExecutionContext, ExecutionContextExecutorService}
import scala.concurrent.duration._

object MemoryQueue {

  final class Config[A](private var mapSize: Int = 4.mb,
                        private var minSegmentSize: Int = 2.mb,
                        private var maxKeyValuesPerSegment: Int = Int.MaxValue,
                        private var deleteSegmentsEventually: Boolean = true,
                        private var fileCache: FileCache.Enable = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                        private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                        private var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
                        private var lastLevelThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.lastLevelThrottle _).asJava,
                        private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                        private var compactionEC: Option[ExecutionContextExecutorService] = None,
                        serializer: Serializer[A]) {

    def setMapSize(mapSize: Int) = {
      this.mapSize = mapSize
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

    def setDeleteSegmentsEventually(deleteSegmentsEventually: Boolean) = {
      this.deleteSegmentsEventually = deleteSegmentsEventually
      this
    }

    def setFileCache(fileCache: FileCache.Enable) = {
      this.fileCache = fileCache
      this
    }

    def setAcceleration(acceleration: JavaFunction[LevelZeroMeter, Accelerator]) = {
      this.acceleration = acceleration
      this
    }

    def setLevelZeroThrottle(levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration]) = {
      this.levelZeroThrottle = levelZeroThrottle
      this
    }

    def setLastLevelThrottle(lastLevelThrottle: JavaFunction[LevelMeter, Throttle]) = {
      this.lastLevelThrottle = lastLevelThrottle
      this
    }

    def setThreadStateCache(threadStateCache: ThreadStateCache) = {
      this.threadStateCache = threadStateCache
      this
    }

    def setCompactionExecutionContext(executionContext: ExecutorService) = {
      this.compactionEC = Some(ExecutionContext.fromExecutorService(executionContext))
      this
    }

    def get(): swaydb.java.Queue[A] = {
      val scalaQueue =
        swaydb.memory.Queue[A, Bag.Less](
          mapSize = mapSize,
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          fileCache = fileCache,
          deleteSegmentsEventually = deleteSegmentsEventually,
          acceleration = acceleration.asScala,
          levelZeroThrottle = levelZeroThrottle.asScala,
          lastLevelThrottle = lastLevelThrottle.asScala,
          threadStateCache = threadStateCache
        )(serializer = serializer,
          bag = Bag.less,
          compactionEC = compactionEC.getOrElse(DefaultExecutionContext.compactionEC))

      swaydb.java.Queue[A](scalaQueue)
    }
  }

  def config[A](serializer: JavaSerializer[A]): Config[A] =
    new Config(serializer = SerializerConverter.toScala(serializer))
}
