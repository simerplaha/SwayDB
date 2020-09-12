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

import java.time.Duration
import java.util.concurrent.ExecutorService

import swaydb.Bag
import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.util.Eithers
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{FileCache, ThreadStateCache}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.Java.JavaFunction
import swaydb.data.util.StorageUnits._
import swaydb.java._
import swaydb.java.data.slice.{Slice => JavaSlice}
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.memory.DefaultConfigs
import swaydb.serializers.Serializer

import scala.compat.java8.DurationConverters._
import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

object MemorySetMap {

  final class Config[K, V](private var mapSize: Int = 4.mb,
                           private var minSegmentSize: Int = 2.mb,
                           private var maxKeyValuesPerSegment: Int = Int.MaxValue,
                           private var deleteSegmentsEventually: Boolean = true,
                           private var shutdownTimeout: Duration = 30.seconds.toJava,
                           private var fileCache: FileCache.Enable = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                           private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                           private var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
                           private var lastLevelThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.lastLevelThrottle _).asJava,
                           private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                           private var byteComparator: KeyComparator[JavaSlice[java.lang.Byte]] = null,
                           private var typedComparator: KeyComparator[K] = null,
                           private var compactionEC: Option[ExecutionContext] = None,
                           keySerializer: Serializer[K],
                           valueSerializer: Serializer[V]) {

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

    def setShutdownTimeout(duration: Duration) = {
      this.shutdownTimeout = duration
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

    def setByteKeyComparator(byteComparator: KeyComparator[JavaSlice[java.lang.Byte]]) = {
      this.byteComparator = byteComparator
      this
    }

    def setTypedKeyComparator(typedComparator: KeyComparator[K]) = {
      this.typedComparator = typedComparator
      this
    }

    def setCompactionExecutionContext(executionContext: ExecutorService) = {
      this.compactionEC = Some(ExecutionContext.fromExecutorService(executionContext))
      this
    }

    def get(): swaydb.java.SetMap[K, V] = {
      val comparator: Either[KeyComparator[JavaSlice[java.lang.Byte]], KeyComparator[K]] =
        Eithers.nullCheck(
          left = byteComparator,
          right = typedComparator,
          default = KeyComparator.lexicographic
        )

      val scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, keySerializer)

      val scalaMap =
        swaydb.memory.SetMap[K, V, Bag.Less](
          mapSize = mapSize,
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          fileCache = fileCache,
          deleteSegmentsEventually = deleteSegmentsEventually,
          shutdownTimeout = shutdownTimeout.toScala,
          acceleration = acceleration.asScala,
          levelZeroThrottle = levelZeroThrottle.asScala,
          lastLevelThrottle = lastLevelThrottle.asScala,
          threadStateCache = threadStateCache
        )(keySerializer = keySerializer,
          valueSerializer = valueSerializer,
          bag = Bag.less,
          byteKeyOrder = scalaKeyOrder,
          compactionEC = compactionEC.getOrElse(DefaultExecutionContext.compactionEC)
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
