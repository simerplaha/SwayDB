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
 * If you modify this Program or any covered work, only by linking or combining
 * it with separate works, the licensors of this Program grant you additional
 * permission to convey the resulting work.
 */

package swaydb.java.memory

import java.util.concurrent.ExecutorService

import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.util.Eithers
import swaydb.data.{Atomic, Functions, OptimiseWrites}
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{FileCache, ThreadStateCache}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.Java.JavaFunction
import swaydb.data.util.StorageUnits._
import swaydb.java._
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.memory.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.{Apply, Bag, PureFunction}

import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

object MemorySet {

  final class Config[A, F](private var mapSize: Int = 4.mb,
                           private var minSegmentSize: Int = 2.mb,
                           private var maxKeyValuesPerSegment: Int = Int.MaxValue,
                           private var deleteSegmentsEventually: Boolean = true,
                           private var optimiseWrites: OptimiseWrites = DefaultConfigs.optimiseWrites(),
                           private var atomic: Atomic = DefaultConfigs.atomic(),
                           private var fileCache: FileCache.Enable = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                           private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                           private var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
                           private var lastLevelThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.lastLevelThrottle _).asJava,
                           private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                           private var byteComparator: KeyComparator[Slice[java.lang.Byte]] = null,
                           private var typedComparator: KeyComparator[A] = null,
                           private var compactionEC: Option[ExecutionContext] = None)(implicit functionClassTag: ClassTag[F],
                                                                                      serializer: Serializer[A],
                                                                                      functions: Functions[F],
                                                                                      evd: F <:< PureFunction[A, Nothing, Apply.Set[Nothing]]) {

    def setMapSize(mapSize: Int) = {
      this.mapSize = mapSize
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

    def setByteKeyComparator(byteComparator: KeyComparator[Slice[java.lang.Byte]]) = {
      this.byteComparator = byteComparator
      this
    }

    def setTypedKeyComparator(typedComparator: KeyComparator[A]) = {
      this.typedComparator = typedComparator
      this
    }

    def setCompactionExecutionContext(executionContext: ExecutorService) = {
      this.compactionEC = Some(ExecutionContext.fromExecutorService(executionContext))
      this
    }

    def get(): swaydb.java.Set[A, F] = {
      val comparator: Either[KeyComparator[Slice[java.lang.Byte]], KeyComparator[A]] =
        Eithers.nullCheck(
          left = byteComparator,
          right = typedComparator,
          default = KeyComparator.lexicographic
        )

      val scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, serializer)

      val scalaMap =
        swaydb.memory.Set[A, PureFunction.Set[A], Bag.Less](
          mapSize = mapSize,
          minSegmentSize = minSegmentSize,
          maxKeyValuesPerSegment = maxKeyValuesPerSegment,
          fileCache = fileCache,
          deleteSegmentsEventually = deleteSegmentsEventually,
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          acceleration = acceleration.asScala,
          levelZeroThrottle = levelZeroThrottle.asScala,
          lastLevelThrottle = lastLevelThrottle.asScala,
          threadStateCache = threadStateCache
        )(serializer = serializer,
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[PureFunction.Set[A]]],
          bag = Bag.less,
          functions = functions.asInstanceOf[Functions[PureFunction.Set[A]]],
          byteKeyOrder = scalaKeyOrder,
          compactionEC = compactionEC.getOrElse(DefaultExecutionContext.compactionEC)
        )

      swaydb.java.Set[A, F](scalaMap.asInstanceOf[swaydb.Set[A, F, Bag.Less]])
    }
  }

  def functionsOn[A](serializer: JavaSerializer[A],
                     functions: java.lang.Iterable[PureFunction[A, Void, Apply.Set[Void]]]): Config[A, PureFunction[A, Void, Apply.Set[Void]]] = {

    implicit val scalaFunctions = functions.castToNothingFunctions
    implicit val scalaSerializer: Serializer[A] = SerializerConverter.toScala(serializer)
    val config: Config[A, PureFunction[A, Nothing, Apply.Set[Nothing]]] = new Config()

    config.asInstanceOf[Config[A, PureFunction[A, Void, Apply.Set[Void]]]]
  }

  def functionsOff[A](serializer: JavaSerializer[A]): Config[A, Void] = {
    implicit val scalaSerializer: Serializer[A] = SerializerConverter.toScala(serializer)
    implicit val evd: Void <:< PureFunction[A, Nothing, Apply.Set[Nothing]] = null

    new Config()
  }
}
