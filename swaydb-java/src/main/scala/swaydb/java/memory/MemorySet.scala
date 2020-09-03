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

import swaydb.core.util.Eithers
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config.{FileCache, ThreadStateCache}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.Java.JavaFunction
import swaydb.data.util.StorageUnits._
import swaydb.java._
import swaydb.java.data.slice.ByteSlice
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.memory.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.{Apply, Bag}

import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration._
import scala.reflect.ClassTag

object MemorySet {

  final class Config[A, F](private var mapSize: Int = 4.mb,
                           private var minSegmentSize: Int = 2.mb,
                           private var maxKeyValuesPerSegment: Int = Int.MaxValue,
                           private var deleteSegmentsEventually: Boolean = true,
                           private var fileCache: FileCache.Enable = DefaultConfigs.fileCache(),
                           private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                           private var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
                           private var lastLevelThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.lastLevelThrottle _).asJava,
                           private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                           private var byteComparator: KeyComparator[ByteSlice] = null,
                           private var typedComparator: KeyComparator[A] = null,
                           serializer: Serializer[A],
                           functionClassTag: ClassTag[_]) {

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

    def setByteComparator(byteComparator: KeyComparator[ByteSlice]) = {
      this.byteComparator = byteComparator
      this
    }

    def setTypedComparator(typedComparator: KeyComparator[A]) = {
      this.typedComparator = typedComparator
      this
    }

    private val functions = swaydb.Set.Functions[A, swaydb.PureFunction.OnKey[A, Nothing, Apply.Set]]()(serializer)

    def registerFunctions(functions: F*): Config[A, F] = {
      functions.foreach(registerFunction(_))
      this
    }

    def registerFunction(function: F): Config[A, F] = {
      val scalaFunction = PureFunction.asScala(function.asInstanceOf[swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]])
      functions.register(scalaFunction)
      this
    }

    def removeFunction(function: F): Config[A, F] = {
      val scalaFunction = function.asInstanceOf[swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]].id.asInstanceOf[Slice[Byte]]
      functions.core.remove(scalaFunction)
      this
    }

    def get(): swaydb.java.Set[A, F] = {
      val comparator: Either[KeyComparator[ByteSlice], KeyComparator[A]] =
        Eithers.nullCheck(
          left = byteComparator,
          right = typedComparator,
          default = KeyComparator.lexicographic
        )

      val scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, serializer)

      val scalaMap =
        swaydb.memory.Set[A, swaydb.PureFunction.OnKey[A, Void, Apply.Set], Bag.Less](
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
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[swaydb.PureFunction.OnKey[A, Void, Apply.Set]]],
          bag = Bag.less,
          functions = functions.asInstanceOf[swaydb.Set.Functions[A, swaydb.PureFunction.OnKey[A, Void, Apply.Set]]],
          byteKeyOrder = scalaKeyOrder
        )

      swaydb.java.Set[A, F](scalaMap)
    }
  }

  def functionsOn[A](serializer: JavaSerializer[A]): Config[A, swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]]] =
    new Config(
      serializer = SerializerConverter.toScala(serializer),
      functionClassTag = ClassTag(classOf[swaydb.PureFunction.OnKey[A, Void, Apply.Set]])
    )

  def functionsOff[A](serializer: JavaSerializer[A]): Config[A, Void] =
    new Config[A, Void](
      serializer = SerializerConverter.toScala(serializer),
      functionClassTag = ClassTag.Nothing
    )
}
