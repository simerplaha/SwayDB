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
import swaydb.data.config.{Dir, MMAP, RecoveryMode}
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.StorageUnits._
import swaydb.java.data.slice.ByteSlice
import swaydb.java.data.util.Java.{JavaFunction, _}
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.java.{IO, KeyOrderConverter, PureFunction, Return}
import swaydb.serializers.Serializer
import swaydb.{Apply, SwayDB, Bag}

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{FiniteDuration, _}
import scala.reflect.ClassTag

object SetConfig {

  class Config[A, F <: swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]], SF](@BeanProperty var dir: Path,
                                                                                      @BeanProperty var maxOpenSegments: Int = 1000,
                                                                                      @BeanProperty var memoryCacheSize: Int = 100.mb,
                                                                                      @BeanProperty var mapSize: Int = 4.mb,
                                                                                      @BooleanBeanProperty var mmapMaps: Boolean = true,
                                                                                      @BeanProperty var recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                                                                                      @BooleanBeanProperty var mmapAppendix: Boolean = true,
                                                                                      @BeanProperty var mmapSegments: MMAP = MMAP.WriteAndRead,
                                                                                      @BeanProperty var minSegmentSize: Int = 2.mb,
                                                                                      @BeanProperty var appendixFlushCheckpointSize: Int = 2.mb,
                                                                                      @BeanProperty var otherDirs: Seq[Dir] = Seq.empty,
                                                                                      @BeanProperty var memorySweeperPollInterval: FiniteDuration = 10.seconds,
                                                                                      @BeanProperty var fileSweeperPollInterval: FiniteDuration = 10.seconds,
                                                                                      @BeanProperty var mightContainFalsePositiveRate: Double = 0.01,
                                                                                      @BeanProperty var blockSize: Int = 4098,
                                                                                      @BooleanBeanProperty var compressDuplicateValues: Boolean = true,
                                                                                      @BooleanBeanProperty var deleteSegmentsEventually: Boolean = true,
                                                                                      @BooleanBeanProperty var cacheKeyValueIds: Boolean = true,
                                                                                      @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                                                                                      @BeanProperty var comparator: IO[Comparator[ByteSlice], Comparator[A]] = IO.leftNeverException[Comparator[ByteSlice], Comparator[A]](swaydb.java.SwayDB.defaultComparator),
                                                                                      @BeanProperty var fileSweeperExecutorService: ExecutorService = SwayDB.sweeperExecutionContext.threadPool,
                                                                                      serializer: Serializer[A],
                                                                                      functionClassTag: ClassTag[SF]) {

    implicit def scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, serializer)

    implicit def fileSweeperEC: ExecutionContext = fileSweeperExecutorService.asScala

    def init(): swaydb.java.Set[A, F] = {
      val scalaMap =
        swaydb.persistent.Set[A, SF, Bag.Less](
          dir = dir,
          maxOpenSegments = maxOpenSegments,
          memoryCacheSize = memoryCacheSize,
          mapSize = mapSize,
          mmapMaps = mmapMaps,
          recoveryMode = recoveryMode,
          mmapAppendix = mmapAppendix,
          mmapSegments = mmapSegments,
          minSegmentSize = minSegmentSize,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          otherDirs = otherDirs,
          memorySweeperPollInterval = memorySweeperPollInterval,
          fileSweeperPollInterval = fileSweeperPollInterval,
          mightContainFalsePositiveRate = mightContainFalsePositiveRate,
          blockSize = blockSize,
          compressDuplicateValues = compressDuplicateValues,
          deleteSegmentsEventually = deleteSegmentsEventually,
          cacheKeyValueIds = cacheKeyValueIds,
          acceleration = acceleration.asScala
        )(serializer = serializer,
          functionClassTag = functionClassTag,
          bag = Bag.less,
          keyOrder = Left(scalaKeyOrder),
          fileSweeperEC = fileSweeperEC
        ).get

      swaydb.java.Set[A, F](scalaMap)
    }
  }

  def withFunctions[A](dir: Path,
                       keySerializer: JavaSerializer[A]): Config[A, swaydb.java.PureFunction.OnKey[A, Void, Return.Set[Void]], swaydb.PureFunction.OnKey[A, Void, Apply.Set[Void]]] =
    new Config(
      dir = dir,
      serializer = SerializerConverter.toScala(keySerializer),
      functionClassTag = ClassTag.Any.asInstanceOf[ClassTag[swaydb.PureFunction.OnKey[A, Void, Apply.Set[Void]]]]
    )

  def withoutFunctions[A](dir: Path,
                          serializer: JavaSerializer[A]): Config[A, PureFunction.VoidS[A], Void] =
    new Config[A, PureFunction.VoidS[A], Void](
      dir = dir,
      serializer = SerializerConverter.toScala(serializer),
      functionClassTag = ClassTag.Nothing.asInstanceOf[ClassTag[Void]]
    )

}
