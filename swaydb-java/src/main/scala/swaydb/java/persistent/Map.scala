/*
 * Copyright (c) 2019 Simer Plaha (@simerplaha)
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
import swaydb.java.{IO, KeyOrderConverter, Return}
import swaydb.serializers.Serializer
import swaydb.{Apply, SwayDB, Tag}

import scala.beans.{BeanProperty, BooleanBeanProperty}
import scala.compat.java8.DurationConverters._
import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration._
import scala.reflect.ClassTag

object Map {

  class Config[K, V, F <: swaydb.java.PureFunction[K, V, Return.Map[V]], SF](@BeanProperty var dir: Path,
                                                                             @BeanProperty var maxOpenSegments: Int = 1000,
                                                                             @BeanProperty var memoryCacheSize: Int = 100.mb,
                                                                             @BeanProperty var blockSize: Int = 4098,
                                                                             @BeanProperty var mapSize: Int = 4.mb,
                                                                             @BooleanBeanProperty var mmapMaps: Boolean = true,
                                                                             @BeanProperty var recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                                                                             @BooleanBeanProperty var mmapAppendix: Boolean = true,
                                                                             @BeanProperty var mmapSegments: MMAP = MMAP.WriteAndRead,
                                                                             @BeanProperty var segmentSize: Int = 2.mb,
                                                                             @BeanProperty var appendixFlushCheckpointSize: Int = 2.mb,
                                                                             @BeanProperty var otherDirs: Seq[Dir] = Seq.empty,
                                                                             @BeanProperty var memorySweeperPollInterval: java.time.Duration = 10.seconds.toJava,
                                                                             @BeanProperty var fileSweeperPollInterval: java.time.Duration = 10.seconds.toJava,
                                                                             @BeanProperty var mightContainFalsePositiveRate: Double = 0.01,
                                                                             @BooleanBeanProperty var compressDuplicateValues: Boolean = true,
                                                                             @BooleanBeanProperty var deleteSegmentsEventually: Boolean = true,
                                                                             @BeanProperty var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                                                                             @BeanProperty var comparator: IO[Comparator[ByteSlice], Comparator[K]] = IO.leftNeverException[Comparator[ByteSlice], Comparator[K]](swaydb.java.SwayDB.defaultComparator),
                                                                             @BeanProperty var fileSweeperExecutorService: ExecutorService = SwayDB.defaultExecutorService,
                                                                             keySerializer: Serializer[K],
                                                                             valueSerializer: Serializer[V],
                                                                             functionClassTag: ClassTag[SF]) {

    implicit def scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, keySerializer)

    implicit def fileSweeperEC: ExecutionContext = fileSweeperExecutorService.asScala

    def init(): IO[Throwable, swaydb.java.MapIO[K, V, F]] =
      IO.fromScala {
        swaydb.IO {
          val scalaMap =
            swaydb.persistent.Map[K, V, SF, swaydb.IO.ThrowableIO](
              dir = dir,
              maxOpenSegments = maxOpenSegments,
              memoryCacheSize = memoryCacheSize,
              blockSize = blockSize,
              mapSize = mapSize,
              mmapMaps = mmapMaps,
              recoveryMode = recoveryMode,
              mmapAppendix = mmapAppendix,
              mmapSegments = mmapSegments,
              segmentSize = segmentSize,
              appendixFlushCheckpointSize = appendixFlushCheckpointSize,
              otherDirs = otherDirs,
              memorySweeperPollInterval = memorySweeperPollInterval.toScala,
              fileSweeperPollInterval = fileSweeperPollInterval.toScala,
              mightContainFalsePositiveRate = mightContainFalsePositiveRate,
              compressDuplicateValues = compressDuplicateValues,
              deleteSegmentsEventually = deleteSegmentsEventually,
              acceleration = acceleration.asScala
            )(keySerializer = keySerializer,
              valueSerializer = valueSerializer,
              functionClassTag = functionClassTag,
              tag = Tag.throwableIO,
              keyOrder = Left(scalaKeyOrder),
              fileSweeperEC = fileSweeperEC
            ).get

          swaydb.java.MapIO[K, V, F](scalaMap)
        }
      }
  }

  def configFunctions[K, V, F](dir: Path,
                               keySerializer: JavaSerializer[K],
                               valueSerializer: JavaSerializer[V]): Config[K, V, swaydb.java.PureFunction[K, V, Return.Map[V]], swaydb.PureFunction[K, V, Apply.Map[V]]] =
    new Config(
      dir = dir,
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Any.asInstanceOf[ClassTag[swaydb.PureFunction[K, V, Apply.Map[V]]]]
    )

  def config[K, V](dir: Path,
                   keySerializer: JavaSerializer[K],
                   valueSerializer: JavaSerializer[V]): Config[K, V, swaydb.java.PureFunction.VoidM[K, V], Void] =
    new Config[K, V, swaydb.java.PureFunction.VoidM[K, V], Void](
      dir = dir,
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Nothing.asInstanceOf[ClassTag[Void]]
    )

}
