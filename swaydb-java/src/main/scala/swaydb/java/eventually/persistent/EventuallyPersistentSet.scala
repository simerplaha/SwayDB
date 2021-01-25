/*
 * Copyright (c) 2018 Simer JS Plaha (simer.j@gmail.com - @simerplaha)
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

package swaydb.java.eventually.persistent

import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.util.Eithers
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.CompactionConfig
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.Java.JavaFunction
import swaydb.data.util.StorageUnits._
import swaydb.data.{Atomic, Functions, OptimiseWrites}
import swaydb.eventually.persistent.DefaultConfigs
import swaydb.java._
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.serializers.Serializer
import swaydb.{Apply, Bag, CommonConfigs, Glass, PureFunction}

import java.nio.file.Path
import java.util.Collections
import scala.compat.java8.DurationConverters.DurationOps
import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object EventuallyPersistentSet {

  final class Config[A, F](dir: Path,
                           private var mapSize: Int = DefaultConfigs.mapSize,
                           private var appliedFunctionsMapSize: Int = 4.mb,
                           private var clearAppliedFunctionsOnBoot: Boolean = false,
                           private var maxMemoryLevelSize: Int = 100.mb,
                           private var maxSegmentsToPush: Int = 5,
                           private var memoryLevelSegmentSize: Int = DefaultConfigs.segmentSize,
                           private var memoryLevelMaxKeyValuesCountPerSegment: Int = 200000,
                           private var persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                           private var otherDirs: java.util.Collection[Dir] = Collections.emptyList(),
                           private var cacheKeyValueIds: Boolean = true,
                           private var mmapPersistentLevelAppendix: MMAP.Map = DefaultConfigs.mmap(),
                           private var memorySegmentDeleteDelay: FiniteDuration = CommonConfigs.segmentDeleteDelay,
                           private var compactionConfig: Option[CompactionConfig] = None,
                           private var optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                           private var atomic: Atomic = CommonConfigs.atomic(),
                           private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = DefaultConfigs.accelerator.asJava,
                           private var persistentLevelSortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                           private var persistentLevelRandomSearchIndex: RandomSearchIndex = DefaultConfigs.randomSearchIndex(),
                           private var binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                           private var mightContainIndex: MightContainIndex = DefaultConfigs.mightContainIndex(),
                           private var valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                           private var segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                           private var fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                           private var memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                           private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                           private var byteComparator: KeyComparator[Slice[java.lang.Byte]] = null,
                           private var typedComparator: KeyComparator[A] = null)(implicit functionClassTag: ClassTag[F],
                                                                                 serializer: Serializer[A],
                                                                                 functions: Functions[F],
                                                                                 evd: F <:< PureFunction[A, Nothing, Apply.Set[Nothing]]) {

    def setMapSize(mapSize: Int) = {
      this.mapSize = mapSize
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

    def setAppliedFunctionsMapSize(size: Int) = {
      this.appliedFunctionsMapSize = size
      this
    }

    def setClearAppliedFunctionsOnBoot(clear: Boolean) = {
      this.clearAppliedFunctionsOnBoot = clear
      this
    }

    def setMaxMemoryLevelSize(maxMemoryLevelSize: Int) = {
      this.maxMemoryLevelSize = maxMemoryLevelSize
      this
    }

    def setMaxSegmentsToPush(maxSegmentsToPush: Int) = {
      this.maxSegmentsToPush = maxSegmentsToPush
      this
    }

    def setMemoryLevelSegmentSize(memoryLevelSegmentSize: Int) = {
      this.memoryLevelSegmentSize = memoryLevelSegmentSize
      this
    }

    def setMemoryLevelMaxKeyValuesCountPerSegment(memoryLevelMaxKeyValuesCountPerSegment: Int) = {
      this.memoryLevelMaxKeyValuesCountPerSegment = memoryLevelMaxKeyValuesCountPerSegment
      this
    }

    def setPersistentLevelAppendixFlushCheckpointSize(persistentLevelAppendixFlushCheckpointSize: Int) = {
      this.persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize
      this
    }

    def setOtherDirs(otherDirs: java.util.Collection[Dir]) = {
      this.otherDirs = otherDirs
      this
    }

    def setCacheKeyValueIds(cacheKeyValueIds: Boolean) = {
      this.cacheKeyValueIds = cacheKeyValueIds
      this
    }

    def setMmapPersistentLevelAppendix(mmapPersistentLevelAppendix: MMAP.Map) = {
      this.mmapPersistentLevelAppendix = mmapPersistentLevelAppendix
      this
    }

    def setMemorySegmentDeleteDelay(memorySegmentDeleteDelay: java.time.Duration) = {
      this.memorySegmentDeleteDelay = memorySegmentDeleteDelay.toScala
      this
    }

    def setAcceleration(acceleration: JavaFunction[LevelZeroMeter, Accelerator]) = {
      this.acceleration = acceleration
      this
    }

    def setPersistentLevelSortedKeyIndex(persistentLevelSortedKeyIndex: SortedKeyIndex) = {
      this.persistentLevelSortedKeyIndex = persistentLevelSortedKeyIndex
      this
    }

    def setPersistentLevelRandomSearchIndex(persistentLevelRandomSearchIndex: RandomSearchIndex) = {
      this.persistentLevelRandomSearchIndex = persistentLevelRandomSearchIndex
      this
    }

    def setBinarySearchIndex(binarySearchIndex: BinarySearchIndex) = {
      this.binarySearchIndex = binarySearchIndex
      this
    }

    def setMightContainIndex(mightContainIndex: MightContainIndex) = {
      this.mightContainIndex = mightContainIndex
      this
    }

    def setValuesConfig(valuesConfig: ValuesConfig) = {
      this.valuesConfig = valuesConfig
      this
    }

    def setSegmentConfig(segmentConfig: SegmentConfig) = {
      this.segmentConfig = segmentConfig
      this
    }

    def setFileCache(fileCache: FileCache.On) = {
      this.fileCache = fileCache
      this
    }

    def setMemoryCache(memoryCache: MemoryCache) = {
      this.memoryCache = memoryCache
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

    def get(): swaydb.java.Set[A, F] = {
      val comparator: Either[KeyComparator[Slice[java.lang.Byte]], KeyComparator[A]] =
        Eithers.nullCheck(
          left = byteComparator,
          right = typedComparator,
          default = KeyComparator.lexicographic
        )

      val scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, serializer)

      val scalaMap =
        swaydb.eventually.persistent.Set[A, PureFunction.Set[A], Glass](
          dir = dir,
          mapSize = mapSize,
          appliedFunctionsMapSize = appliedFunctionsMapSize,
          clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
          maxMemoryLevelSize = maxMemoryLevelSize,
          maxSegmentsToPush = maxSegmentsToPush,
          memoryLevelSegmentSize = memoryLevelSegmentSize,
          memoryLevelMaxKeyValuesCountPerSegment = memoryLevelMaxKeyValuesCountPerSegment,
          persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
          otherDirs = otherDirs.asScala.toSeq,
          cacheKeyValueIds = cacheKeyValueIds,
          mmapPersistentLevelAppendix = mmapPersistentLevelAppendix,
          memorySegmentDeleteDelay = memorySegmentDeleteDelay,
          compactionConfig = compactionConfig getOrElse CommonConfigs.compactionConfig(),
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          acceleration = acceleration.apply,
          persistentLevelSortedKeyIndex = persistentLevelSortedKeyIndex,
          persistentLevelRandomSearchIndex = persistentLevelRandomSearchIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainIndex = mightContainIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
          threadStateCache = threadStateCache
        )(serializer = serializer,
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[PureFunction.Set[A]]],
          bag = Bag.glass,
          functions = functions.asInstanceOf[Functions[PureFunction.Set[A]]],
          byteKeyOrder = scalaKeyOrder
        )

      swaydb.java.Set[A, F](scalaMap.asInstanceOf[swaydb.Set[A, F, Glass]])
    }
  }

  def functionsOn[A](dir: Path,
                     serializer: JavaSerializer[A],
                     functions: java.lang.Iterable[PureFunction[A, Void, Apply.Set[Void]]]): Config[A, PureFunction[A, Void, Apply.Set[Void]]] = {

    implicit val scalaFunctions = functions.castToNothingFunctions
    implicit val scalaSerializer: Serializer[A] = SerializerConverter.toScala(serializer)
    val config: Config[A, PureFunction[A, Nothing, Apply.Set[Nothing]]] = new Config(dir)

    config.asInstanceOf[Config[A, PureFunction[A, Void, Apply.Set[Void]]]]
  }

  def functionsOff[A](dir: Path,
                      serializer: JavaSerializer[A]): Config[A, Void] = {
    implicit val scalaSerializer: Serializer[A] = SerializerConverter.toScala(serializer)
    implicit val evd: Void <:< PureFunction[A, Nothing, Apply.Set[Nothing]] = null

    new Config(dir)
  }
}
