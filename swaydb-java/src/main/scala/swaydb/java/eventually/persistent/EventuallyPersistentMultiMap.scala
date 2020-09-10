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

package swaydb.java.eventually.persistent

import java.lang
import java.nio.file.Path
import java.time.Duration
import java.util.Collections
import java.util.concurrent.ExecutorService

import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.build.BuildValidator
import swaydb.core.util.Eithers
import swaydb.data.DataType
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.util.Java.JavaFunction
import swaydb.data.util.StorageUnits._
import swaydb.eventually.persistent.DefaultConfigs
import swaydb.java.data.slice
import swaydb.java.data.slice.{Slice => JavaSlice}
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.java.{KeyComparator, KeyOrderConverter, PureFunction, Return}
import swaydb.serializers.Serializer
import swaydb.{Apply, Bag}

import scala.compat.java8.DurationConverters._
import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object EventuallyPersistentMultiMap {

  final class Config[M, K, V, F](dir: Path,
                                 private var mapSize: Int = 4.mb,
                                 private var appliedFunctionsMapSize: Int = 4.mb,
                                 private var clearAppliedFunctionsOnBoot: Boolean = false,
                                 private var maxMemoryLevelSize: Int = 100.mb,
                                 private var maxSegmentsToPush: Int = 5,
                                 private var memoryLevelSegmentSize: Int = 2.mb,
                                 private var memoryLevelMaxKeyValuesCountPerSegment: Int = 200000,
                                 private var persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                                 private var otherDirs: java.util.Collection[Dir] = Collections.emptyList(),
                                 private var cacheKeyValueIds: Boolean = true,
                                 private var mmapPersistentLevelAppendix: MMAP.Map = DefaultConfigs.mmap(),
                                 private var deleteMemorySegmentsEventually: Boolean = true,
                                 private var shutdownTimeout: Duration = 30.seconds.toJava,
                                 private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                                 private var persistentLevelSortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                                 private var persistentLevelRandomKeyIndex: RandomKeyIndex = DefaultConfigs.randomKeyIndex(),
                                 private var binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                                 private var mightContainKeyIndex: MightContainIndex = DefaultConfigs.mightContainKeyIndex(),
                                 private var valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                                 private var segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                                 private var fileCache: FileCache.Enable = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                 private var memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                                 private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                                 private var byteComparator: KeyComparator[JavaSlice[java.lang.Byte]] = null,
                                 private var typedComparator: KeyComparator[K] = null,
                                 private var compactionEC: Option[ExecutionContext] = None,
                                 private var buildValidator: BuildValidator = BuildValidator.DisallowOlderVersions(DataType.Map),
                                 keySerializer: Serializer[K],
                                 mapKeySerializer: Serializer[M],
                                 valueSerializer: Serializer[V],
                                 functionClassTag: ClassTag[_]) {

    def setMapSize(mapSize: Int) = {
      this.mapSize = mapSize
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

    def setDeleteMemorySegmentsEventually(deleteMemorySegmentsEventually: Boolean) = {
      this.deleteMemorySegmentsEventually = deleteMemorySegmentsEventually
      this
    }

    def setShutdownTimeout(duration: Duration) = {
      this.shutdownTimeout = duration
      this
    }

    def setAcceleration(acceleration: JavaFunction[LevelZeroMeter, Accelerator]) = {
      this.acceleration = acceleration
      this
    }

    def setPersistentLevelSortedKeyIndex(persitentLevelSortedKeyIndex: SortedKeyIndex) = {
      this.persistentLevelSortedKeyIndex = persitentLevelSortedKeyIndex
      this
    }

    def setPersistentLevelRandomKeyIndex(persitentLevelRandomKeyIndex: RandomKeyIndex) = {
      this.persistentLevelRandomKeyIndex = persitentLevelRandomKeyIndex
      this
    }

    def setBinarySearchIndex(binarySearchIndex: BinarySearchIndex) = {
      this.binarySearchIndex = binarySearchIndex
      this
    }

    def setMightContainKeyIndex(mightContainKeyIndex: MightContainIndex) = {
      this.mightContainKeyIndex = mightContainKeyIndex
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

    def setFileCache(fileCache: FileCache.Enable) = {
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

    def setByteComparator(byteComparator: KeyComparator[JavaSlice[java.lang.Byte]]) = {
      this.byteComparator = byteComparator
      this
    }

    def setTypedComparator(typedComparator: KeyComparator[K]) = {
      this.typedComparator = typedComparator
      this
    }

    def setCompactionExecutionContext(executionContext: ExecutorService) = {
      this.compactionEC = Some(ExecutionContext.fromExecutorService(executionContext))
      this
    }

    private val functions = swaydb.MultiMap.Functions[M, K, V, swaydb.PureFunction[K, V, Apply.Map[V]]]()(keySerializer, mapKeySerializer, valueSerializer)

    def registerFunctions(functions: F*): Config[M, K, V, F] = {
      functions.foreach(registerFunction(_))
      this
    }

    def registerFunction(function: F): Config[M, K, V, F] = {
      functions.register(PureFunction.asScala(function.asInstanceOf[swaydb.java.PureFunction[K, V, Return.Map[V]]]))
      this
    }

    def setBuildValidator(buildValidator: BuildValidator) = {
      this.buildValidator = buildValidator
      this
    }

    def get(): swaydb.java.MultiMap[M, K, V, F] = {
      val comparator: Either[KeyComparator[slice.Slice[lang.Byte]], KeyComparator[K]] =
        Eithers.nullCheck(
          left = byteComparator,
          right = typedComparator,
          default = KeyComparator.lexicographic
        )

      val scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, keySerializer)

      val scalaMap =
        swaydb.eventually.persistent.MultiMap[M, K, V, swaydb.PureFunction[K, V, Apply.Map[V]], Bag.Less](
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
          shutdownTimeout = shutdownTimeout.toScala,
          cacheKeyValueIds = cacheKeyValueIds,
          mmapPersistentLevelAppendix = mmapPersistentLevelAppendix,
          deleteMemorySegmentsEventually = deleteMemorySegmentsEventually,
          acceleration = acceleration.apply,
          persistentLevelSortedKeyIndex = persistentLevelSortedKeyIndex,
          persistentLevelRandomKeyIndex = persistentLevelRandomKeyIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainKeyIndex = mightContainKeyIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
          threadStateCache = threadStateCache
        )(keySerializer = keySerializer,
          mapKeySerializer = mapKeySerializer,
          valueSerializer = valueSerializer,
          functions = functions,
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[swaydb.PureFunction[K, V, Apply.Map[V]]]],
          bag = Bag.less,
          byteKeyOrder = scalaKeyOrder,
          compactionEC = compactionEC.getOrElse(DefaultExecutionContext.compactionEC),
          buildValidator = buildValidator
        )

      swaydb.java.MultiMap[M, K, V, F](scalaMap)
    }
  }

  def functionsOn[M, K, V](dir: Path,
                           mapKeySerializer: JavaSerializer[M],
                           keySerializer: JavaSerializer[K],
                           valueSerializer: JavaSerializer[V]): Config[M, K, V, swaydb.java.PureFunction[K, V, Return.Map[V]]] =
    new Config(
      dir = dir,
      mapKeySerializer = SerializerConverter.toScala(mapKeySerializer),
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Any
    )

  def functionsOff[M, K, V](dir: Path,
                            mapKeySerializer: JavaSerializer[M],
                            keySerializer: JavaSerializer[K],
                            valueSerializer: JavaSerializer[V]): Config[M, K, V, Void] =
    new Config[M, K, V, Void](
      dir = dir,
      mapKeySerializer = SerializerConverter.toScala(mapKeySerializer),
      keySerializer = SerializerConverter.toScala(keySerializer),
      valueSerializer = SerializerConverter.toScala(valueSerializer),
      functionClassTag = ClassTag.Nothing
    )
}
