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

package swaydb.java.eventually.persistent

import swaydb.config.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.config.compaction.CompactionConfig
import swaydb.config._
import swaydb.configs.level.DefaultExecutionContext
import swaydb.effect.Dir
import swaydb.eventually.persistent.DefaultConfigs
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.java.{KeyComparator, KeyOrderConverter}
import swaydb.serializers.Serializer
import swaydb.slice.Slice
import swaydb.slice.order.KeyOrder
import swaydb.utils.Eithers
import swaydb.utils.Java._
import swaydb.utils.StorageUnits._
import swaydb.{Apply, Bag, CommonConfigs, Glass, PureFunction}

import java.nio.file.Path
import java.util.Collections
import scala.compat.java8.DurationConverters.DurationOps
import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object EventuallyPersistentMap {

  final class Config[K, V, F](dir: Path,
                              private var logSize: Int = DefaultConfigs.logSize,
                              private var appliedFunctionsLogSize: Int = 4.mb,
                              private var clearAppliedFunctionsOnBoot: Boolean = false,
                              private var maxMemoryLevelSize: Int = 100.mb,
                              private var maxSegmentsToPush: Int = 5,
                              private var memoryLevelSegmentSize: Int = DefaultConfigs.segmentSize,
                              private var memoryLevelMaxKeyValuesCountPerSegment: Int = 200000,
                              private var persistentLevelAppendixFlushCheckpointSize: Int = 2.mb,
                              private var otherDirs: java.util.Collection[Dir] = Collections.emptyList(),
                              private var cacheKeyValueIds: Boolean = true,
                              private var mmapPersistentLevelAppendixLogs: MMAP.Log = DefaultConfigs.mmap(),
                              private var memorySegmentDeleteDelay: FiniteDuration = CommonConfigs.segmentDeleteDelay,
                              private var compactionConfig: Option[CompactionConfig] = None,
                              private var optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                              private var atomic: Atomic = CommonConfigs.atomic(),
                              private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = DefaultConfigs.accelerator.asJava,
                              private var persistentLevelSortedIndex: SortedIndex = DefaultConfigs.sortedIndex(),
                              private var persistentLevelHashIndex: HashIndex = DefaultConfigs.hashIndex(),
                              private var binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                              private var bloomFilter: BloomFilter = DefaultConfigs.bloomFilter(),
                              private var valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                              private var segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                              private var fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                              private var memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                              private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
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

    def setAppliedFunctionsLogSize(size: Int) = {
      this.appliedFunctionsLogSize = size
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

    def setMmapPersistentLevelAppendix(mmapPersistentLevelAppendixLogs: MMAP.Log) = {
      this.mmapPersistentLevelAppendixLogs = mmapPersistentLevelAppendixLogs
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

    def setPersistentLevelSortedIndex(persistentLevelSortedIndex: SortedIndex) = {
      this.persistentLevelSortedIndex = persistentLevelSortedIndex
      this
    }

    def setPersistentLevelHashIndex(persistentLevelHashIndex: HashIndex) = {
      this.persistentLevelHashIndex = persistentLevelHashIndex
      this
    }

    def setBinarySearchIndex(binarySearchIndex: BinarySearchIndex) = {
      this.binarySearchIndex = binarySearchIndex
      this
    }

    def setBloomFilter(bloomFilter: BloomFilter) = {
      this.bloomFilter = bloomFilter
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
        swaydb.eventually.persistent.Map[K, V, PureFunction.Map[K, V], Glass](
          dir = dir,
          logSize = logSize,
          appliedFunctionsLogSize = appliedFunctionsLogSize,
          clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
          maxMemoryLevelSize = maxMemoryLevelSize,
          maxSegmentsToPush = maxSegmentsToPush,
          memoryLevelSegmentSize = memoryLevelSegmentSize,
          memoryLevelMaxKeyValuesCountPerSegment = memoryLevelMaxKeyValuesCountPerSegment,
          persistentLevelAppendixFlushCheckpointSize = persistentLevelAppendixFlushCheckpointSize,
          otherDirs = otherDirs.asScala.toSeq,
          cacheKeyValueIds = cacheKeyValueIds,
          mmapPersistentLevelAppendixLogs = mmapPersistentLevelAppendixLogs,
          memorySegmentDeleteDelay = memorySegmentDeleteDelay,
          compactionConfig = compactionConfig getOrElse CommonConfigs.compactionConfig(),
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          acceleration = acceleration.apply,
          persistentLevelSortedIndex = persistentLevelSortedIndex,
          persistentLevelHashIndex = persistentLevelHashIndex,
          binarySearchIndex = binarySearchIndex,
          bloomFilter = bloomFilter,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
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

  def functionsOn[K, V](dir: Path,
                        keySerializer: JavaSerializer[K],
                        valueSerializer: JavaSerializer[V],
                        functions: java.lang.Iterable[PureFunction[K, V, Apply.Map[V]]]): Config[K, V, PureFunction[K, V, Apply.Map[V]]] = {
    implicit val pureFunctions = Functions(functions)
    implicit val scalaKeySerializer: Serializer[K] = SerializerConverter.toScala(keySerializer)
    implicit val scalaValueSerializer: Serializer[V] = SerializerConverter.toScala(valueSerializer)

    new Config(dir = dir)
  }

  def functionsOff[K, V](dir: Path,
                         keySerializer: JavaSerializer[K],
                         valueSerializer: JavaSerializer[V]): Config[K, V, Void] = {
    implicit val scalaKeySerializer: Serializer[K] = SerializerConverter.toScala(keySerializer)
    implicit val scalaValueSerializer: Serializer[V] = SerializerConverter.toScala(valueSerializer)
    implicit val evidence: Void <:< PureFunction[K, V, Apply.Map[V]] = null

    new Config[K, V, Void](dir = dir)
  }
}
