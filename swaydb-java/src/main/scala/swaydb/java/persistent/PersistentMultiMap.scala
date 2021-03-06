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

package swaydb.java.persistent

import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.util.Eithers
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{CompactionConfig, LevelMeter, LevelThrottle, LevelZeroThrottle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice
import swaydb.data.{Atomic, Functions, OptimiseWrites}
import swaydb.effect.Dir
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.java.{KeyComparator, KeyOrderConverter}
import swaydb.persistent.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.utils.Java.JavaFunction
import swaydb.{Apply, Bag, CommonConfigs, Glass, PureFunction}

import java.nio.file.Path
import java.util.Collections
import scala.compat.java8.FunctionConverters._
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import swaydb.utils.StorageUnits._

object PersistentMultiMap {

  final class Config[M, K, V, F](dir: Path,
                                 private var mapSize: Int = DefaultConfigs.mapSize,
                                 private var appliedFunctionsMapSize: Int = 4.mb,
                                 private var clearAppliedFunctionsOnBoot: Boolean = false,
                                 private var mmapMaps: MMAP.Map = DefaultConfigs.mmap(),
                                 private var recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                                 private var mmapAppendix: MMAP.Map = DefaultConfigs.mmap(),
                                 private var appendixFlushCheckpointSize: Int = 2.mb,
                                 private var otherDirs: java.util.Collection[Dir] = Collections.emptyList(),
                                 private var cacheKeyValueIds: Boolean = true,
                                 private var compactionConfig: Option[CompactionConfig] = None,
                                 private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                                 private var sortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                                 private var randomSearchIndex: RandomSearchIndex = DefaultConfigs.randomSearchIndex(),
                                 private var binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                                 private var mightContainIndex: MightContainIndex = DefaultConfigs.mightContainIndex(),
                                 private var optimiseWrites: OptimiseWrites = CommonConfigs.optimiseWrites(),
                                 private var atomic: Atomic = CommonConfigs.atomic(),
                                 private var valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                                 private var segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                                 private var fileCache: FileCache.On = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                                 private var memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                                 private var levelZeroThrottle: JavaFunction[LevelZeroMeter, LevelZeroThrottle] = (DefaultConfigs.levelZeroThrottle _).asJava,
                                 private var levelOneThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelOneThrottle _).asJava,
                                 private var levelTwoThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelTwoThrottle _).asJava,
                                 private var levelThreeThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelThreeThrottle _).asJava,
                                 private var levelFourThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelFourThrottle _).asJava,
                                 private var levelFiveThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelFiveThrottle _).asJava,
                                 private var levelSixThrottle: JavaFunction[LevelMeter, LevelThrottle] = (DefaultConfigs.levelSixThrottle _).asJava,
                                 private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = DefaultConfigs.accelerator.asJava,
                                 private var byteComparator: KeyComparator[Slice[java.lang.Byte]] = null,
                                 private var typedComparator: KeyComparator[K] = null)(implicit functionClassTag: ClassTag[F],
                                                                                       keySerializer: Serializer[K],
                                                                                       mapKeySerializer: Serializer[M],
                                                                                       valueSerializer: Serializer[V],
                                                                                       functions: Functions[F],
                                                                                       evd: F <:< PureFunction[K, V, Apply.Map[V]]) {

    def setMapSize(mapSize: Int) = {
      this.mapSize = mapSize
      this
    }

    def setCompactionConfig(config: CompactionConfig) = {
      this.compactionConfig = Some(config)
      this
    }

    def setAppliedFunctionsMapSize(size: Int) = {
      this.appliedFunctionsMapSize = size
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

    def setClearAppliedFunctionsOnBoot(clear: Boolean) = {
      this.clearAppliedFunctionsOnBoot = clear
      this
    }

    def setMmapMaps(mmapMaps: MMAP.Map) = {
      this.mmapMaps = mmapMaps
      this
    }

    def setRecoveryMode(recoveryMode: RecoveryMode) = {
      this.recoveryMode = recoveryMode
      this
    }

    def setMmapAppendix(mmapAppendix: MMAP.Map) = {
      this.mmapAppendix = mmapAppendix
      this
    }

    def setAppendixFlushCheckpointSize(appendixFlushCheckpointSize: Int) = {
      this.appendixFlushCheckpointSize = appendixFlushCheckpointSize
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

    def setThreadStateCache(threadStateCache: ThreadStateCache) = {
      this.threadStateCache = threadStateCache
      this
    }

    def setSortedKeyIndex(sortedKeyIndex: SortedKeyIndex) = {
      this.sortedKeyIndex = sortedKeyIndex
      this
    }

    def setRandomSearchIndex(randomSearchIndex: RandomSearchIndex) = {
      this.randomSearchIndex = randomSearchIndex
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

    def setLevelZeroThrottle(levelZeroThrottle: JavaFunction[LevelZeroMeter, LevelZeroThrottle]) = {
      this.levelZeroThrottle = levelZeroThrottle
      this
    }

    def setLevelOneThrottle(levelOneThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelOneThrottle = levelOneThrottle
      this
    }

    def setLevelTwoThrottle(levelTwoThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelTwoThrottle = levelTwoThrottle
      this
    }

    def setLevelThreeThrottle(levelThreeThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelThreeThrottle = levelThreeThrottle
      this
    }

    def setLevelFourThrottle(levelFourThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelFourThrottle = levelFourThrottle
      this
    }

    def setLevelFiveThrottle(levelFiveThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelFiveThrottle = levelFiveThrottle
      this
    }

    def setLevelSixThrottle(levelSixThrottle: JavaFunction[LevelMeter, LevelThrottle]) = {
      this.levelSixThrottle = levelSixThrottle
      this
    }

    def setAcceleration(acceleration: JavaFunction[LevelZeroMeter, Accelerator]) = {
      this.acceleration = acceleration
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

    def get(): swaydb.java.MultiMap[M, K, V, F] = {
      val comparator: Either[KeyComparator[Slice[java.lang.Byte]], KeyComparator[K]] =
        Eithers.nullCheck(
          left = byteComparator,
          right = typedComparator,
          default = KeyComparator.lexicographic
        )

      val scalaKeyOrder: KeyOrder[Slice[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, keySerializer)

      val scalaMap =
        swaydb.persistent.MultiMap[M, K, V, PureFunction.Map[K, V], Glass](
          dir = dir,
          mapSize = mapSize,
          appliedFunctionsMapSize = appliedFunctionsMapSize,
          clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
          mmapMaps = mmapMaps,
          recoveryMode = recoveryMode,
          mmapAppendix = mmapAppendix,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          otherDirs = otherDirs.asScala.toSeq,
          cacheKeyValueIds = cacheKeyValueIds,
          compactionConfig = compactionConfig getOrElse CommonConfigs.compactionConfig(),
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          acceleration = acceleration.asScala,
          threadStateCache = threadStateCache,
          sortedKeyIndex = sortedKeyIndex,
          randomSearchIndex = randomSearchIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainIndex = mightContainIndex,
          valuesConfig = valuesConfig,
          segmentConfig = segmentConfig,
          fileCache = fileCache,
          memoryCache = memoryCache,
          levelZeroThrottle = levelZeroThrottle.asScala,
          levelOneThrottle = levelOneThrottle.asScala,
          levelTwoThrottle = levelTwoThrottle.asScala,
          levelThreeThrottle = levelThreeThrottle.asScala,
          levelFourThrottle = levelFourThrottle.asScala,
          levelFiveThrottle = levelFiveThrottle.asScala,
          levelSixThrottle = levelSixThrottle.asScala
        )(keySerializer = keySerializer,
          mapKeySerializer = mapKeySerializer,
          valueSerializer = valueSerializer,
          functions = functions.asInstanceOf[Functions[PureFunction.Map[K, V]]],
          functionClassTag = functionClassTag.asInstanceOf[ClassTag[PureFunction.Map[K, V]]],
          bag = Bag.glass,
          byteKeyOrder = scalaKeyOrder
        )

      swaydb.java.MultiMap[M, K, V, F](scalaMap.asInstanceOf[swaydb.MultiMap[M, K, V, F, Glass]])
    }
  }

  def functionsOn[M, K, V](dir: Path,
                           mapKeySerializer: JavaSerializer[M],
                           keySerializer: JavaSerializer[K],
                           valueSerializer: JavaSerializer[V],
                           functions: java.lang.Iterable[PureFunction[K, V, Apply.Map[V]]]): Config[M, K, V, PureFunction[K, V, Apply.Map[V]]] = {
    implicit val scalaFunctions = Functions(functions)
    implicit val scalaKeySerializer: Serializer[K] = SerializerConverter.toScala(keySerializer)
    implicit val scalaMapKeySerializer: Serializer[M] = SerializerConverter.toScala(mapKeySerializer)
    implicit val scalaValueSerializer: Serializer[V] = SerializerConverter.toScala(valueSerializer)

    new Config(dir)
  }

  def functionsOff[M, K, V](dir: Path,
                            mapKeySerializer: JavaSerializer[M],
                            keySerializer: JavaSerializer[K],
                            valueSerializer: JavaSerializer[V]): Config[M, K, V, Void] = {
    implicit val scalaKeySerializer: Serializer[K] = SerializerConverter.toScala(keySerializer)
    implicit val scalaMapKeySerializer: Serializer[M] = SerializerConverter.toScala(mapKeySerializer)
    implicit val scalaValueSerializer: Serializer[V] = SerializerConverter.toScala(valueSerializer)
    implicit val evidence: Void <:< PureFunction[K, V, Apply.Map[V]] = null

    new Config[M, K, V, Void](dir)
  }
}
