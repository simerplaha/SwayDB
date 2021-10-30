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
import swaydb.java._
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
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

object PersistentSet {

  final class Config[A, F](dir: Path,
                           private var logSize: Int = DefaultConfigs.logSize,
                           private var appliedFunctionsLogSize: Int = 4.mb,
                           private var clearAppliedFunctionsOnBoot: Boolean = false,
                           private var mmapLogs: MMAP.Log = DefaultConfigs.mmap(),
                           private var recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                           private var mmapAppendixLogs: MMAP.Log = DefaultConfigs.mmap(),
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
                           private var typedComparator: KeyComparator[A] = null)(implicit functionClassTag: ClassTag[F],
                                                                                 serializer: Serializer[A],
                                                                                 functions: Functions[F],
                                                                                 evd: F <:< PureFunction[A, Nothing, Apply.Set[Nothing]]) {

    def setLogSize(logSize: Int) = {
      this.logSize = logSize
      this
    }

    def setCompactionConfig(config: CompactionConfig) = {
      this.compactionConfig = Some(config)
      this
    }

    def setAppliedFunctionsLogSize(size: Int) = {
      this.appliedFunctionsLogSize = size
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

    def setMmapMaps(mmapLogs: MMAP.Log) = {
      this.mmapLogs = mmapLogs
      this
    }

    def setRecoveryMode(recoveryMode: RecoveryMode) = {
      this.recoveryMode = recoveryMode
      this
    }

    def setMmapAppendix(mmapAppendixLogs: MMAP.Log) = {
      this.mmapAppendixLogs = mmapAppendixLogs
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
        swaydb.persistent.Set[A, PureFunction.Set[A], Glass](
          dir = dir,
          logSize = logSize,
          appliedFunctionsLogSize = appliedFunctionsLogSize,
          clearAppliedFunctionsOnBoot = clearAppliedFunctionsOnBoot,
          mmapLogs = mmapLogs,
          recoveryMode = recoveryMode,
          mmapAppendixLogs = mmapAppendixLogs,
          appendixFlushCheckpointSize = appendixFlushCheckpointSize,
          otherDirs = otherDirs.asScala.toSeq,
          cacheKeyValueIds = cacheKeyValueIds,
          optimiseWrites = optimiseWrites,
          atomic = atomic,
          compactionConfig = compactionConfig getOrElse CommonConfigs.compactionConfig(),
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
