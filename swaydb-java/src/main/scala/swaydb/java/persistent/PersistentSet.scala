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

package swaydb.java.persistent

import java.nio.file.Path
import java.time.Duration
import java.util.Collections
import java.util.concurrent.ExecutorService

import swaydb.configs.level.DefaultExecutionContext
import swaydb.core.build.BuildValidator
import swaydb.core.util.Eithers
import swaydb.data.{DataType, Functions}
import swaydb.data.accelerate.{Accelerator, LevelZeroMeter}
import swaydb.data.compaction.{LevelMeter, Throttle}
import swaydb.data.config._
import swaydb.data.order.KeyOrder
import swaydb.data.slice.Slice._
import swaydb.data.util.Java.JavaFunction
import swaydb.data.util.StorageUnits._
import swaydb.java._
import swaydb.java.data.slice.ByteSlice
import swaydb.java.serializers.{SerializerConverter, Serializer => JavaSerializer}
import swaydb.persistent.DefaultConfigs
import swaydb.serializers.Serializer
import swaydb.{Apply, Bag, PureFunction}

import scala.compat.java8.DurationConverters._
import scala.compat.java8.FunctionConverters._
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.{DurationInt, FiniteDuration}
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

object PersistentSet {

  final class Config[A, F](dir: Path,
                           private var mapSize: Int = 4.mb,
                           private var appliedFunctionsMapSize: Int = 4.mb,
                           private var clearAppliedFunctionsOnBoot: Boolean = false,
                           private var mmapMaps: MMAP.Map = DefaultConfigs.mmap(),
                           private var recoveryMode: RecoveryMode = RecoveryMode.ReportFailure,
                           private var mmapAppendix: MMAP.Map = DefaultConfigs.mmap(),
                           private var appendixFlushCheckpointSize: Int = 2.mb,
                           private var otherDirs: java.util.Collection[Dir] = Collections.emptyList(),
                           private var cacheKeyValueIds: Boolean = true,
                           private var shutdownTimeout: Duration = 30.seconds.toJava,
                           private var threadStateCache: ThreadStateCache = ThreadStateCache.Limit(hashMapMaxSize = 100, maxProbe = 10),
                           private var sortedKeyIndex: SortedKeyIndex = DefaultConfigs.sortedKeyIndex(),
                           private var randomKeyIndex: RandomKeyIndex = DefaultConfigs.randomKeyIndex(),
                           private var binarySearchIndex: BinarySearchIndex = DefaultConfigs.binarySearchIndex(),
                           private var mightContainKeyIndex: MightContainIndex = DefaultConfigs.mightContainKeyIndex(),
                           private var valuesConfig: ValuesConfig = DefaultConfigs.valuesConfig(),
                           private var segmentConfig: SegmentConfig = DefaultConfigs.segmentConfig(),
                           private var fileCache: FileCache.Enable = DefaultConfigs.fileCache(DefaultExecutionContext.sweeperEC),
                           private var memoryCache: MemoryCache = DefaultConfigs.memoryCache(DefaultExecutionContext.sweeperEC),
                           private var levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration] = (DefaultConfigs.levelZeroThrottle _).asJava,
                           private var levelOneThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelOneThrottle _).asJava,
                           private var levelTwoThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelTwoThrottle _).asJava,
                           private var levelThreeThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelThreeThrottle _).asJava,
                           private var levelFourThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelFourThrottle _).asJava,
                           private var levelFiveThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelFiveThrottle _).asJava,
                           private var levelSixThrottle: JavaFunction[LevelMeter, Throttle] = (DefaultConfigs.levelSixThrottle _).asJava,
                           private var acceleration: JavaFunction[LevelZeroMeter, Accelerator] = (Accelerator.noBrakes() _).asJava,
                           private var byteComparator: KeyComparator[ByteSlice] = null,
                           private var typedComparator: KeyComparator[A] = null,
                           private var compactionEC: Option[ExecutionContext] = None,
                           private var buildValidator: BuildValidator = BuildValidator.DisallowOlderVersions(DataType.Set))(implicit functionClassTag: ClassTag[F],
                                                                                                                            serializer: Serializer[A],
                                                                                                                            functions: Functions[F],
                                                                                                                            evd: F <:< PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]]) {

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

    def setShutdownTimeout(duration: Duration) = {
      this.shutdownTimeout = duration
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

    def setRandomKeyIndex(randomKeyIndex: RandomKeyIndex) = {
      this.randomKeyIndex = randomKeyIndex
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

    def setLevelZeroThrottle(levelZeroThrottle: JavaFunction[LevelZeroMeter, FiniteDuration]) = {
      this.levelZeroThrottle = levelZeroThrottle
      this
    }

    def setLevelOneThrottle(levelOneThrottle: JavaFunction[LevelMeter, Throttle]) = {
      this.levelOneThrottle = levelOneThrottle
      this
    }

    def setLevelTwoThrottle(levelTwoThrottle: JavaFunction[LevelMeter, Throttle]) = {
      this.levelTwoThrottle = levelTwoThrottle
      this
    }

    def setLevelThreeThrottle(levelThreeThrottle: JavaFunction[LevelMeter, Throttle]) = {
      this.levelThreeThrottle = levelThreeThrottle
      this
    }

    def setLevelFourThrottle(levelFourThrottle: JavaFunction[LevelMeter, Throttle]) = {
      this.levelFourThrottle = levelFourThrottle
      this
    }

    def setLevelFiveThrottle(levelFiveThrottle: JavaFunction[LevelMeter, Throttle]) = {
      this.levelFiveThrottle = levelFiveThrottle
      this
    }

    def setLevelSixThrottle(levelSixThrottle: JavaFunction[LevelMeter, Throttle]) = {
      this.levelSixThrottle = levelSixThrottle
      this
    }

    def setAcceleration(acceleration: JavaFunction[LevelZeroMeter, Accelerator]) = {
      this.acceleration = acceleration
      this
    }

    def setByteKeyComparator(byteComparator: KeyComparator[ByteSlice]) = {
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

    def setBuildValidator(buildValidator: BuildValidator) = {
      this.buildValidator = buildValidator
      this
    }

    def get(): swaydb.java.Set[A, F] = {
      val comparator: Either[KeyComparator[ByteSlice], KeyComparator[A]] =
        Eithers.nullCheck(
          left = byteComparator,
          right = typedComparator,
          default = KeyComparator.lexicographic
        )

      val scalaKeyOrder: KeyOrder[Sliced[Byte]] = KeyOrderConverter.toScalaKeyOrder(comparator, serializer)

      val scalaMap =
        swaydb.persistent.Set[A, PureFunction.Set[A], Bag.Less](
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
          shutdownTimeout = shutdownTimeout.toScala,
          acceleration = acceleration.asScala,
          threadStateCache = threadStateCache,
          sortedKeyIndex = sortedKeyIndex,
          randomKeyIndex = randomKeyIndex,
          binarySearchIndex = binarySearchIndex,
          mightContainKeyIndex = mightContainKeyIndex,
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
          bag = Bag.less,
          functions = functions.asInstanceOf[Functions[PureFunction.Set[A]]],
          byteKeyOrder = scalaKeyOrder,
          compactionEC = compactionEC.getOrElse(DefaultExecutionContext.compactionEC),
          buildValidator = buildValidator
        )

      swaydb.java.Set[A, F](scalaMap.asInstanceOf[swaydb.Set[A, F, Bag.Less]])
    }
  }

  def functionsOn[A](dir: Path,
                     serializer: JavaSerializer[A],
                     functions: java.lang.Iterable[PureFunction.OnKey[A, Void, Apply.Set[Void]]]): Config[A, PureFunction.OnKey[A, Void, Apply.Set[Void]]] = {

    implicit val scalaFunctions = functions.castToNothingFunctions
    implicit val scalaSerializer: Serializer[A] = SerializerConverter.toScala(serializer)
    val config: Config[A, PureFunction.Set[A]] = new Config(dir)

    config.asInstanceOf[Config[A, PureFunction.OnKey[A, Void, Apply.Set[Void]]]]
  }

  def functionsOff[A](dir: Path,
                      serializer: JavaSerializer[A]): Config[A, Void] = {

    implicit val scalaSerializer: Serializer[A] = SerializerConverter.toScala(serializer)
    implicit val evd: Void <:< PureFunction.OnKey[A, Nothing, Apply.Set[Nothing]] = null

    new Config(dir)
  }
}
